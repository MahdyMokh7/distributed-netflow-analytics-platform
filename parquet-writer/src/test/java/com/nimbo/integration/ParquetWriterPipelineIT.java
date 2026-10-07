package com.nimbo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.consumer.KafkaOffsetCommitter;
import com.nimbo.ingester.consumer.NetflowConsumer;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.HourWindowPolicy;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import com.nimbo.support.KafkaContainerExtension;
import com.nimbo.support.NetFlowTestData;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ParquetWriterPipelineIT {

  @RegisterExtension static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

  @TempDir static Path storageRoot;

  private final NetFlowTestData testData = new NetFlowTestData();

  @Test
  void givenCompleteKafkaBatch_whenConsumed_thenDurableParquetPrecedesOffsetCommit()
      throws Exception {
    String topic = "pipeline-valid-topic";
    String dlqTopic = "pipeline-valid-dlq";
    String group = "pipeline-valid-group";
    String basePath = storageRoot.resolve("netflow/pipeline-valid").toString();
    KAFKA.createTopics(topic, dlqTopic);
    try (PipelineFixture pipeline = pipeline(topic, dlqTopic, group, basePath, 3, Clock.systemUTC(), true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer()) {
      pipeline.start();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.1.1", "10.0.0.1", 1_000L))))
          .get();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.1.2", "10.0.0.1", 2_000L))))
          .get();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.1.3", "10.0.0.1", 3_000L))))
          .get();

      await()
          .atMost(Duration.ofMinutes(1))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> finalizedFiles(pipeline.fileSystem(), basePath).size() == 1);

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> Long.valueOf(3L).equals(KAFKA.committedOffset(group, topic, 0)));

      List<GenericRecord> persisted =
          readRecords(localConfiguration(), finalizedFiles(pipeline.fileSystem(), basePath).get(0));

      assertThat(persisted).hasSize(3);
      assertThat(persisted)
          .extracting(
              testRecord -> testRecord.get("IPV4_SRC_ADDR").toString(),
              testRecord -> testRecord.get("IPV4_DST_ADDR").toString(),
              testRecord -> testRecord.get("IN_BYTES"),
              testRecord -> testRecord.get("OUT_BYTES"))
          .containsExactly(
              tuple("192.168.1.1", "10.0.0.1", 1_000L, 500L),
              tuple("192.168.1.2", "10.0.0.1", 2_000L, 1_000L),
              tuple("192.168.1.3", "10.0.0.1", 3_000L, 1_500L));
      assertThat(persisted)
          .allSatisfy(testRecord -> assertThat(testRecord.get("PROCESS_TIME")).isNotNull());
      assertThat(finalizedFiles(pipeline.fileSystem(), basePath).get(0).getName())
          .doesNotStartWith(".");
      assertThat(KAFKA.committedOffset(group, topic, 0)).isEqualTo(3L);
      assertThat(
              pipeline
                  .registry()
                  .get("ingester_records_written_total")
                  .tag("instance", "1")
                  .counter()
                  .count())
          .isEqualTo(3.0);
    }
  }

  @Test
  void givenRecordsSpanningHourBoundary_whenConsumed_thenEachHourGetsItsOwnFile() throws Exception {
    String topic = "pipeline-hour-boundary-topic";
    String dlqTopic = "pipeline-hour-boundary-dlq";
    String group = "pipeline-hour-boundary-group";
    String basePath = storageRoot.resolve("netflow/pipeline-hour-boundary").toString();
    KAFKA.createTopics(topic, dlqTopic);

    long firstHourMillis = Instant.parse("2026-09-18T10:45:00.000Z").toEpochMilli();
    long secondHourMillis = Instant.parse("2026-09-18T11:15:00.000Z").toEpochMilli();

    Clock clock = Clock.fixed(Instant.parse("2026-09-18T11:30:00Z"), ZoneOffset.UTC);
    try (PipelineFixture pipeline = pipeline(topic, dlqTopic, group, basePath, 2, clock, true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer()) {
      pipeline.start();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  null,
                  firstHourMillis,
                  null,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.10.1", "10.0.0.1", 1_000L))))
          .get();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  null,
                  secondHourMillis,
                  null,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.10.2", "10.0.0.1", 2_000L))))
          .get();

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> finalizedFiles(pipeline.fileSystem(), basePath).size() == 2);
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> Long.valueOf(2L).equals(KAFKA.committedOffset(group, topic, 0)));

    FileSystem fs = FileSystem.newInstance(localConfiguration());
    try {
      List<org.apache.hadoop.fs.Path> files = finalizedFiles(fs, basePath);
      assertThat(files).hasSize(2);

      List<String> hourFolders = files.stream().map(f -> f.getParent().getName()).sorted().toList();
      assertThat(hourFolders).containsExactly("10", "11");

      List<Long> processTimes = new ArrayList<>();
      for (org.apache.hadoop.fs.Path file : files) {
        List<GenericRecord> records = readRecords(localConfiguration(), file);
        assertThat(records).hasSize(1);
        processTimes.add((Long) records.get(0).get("PROCESS_TIME"));
      }
      assertThat(processTimes).containsExactlyInAnyOrder(firstHourMillis, secondHourMillis);
    } finally {
      fs.close();
    }
  }

  @Test
  void givenRecordsSpanningDayBoundary_whenConsumed_thenEachDayGetsItsOwnFile() throws Exception {
    String topic = "pipeline-day-boundary-topic";
    String dlqTopic = "pipeline-day-boundary-dlq";
    String group = "pipeline-day-boundary-group";
    String basePath = storageRoot.resolve("netflow/pipeline-day-boundary").toString();
    KAFKA.createTopics(topic, dlqTopic);

    long lastHourOfDay = Instant.parse("2026-09-18T23:45:00.000Z").toEpochMilli();
    long firstHourOfNextDay = Instant.parse("2026-09-19T00:15:00.000Z").toEpochMilli();

    Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:30:00Z"), ZoneOffset.UTC);
    try (PipelineFixture pipeline = pipeline(topic, dlqTopic, group, basePath, 2, clock, true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer()) {
      pipeline.start();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  null,
                  lastHourOfDay,
                  null,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.20.1", "10.0.0.1", 1_000L))))
          .get();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  null,
                  firstHourOfNextDay,
                  null,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.20.2", "10.0.0.1", 2_000L))))
          .get();

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> finalizedFiles(pipeline.fileSystem(), basePath).size() == 2);
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> Long.valueOf(2L).equals(KAFKA.committedOffset(group, topic, 0)));

    FileSystem fs = FileSystem.newInstance(localConfiguration());
    try {
      List<org.apache.hadoop.fs.Path> files = finalizedFiles(fs, basePath);
      assertThat(files).hasSize(2);

      List<String> dateFolders =
          files.stream().map(f -> f.getParent().getParent().getName()).sorted().toList();
      assertThat(dateFolders).containsExactly("2026-09-18", "2026-09-19");

      for (org.apache.hadoop.fs.Path file : files) {
        assertThat(readRecords(localConfiguration(), file)).hasSize(1);
      }
    } finally {
      fs.close();
    }
  }

  @Test
  void givenRecordOutsideAllowedHourWindow_whenConsumed_thenItIsDurablyPublishedAndCommitted()
      throws Exception {
    String topic = "pipeline-late-topic";
    String dlqTopic = "pipeline-late-dlq";
    String lateTopic = dlqTopic + "-late";
    String group = "pipeline-late-group";
    String basePath = storageRoot.resolve("netflow/pipeline-late").toString();
    Instant now = Instant.parse("2026-09-18T11:30:00Z");
    long lateTimestamp = now.minus(Duration.ofHours(2)).toEpochMilli();
    KAFKA.createTopics(topic, dlqTopic, lateTopic);

    try (PipelineFixture pipeline =
            pipeline(topic, dlqTopic, group, basePath, 5, Clock.fixed(now, ZoneOffset.UTC), true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer();
        var lateReader = KAFKA.consumer("pipeline-late-reader")) {
      lateReader.subscribe(Collections.singletonList(lateTopic));
      pipeline.start();
      byte[] key = {7};
      byte[] payload =
          pipeline.serialize(
              topic, testData.validRecord("192.168.30.1", "10.0.0.1", 1_000L));
      producer.send(new ProducerRecord<>(topic, null, lateTimestamp, key, payload)).get();

      ConsumerRecord<byte[], byte[]> lateRecord = pollOne(lateReader);
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Long.valueOf(1L).equals(KAFKA.committedOffset(group, topic, 0)));

      assertThat(lateRecord.key()).containsExactly(key);
      assertThat(lateRecord.value()).containsExactly(payload);
      assertThat(lateRecord.timestamp()).isEqualTo(lateTimestamp);
      assertThat(header(lateRecord, "error-type")).isEqualTo("LATE_RECORD");
      assertThat(header(lateRecord, "original-topic")).isEqualTo(topic);
      assertThat(finalizedFiles(pipeline.fileSystem(), basePath)).isEmpty();
      assertThat(KAFKA.committedOffset(group, topic, 0)).isEqualTo(1L);
    }
  }

  @Test
  void givenHistoricalRecordAndLateRoutingDisabled_whenConsumed_thenItIsWrittenToParquet()
      throws Exception {
    String topic = "pipeline-backfill-topic";
    String dlqTopic = "pipeline-backfill-dlq";
    String group = "pipeline-backfill-group";
    String basePath = storageRoot.resolve("netflow/pipeline-backfill").toString();
    Instant now = Instant.parse("2026-10-07T11:30:00Z");
    long historicalTimestamp = Instant.parse("2026-10-03T08:15:00Z").toEpochMilli();
    KAFKA.createTopics(topic, dlqTopic, dlqTopic + "-late");

    try (PipelineFixture pipeline =
            pipeline(topic, dlqTopic, group, basePath, 1, Clock.fixed(now, ZoneOffset.UTC), false);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer()) {
      pipeline.start();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  null,
                  historicalTimestamp,
                  null,
                  pipeline.serialize(
                      topic, testData.validRecord("192.168.40.1", "10.0.0.1", 1_000L))))
          .get();

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> finalizedFiles(pipeline.fileSystem(), basePath).size() == 1);
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Long.valueOf(1L).equals(KAFKA.committedOffset(group, topic, 0)));

      org.apache.hadoop.fs.Path file = finalizedFiles(pipeline.fileSystem(), basePath).get(0);
      assertThat(file.getParent().getName()).isEqualTo("08");
      assertThat(file.getParent().getParent().getName()).isEqualTo("2026-10-03");
      assertThat(readRecords(localConfiguration(), file)).hasSize(1);
      assertThat(KAFKA.committedOffset(group, topic, 0)).isEqualTo(1L);
    }
  }

  @Test
  void givenInvalidNetFlow_whenConsumed_thenRecordIsSentToDlqAndExcludedFromHdfs()
      throws Exception {
    String topic = "pipeline-validation-topic";
    String dlqTopic = "pipeline-validation-dlq";
    String basePath = "/netflow/pipeline-validation";
    KAFKA.createTopics(topic, dlqTopic);
    try (PipelineFixture pipeline =
            pipeline(topic, dlqTopic, "pipeline-validation-group", basePath, 5, Clock.systemUTC(), true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer();
        var dlqReader = KAFKA.consumer("pipeline-validation-dlq-reader")) {
      dlqReader.subscribe(Collections.singletonList(dlqTopic));
      pipeline.start();
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  pipeline.serialize(topic, testData.validRecord("999.1.1.1", "10.0.0.1", 500L))))
          .get();

      ConsumerRecord<byte[], byte[]> dlqRecord = pollOne(dlqReader);

      assertThat(header(dlqRecord, "error-type")).isEqualTo("VALIDATION_ERROR");
      assertThat(header(dlqRecord, "original-topic")).isEqualTo(topic);
      assertThat(header(dlqRecord, "error-message"))
          .isEqualTo("IPV4_SRC_ADDR invalid IPv4: 999.1.1.1");
      assertThat(finalizedFiles(pipeline.fileSystem(), basePath)).isEmpty();
      assertThat(
              pipeline
                  .registry()
                  .get("ingester_dlq_records_total")
                  .tags("instance", "1", "reason", "VALIDATION_ERROR")
                  .counter()
                  .count())
          .isEqualTo(1.0);
    }
  }

  @Test
  void givenMalformedKafkaPayload_whenConsumed_thenDeserializationFailureReachesDlq()
      throws Exception {
    String topic = "pipeline-malformed-topic";
    String dlqTopic = "pipeline-malformed-dlq";
    String basePath = "/netflow/pipeline-malformed";
    KAFKA.createTopics(topic, dlqTopic);
    try (PipelineFixture pipeline =
            pipeline(topic, dlqTopic, "pipeline-malformed-group", basePath, 5, Clock.systemUTC(), true);
        KafkaProducer<byte[], byte[]> producer = KAFKA.producer();
        var dlqReader = KAFKA.consumer("pipeline-malformed-dlq-reader")) {
      dlqReader.subscribe(Collections.singletonList(dlqTopic));
      pipeline.start();
      producer.send(new ProducerRecord<>(topic, new byte[] {1, 2, 3, 4})).get();

      ConsumerRecord<byte[], byte[]> dlqRecord = pollOne(dlqReader);

      assertThat(header(dlqRecord, "error-type")).isEqualTo("DESERIALIZATION_ERROR");
      assertThat(header(dlqRecord, "original-topic")).isEqualTo(topic);
      assertThat(header(dlqRecord, "error-message")).isEqualTo("Failed to deserialize Avro record");
      assertThat(finalizedFiles(pipeline.fileSystem(), basePath)).isEmpty();
    }
  }

  private PipelineFixture pipeline(String topic, String dlqTopic, String group, String basePath, long batchSize,
      Clock clock, boolean lateRoutingEnabled) throws Exception {
    Configuration configuration = localConfiguration();
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(basePath);
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }

    KafkaProperties kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(KAFKA.bootstrapServers());
    kafkaProperties.setTopic(topic);
    kafkaProperties.setDlqTopic(dlqTopic);
    kafkaProperties.setLateTopic(dlqTopic + "-late");
    kafkaProperties.setConsumerGroup(group);
    kafkaProperties.setEnableAutoCommit(false);
    kafkaProperties.setAutoOffsetReset("earliest");
    kafkaProperties.setPollTimeoutMs(100);
    kafkaProperties.setMaxPollRecords(500);
    kafkaProperties.setSessionTimeoutMs(10_000);
    kafkaProperties.setHeartbeatIntervalMs(3_000);
    kafkaProperties.setMaxPollIntervalMs(60_000);

    HdfsProperties hdfsProperties = new HdfsProperties();
    hdfsProperties.setUri(configuration.get("fs.defaultFS"));
    hdfsProperties.setBasePath(basePath);

    ParquetProperties parquetProperties = new ParquetProperties();
    parquetProperties.setCompressionCodec("SNAPPY");
    parquetProperties.setBlockSizeBytes(128 * 1024 * 1024);
    parquetProperties.setPageSizeBytes(1024 * 1024);
    parquetProperties.setEnableDictionary(true);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ParquetWriterMetrics metrics = new ParquetWriterMetrics(registry);
    metrics.registerInstance(1);
    NetFlowStorage storage =
        new NetFlowStorage(
            new BatchPolicy(batchSize, 1024 * 1024, Duration.ofMinutes(2)),
            new ParquetFileWriter(fileSystem, parquetProperties),
            new HdfsFileStorage(fileSystem),
            new FileNameGenerator(),
            hdfsProperties,
            new HourWindowPolicy(1, lateRoutingEnabled, clock),
            metrics,
            1,
            List.of(testData.sourceSchema()));
    KafkaProducer<byte[], byte[]> dlqProducer = KAFKA.producer();
    Map<String, Object> registryConfig =
        Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KAFKA.schemaRegistryUrl());
    KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
    avroSerializer.configure(registryConfig, false);
    KafkaAvroDeserializer avroDelegate = new KafkaAvroDeserializer();
    avroDelegate.configure(registryConfig, false);
    NetflowConsumer consumer =
        new NetflowConsumer(
            KAFKA.consumer(group),
            kafkaProperties,
            new AvroDeserializer(avroDelegate),
            new DataProcessingService(storage),
            new NetflowValidationService(),
            new DlqService(dlqProducer, kafkaProperties),
            new KafkaOffsetCommitter(),
            1,
            metrics);
    return new PipelineFixture(consumer, dlqProducer, fileSystem, registry, avroSerializer);
  }

  private Configuration localConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "file://" + storageRoot.toAbsolutePath());
    configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return configuration;
  }

  private ConsumerRecord<byte[], byte[]> pollOne(
      org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer) {
    AtomicReference<ConsumerRecord<byte[], byte[]>> received = new AtomicReference<>();
    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(100))
        .until(
            () -> {
              var records = consumer.poll(Duration.ofMillis(100));
              if (!records.isEmpty()) {
                received.set(records.iterator().next());
              }
              return received.get() != null;
            });
    return received.get();
  }

  private List<org.apache.hadoop.fs.Path> finalizedFiles(FileSystem fileSystem, String basePath)
      throws Exception {
    org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(basePath);
    if (!fileSystem.exists(root)) {
      return List.of();
    }

    AtomicReference<List<org.apache.hadoop.fs.Path>> finalizedFiles = new AtomicReference<>();
    await()
        .atMost(Duration.ofMillis(300))
        .pollInterval(Duration.ofMillis(50))
        .ignoreException(RuntimeException.class)
        .until(
            () -> {
              List<org.apache.hadoop.fs.Path> files = new ArrayList<>();
              var iterator = fileSystem.listFiles(root, true);
              while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                String name = file.getPath().getName();
                if (!name.startsWith(".") && name.endsWith(".parquet")) {
                  files.add(file.getPath());
                }
              }
              files.sort(java.util.Comparator.comparing(org.apache.hadoop.fs.Path::toString));
              finalizedFiles.set(files);
              return true;
            });
    return finalizedFiles.get();
  }

  private List<GenericRecord> readRecords(
      Configuration configuration, org.apache.hadoop.fs.Path path) throws Exception {
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, configuration))
            .withDataModel(GenericData.get())
            .build()) {
      GenericRecord testRecord;
      while ((testRecord = reader.read()) != null) {
        records.add(testRecord);
      }
    }
    return records;
  }

  private String header(ConsumerRecord<byte[], byte[]> testRecord, String name) {
    var header = Objects.requireNonNull(testRecord.headers().lastHeader(name));
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private record PipelineFixture(
      NetflowConsumer consumer,
      KafkaProducer<byte[], byte[]> dlqProducer,
      FileSystem fileSystem,
      SimpleMeterRegistry registry,
      KafkaAvroSerializer avroSerializer)
      implements AutoCloseable {

    void start() {
      consumer.start();
    }

    byte[] serialize(String topic, GenericRecord genericRecord) {
      return avroSerializer.serialize(topic, genericRecord);
    }

    @Override
    public void close() throws Exception {
      consumer.shutdown();
      dlqProducer.close();
      avroSerializer.close();
      registry.close();
      fileSystem.close();
    }
  }
}
