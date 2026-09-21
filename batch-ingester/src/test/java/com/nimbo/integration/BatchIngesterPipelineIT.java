package com.nimbo.integration;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.consumer.KafkaOffsetCommitter;
import com.nimbo.ingester.consumer.NetflowConsumer;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.metrics.BatchIngesterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import com.nimbo.support.HdfsContainerExtension;
import com.nimbo.support.KafkaContainerExtension;
import com.nimbo.support.NetFlowTestData;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

class BatchIngesterPipelineIT {

    @RegisterExtension
    static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

    @RegisterExtension
    static final HdfsContainerExtension HDFS = new HdfsContainerExtension();

    private final NetFlowTestData testData = new NetFlowTestData();

    @Test
    void givenCompleteKafkaBatch_whenConsumed_thenDurableParquetPrecedesOffsetCommit() throws Exception {
        // Given: three deterministic NetFlow records and a matching rollover threshold
        String topic = "pipeline-valid-topic";
        String dlqTopic = "pipeline-valid-dlq";
        String group = "pipeline-valid-group";
        String basePath = "/netflow/pipeline-valid";
        KAFKA.createTopics(topic, dlqTopic);
        try (PipelineFixture pipeline = pipeline(topic, dlqTopic, group, basePath, 3);
             KafkaProducer<byte[], byte[]> producer = KAFKA.producer()) {
            pipeline.start();
            producer.send(new ProducerRecord<>(topic, testData.serialize(
                    testData.validRecord("192.168.1.1", "10.0.0.1", 1_000L)))).get();
            producer.send(new ProducerRecord<>(topic, testData.serialize(
                    testData.validRecord("192.168.1.2", "10.0.0.1", 2_000L)))).get();
            producer.send(new ProducerRecord<>(topic, testData.serialize(
                    testData.validRecord("192.168.1.3", "10.0.0.1", 3_000L)))).get();

            // When: the consumer completes the atomic batch rollover
            await().atMost(Duration.ofMinutes(6)).pollInterval(Duration.ofMillis(100))
                    .until(() -> finalizedFiles(pipeline.fileSystem(), basePath).size() == 1);
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                    .until(() -> Long.valueOf(3L).equals(KAFKA.committedOffset(group, topic, 0)));
            List<GenericRecord> persisted = readRecords(
                    HDFS.configuration(), finalizedFiles(pipeline.fileSystem(), basePath).get(0));

            // Then: exact records, processing timestamps, metrics, and next offset are durable
            assertThat(persisted).hasSize(3);
            assertThat(persisted)
                    .extracting(
                            record -> record.get("IPV4_SRC_ADDR").toString(),
                            record -> record.get("IPV4_DST_ADDR").toString(),
                            record -> record.get("IN_BYTES"),
                            record -> record.get("OUT_BYTES"))
                    .containsExactly(
                            tuple("192.168.1.1", "10.0.0.1", 1_000L, 500L),
                            tuple("192.168.1.2", "10.0.0.1", 2_000L, 1_000L),
                            tuple("192.168.1.3", "10.0.0.1", 3_000L, 1_500L));
            assertThat(persisted).allSatisfy(record -> assertThat(record.get("EVENT_TIME")).isNotNull());
            assertThat(finalizedFiles(pipeline.fileSystem(), basePath).get(0).getName()).doesNotStartWith(".");
            assertThat(KAFKA.committedOffset(group, topic, 0)).isEqualTo(3L);
            assertThat(pipeline.registry().get("ingester_records_written_total")
                            .tag("instance", "1").counter().count())
                    .isEqualTo(3.0);
        }
    }

    @Test
    void givenInvalidNetFlow_whenConsumed_thenRecordIsSentToDlqAndExcludedFromHdfs() throws Exception {
        // Given: one schema-valid record containing an invalid source address
        String topic = "pipeline-validation-topic";
        String dlqTopic = "pipeline-validation-dlq";
        String basePath = "/netflow/pipeline-validation";
        KAFKA.createTopics(topic, dlqTopic);
        try (PipelineFixture pipeline = pipeline(topic, dlqTopic, "pipeline-validation-group", basePath, 5);
             KafkaProducer<byte[], byte[]> producer = KAFKA.producer();
             var dlqReader = KAFKA.consumer("pipeline-validation-dlq-reader")) {
            dlqReader.subscribe(Collections.singletonList(dlqTopic));
            pipeline.start();
            producer.send(new ProducerRecord<>(topic, testData.serialize(
                    testData.validRecord("999.1.1.1", "10.0.0.1", 500L)))).get();

            // When: validation routes the rejected record through the real DLQ producer
            ConsumerRecord<byte[], byte[]> dlqRecord = pollOne(dlqReader);

            // Then: diagnostics are preserved and no finalized Parquet data is created
            assertThat(header(dlqRecord, "error-type")).isEqualTo("VALIDATION_ERROR");
            assertThat(header(dlqRecord, "original-topic")).isEqualTo(topic);
            assertThat(header(dlqRecord, "error-message"))
                    .isEqualTo("IPV4_SRC_ADDR invalid IPv4: 999.1.1.1");
            assertThat(finalizedFiles(pipeline.fileSystem(), basePath)).isEmpty();
            assertThat(pipeline.registry().get("ingester_dlq_records_total")
                            .tags("instance", "1", "reason", "VALIDATION_ERROR").counter().count())
                    .isEqualTo(1.0);
        }
    }

    @Test
    void givenMalformedKafkaPayload_whenConsumed_thenDeserializationFailureReachesDlq() throws Exception {
        // Given: fixed bytes that cannot be decoded with the production Avro schema
        String topic = "pipeline-malformed-topic";
        String dlqTopic = "pipeline-malformed-dlq";
        String basePath = "/netflow/pipeline-malformed";
        KAFKA.createTopics(topic, dlqTopic);
        try (PipelineFixture pipeline = pipeline(topic, dlqTopic, "pipeline-malformed-group", basePath, 5);
             KafkaProducer<byte[], byte[]> producer = KAFKA.producer();
             var dlqReader = KAFKA.consumer("pipeline-malformed-dlq-reader")) {
            dlqReader.subscribe(Collections.singletonList(dlqTopic));
            pipeline.start();
            producer.send(new ProducerRecord<>(topic, new byte[] {1, 2, 3, 4})).get();

            // When: production deserialization rejects the consumed payload
            ConsumerRecord<byte[], byte[]> dlqRecord = pollOne(dlqReader);

            // Then: the DLQ classifies the failure and HDFS remains untouched
            assertThat(header(dlqRecord, "error-type")).isEqualTo("DESERIALIZATION_ERROR");
            assertThat(header(dlqRecord, "original-topic")).isEqualTo(topic);
            assertThat(header(dlqRecord, "error-message")).isEqualTo("Failed to deserialize Avro record");
            assertThat(finalizedFiles(pipeline.fileSystem(), basePath)).isEmpty();
        }
    }

    private PipelineFixture pipeline(
            String topic, String dlqTopic, String group, String basePath, long batchSize) throws Exception {
        FileSystem fileSystem = HDFS.fileSystem();
        Path path = new Path(basePath);
        fileSystem.delete(path, true);

        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.setBootstrapServers(KAFKA.bootstrapServers());
        kafkaProperties.setTopic(topic);
        kafkaProperties.setDlqTopic(dlqTopic);
        kafkaProperties.setConsumerGroup(group);
        kafkaProperties.setEnableAutoCommit(false);
        kafkaProperties.setAutoOffsetReset("earliest");
        kafkaProperties.setPollTimeoutMs(100);
        kafkaProperties.setMaxPollRecords(500);
        kafkaProperties.setSessionTimeoutMs(10_000);
        kafkaProperties.setHeartbeatIntervalMs(3_000);
        kafkaProperties.setMaxPollIntervalMs(60_000);

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri(HDFS.uri());
        hdfsProperties.setBasePath(basePath);

        ParquetProperties parquetProperties = new ParquetProperties();
        parquetProperties.setCompressionCodec("SNAPPY");
        parquetProperties.setBlockSizeBytes(128 * 1024 * 1024);
        parquetProperties.setPageSizeBytes(1024 * 1024);
        parquetProperties.setEnableDictionary(true);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        BatchIngesterMetrics metrics = new BatchIngesterMetrics(registry);
        metrics.registerInstance(1);
        NetFlowStorage storage = new NetFlowStorage(
                new BatchPolicy(batchSize, 1024 * 1024, Duration.ofMinutes(2)),
                new ParquetFileWriter(fileSystem, parquetProperties),
                new HdfsFileStorage(fileSystem),
                new FileNameGenerator(),
                hdfsProperties,
                testData.storageSchema(),
                metrics,
                1);
        KafkaProducer<byte[], byte[]> dlqProducer = KAFKA.producer();
        NetflowConsumer consumer = new NetflowConsumer(
                KAFKA.consumer(group),
                kafkaProperties,
                new AvroDeserializer(testData.sourceSchema()),
                new DataProcessingService(storage),
                new NetflowValidationService(),
                new DlqService(dlqProducer, kafkaProperties),
                new KafkaOffsetCommitter(),
                1,
                metrics);
        return new PipelineFixture(consumer, dlqProducer, fileSystem, registry);
    }

    private ConsumerRecord<byte[], byte[]> pollOne(org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer) {
        AtomicReference<ConsumerRecord<byte[], byte[]>> received = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).until(() -> {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                received.set(records.iterator().next());
            }
            return received.get() != null;
        });
        return received.get();
    }

    private List<Path> finalizedFiles(FileSystem fileSystem, String basePath) throws Exception {
        Path root = new Path(basePath);
        if (!fileSystem.exists(root)) {
            return List.of();
        }
        List<Path> files = new ArrayList<>();
        var iterator = fileSystem.listFiles(root, true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            String name = file.getPath().getName();
            if (!name.startsWith(".") && name.endsWith(".parquet")) {
                files.add(file.getPath());
            }
        }
        files.sort(java.util.Comparator.comparing(Path::toString));
        return files;
    }

    private List<GenericRecord> readRecords(Configuration configuration, Path path) throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(path, configuration))
                .withDataModel(GenericData.get())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private String header(ConsumerRecord<byte[], byte[]> record, String name) {
        var header = Objects.requireNonNull(record.headers().lastHeader(name));
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private record PipelineFixture(
            NetflowConsumer consumer,
            KafkaProducer<byte[], byte[]> dlqProducer,
            FileSystem fileSystem,
            SimpleMeterRegistry registry) implements AutoCloseable {

        void start() {
            consumer.start();
        }

        @Override
        public void close() throws Exception {
            consumer.shutdown();
            dlqProducer.close();
            registry.close();
        }
    }
}
