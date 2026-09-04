package com.nimbo.integration;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.consumer.KafkaOffsetCommitter;
import com.nimbo.ingester.consumer.NetflowConsumer;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import com.nimbo.metrics.BatchIngesterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class NetflowHDFSWriterPipelineIT {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    private static final GenericContainer<?> HDFS_CONTAINER =
            new GenericContainer<>(DockerImageName.parse("apache/hadoop:3.3.6"))
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("hadoop/core-site.xml"),
                            "/opt/hadoop/etc/hadoop/core-site.xml")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("hadoop/hdfs-site.xml"),
                            "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                    .withExposedPorts(8020, 9870, 9866)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("hadoop/start-hdfs.sh"),
                            "/opt/hadoop/start-hdfs.sh")
                    .withCommand("bash", "/opt/hadoop/start-hdfs.sh")
                    .waitingFor(Wait.forLogMessage(".*HDFS_READY.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    private String topic;
    private String dlqTopic;
    private static final int BATCH_SIZE = 5;

    private Schema sourceSchema;
    private Schema storageSchema;
    private Configuration hadoopConfiguration;
    private FileSystem fileSystem;

    private KafkaProperties kafkaProperties;
    private KafkaProducer<byte[], byte[]> testProducer;
    private KafkaConsumer<byte[], byte[]> pipelineKafkaConsumer;
    private KafkaConsumer<byte[], byte[]> dlqReaderConsumer;
    private DlqService dlqService;

    private NetflowConsumer netflowConsumer;
    private BatchIngesterMetrics metrics;

    @BeforeEach
    void setUp() throws Exception {
        sourceSchema = loadNetflowV2Schema();
        storageSchema = withProcessingTime(sourceSchema);

        hadoopConfiguration = createHadoopConfiguration();
        fileSystem = FileSystem.get(
                URI.create(hadoopConfiguration.get("fs.defaultFS")), hadoopConfiguration, "hadoop");
        assertTrue(fileSystem.exists(new Path("/")), "HDFS root should be accessible");

        topic = "netflow-" + UUID.randomUUID();
        dlqTopic = "netflow-dlq-" + UUID.randomUUID();
        kafkaProperties = buildKafkaProperties();
        createTopics();

        testProducer = createProducer();
        dlqService = new DlqService(createProducer(), kafkaProperties);
        dlqReaderConsumer = createRawConsumer("dlq-reader-" + UUID.randomUUID());
        dlqReaderConsumer.subscribe(Collections.singletonList(dlqTopic));

        metrics = new BatchIngesterMetrics(new SimpleMeterRegistry());
        metrics.registerInstance(1);

        NetFlowStorage netFlowStorage = createNetFlowStorage(fileSystem);
        DataProcessingService dataProcessingService = new DataProcessingService(netFlowStorage);
        NetflowValidationService validationService = new NetflowValidationService();
        AvroDeserializer avroDeserializer = new AvroDeserializer(sourceSchema);

        pipelineKafkaConsumer = createConsumer("netflow-consumer-" + UUID.randomUUID());

        netflowConsumer = new NetflowConsumer(
                pipelineKafkaConsumer,
                kafkaProperties,
                avroDeserializer,
                dataProcessingService,
                validationService,
                dlqService,
                new KafkaOffsetCommitter(),
                1,
                metrics
        );
    }

    @AfterEach
    void tearDown() {
        if (netflowConsumer != null) {
            netflowConsumer.shutdown();
        }
        closeQuietly(testProducer);
        closeQuietly(dlqReaderConsumer);
        closeQuietly(fileSystem);
    }

    @Test
    @DisplayName("Should consume real Kafka records, validate, and persist as Parquet in HDFS")
    void shouldConsumeValidateAndPersistThroughFullPipeline() throws Exception {

        netflowConsumer.start();

        List<GenericRecord> sentRecords = new ArrayList<>();
        for (int i = 1; i <= BATCH_SIZE; i++) {
            GenericRecord record = validRecord("192.168.1." + i, "10.0.0.1", 1000L * i);
            sentRecords.add(record);
            publish(topic, record);
        }
        testProducer.flush();

        Path storageDirectory = new Path("/netflow/parquet");

        await().atMost(ofSeconds(30))
                .pollInterval(ofSeconds(1))
                .until(() -> {
                    if (!fileSystem.exists(storageDirectory)) return false;
                    FileStatus[] files = fileSystem.listStatus(storageDirectory);
                    return Arrays.stream(files)
                            .filter(f -> !f.getPath().getName().startsWith("."))
                            .filter(f -> f.getPath().getName().endsWith(".parquet"))
                            .findFirst()
                            .map(f -> f.getLen() > 0)
                            .orElse(false);
                });

        FileStatus[] files = Arrays.stream(fileSystem.listStatus(storageDirectory))
                .filter(fs -> !fs.getPath().getName().startsWith("."))
                .toArray(FileStatus[]::new);
        assertEquals(1, files.length, "Exactly one finalized Parquet file should be created");

        Path parquetFile = files[0].getPath();
        assertTrue(parquetFile.getName().endsWith(".parquet"));
        assertFalse(parquetFile.getName().startsWith("."), "Temp file must not remain");
        assertTrue(files[0].getLen() > 0);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(parquetFile, hadoopConfiguration)).withDataModel(GenericData.get()).build()) {

            int count = 0;
            GenericRecord actual;
            while ((actual = reader.read()) != null) {
                GenericRecord expected = sentRecords.get(count);
                assertEquals(expected.get("IPV4_SRC_ADDR").toString(), actual.get("IPV4_SRC_ADDR").toString());
                assertEquals(expected.get("IPV4_DST_ADDR").toString(), actual.get("IPV4_DST_ADDR").toString());
                assertEquals(expected.get("IN_BYTES"), actual.get("IN_BYTES"));
                assertNotNull(actual.get("PROCESSING_TIME"), "PROCESSING_TIME should be stamped by storage layer");
                count++;
            }
            assertEquals(sentRecords.size(), count, "All produced Kafka records must land in Parquet");
        }
    }

    @Test
    @DisplayName("Should route a record that fails validation to the DLQ topic instead of HDFS")
    void shouldRouteInvalidRecordToDlq() throws Exception {

        netflowConsumer.start();

        GenericRecord invalid = validRecord("999.999.999.999", "10.0.0.1", 500L); // bad IPv4
        publish(topic, invalid);
        testProducer.flush();

        ConsumerRecord<byte[], byte[]> dlqRecord = pollForOne(dlqReaderConsumer, Duration.ofSeconds(20));

        assertNotNull(dlqRecord, "Invalid record should have been published to the DLQ topic");
        assertEquals("VALIDATION_ERROR", headerValue(dlqRecord, "error-type"));
        assertEquals(topic, headerValue(dlqRecord, "original-topic"));
        assertNotNull(headerValue(dlqRecord, "error-message"));

        // and it must not have reached HDFS
        Path storageDirectory = new Path("/netflow/parquet");
        boolean exists = fileSystem.exists(storageDirectory);
        assertFalse(exists && fileSystem.listStatus(storageDirectory).length > 0,
                "Invalid record must not be persisted to HDFS");
    }


    private KafkaProperties buildKafkaProperties() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers());
        properties.setTopic(topic);
        properties.setDlqTopic(dlqTopic);
        properties.setConsumerGroup("netflow-consumer-it-" + UUID.randomUUID());
        properties.setEnableAutoCommit(false);
        properties.setAutoOffsetReset("earliest");
        properties.setPollTimeoutMs(500);
        properties.setMaxPollRecords(500);
        properties.setSessionTimeoutMs(10_000);
        properties.setHeartbeatIntervalMs(3_000);
        properties.setMaxPollIntervalMs(60_000);
        properties.setConsumerInstances(1);
        return properties;
    }

    private void createTopics() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(
                    new NewTopic(topic, 1, (short) 1),
                    new NewTopic(dlqTopic, 1, (short) 1)
            )).all().get();
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaProperties.isEnableAutoCommit());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaProperties.getSessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaProperties.getHeartbeatIntervalMs());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaProperties.getMaxPollIntervalMs());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getMaxPollRecords());
        return new KafkaConsumer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> createRawConsumer(String groupId) {
        return createConsumer(groupId);
    }

    private Configuration createHadoopConfiguration() {
        String hdfsUri = "hdfs://" + HDFS_CONTAINER.getHost() + ":" + HDFS_CONTAINER.getMappedPort(8020);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("dfs.replication", "1");
        return conf;
    }

    private NetFlowStorage createNetFlowStorage(FileSystem fs) {
        BatchPolicy batchPolicy = new BatchPolicy(BATCH_SIZE, 1024 * 1024, Duration.ofMinutes(2));

        ParquetProperties parquetProperties = new ParquetProperties();
        parquetProperties.setCompressionCodec("SNAPPY");
        parquetProperties.setBlockSizeBytes(128 * 1024 * 1024);
        parquetProperties.setPageSizeBytes(1024 * 1024);
        parquetProperties.setEnableDictionary(true);

        ParquetFileWriter parquetFileWriter = new ParquetFileWriter(fs, parquetProperties);
        FileNameGenerator fileNameGenerator = new FileNameGenerator();
        HdfsFileStorage hdfsFileStorage = new HdfsFileStorage(fs);

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setBasePath("/netflow/parquet");

        return new NetFlowStorage(
                batchPolicy,
                parquetFileWriter,
                hdfsFileStorage,
                fileNameGenerator,
                hdfsProperties,
                storageSchema,
                metrics,
                1
        );
    }

    private Schema loadNetflowV2Schema() throws IOException {
        try (var in = getClass().getClassLoader().getResourceAsStream("avro/NetFlowV2Record.avsc")) {
            assertNotNull(in, "netflow_v2.avsc must be on the test classpath");
            return new Schema.Parser().parse(in);
        }
    }

    private Schema withProcessingTime(Schema source) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field field : source.getFields()) {
            fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
        }
        fields.add(new Schema.Field(
                "PROCESSING_TIME",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)),
                "Processing time",
                (Object) null
        ));
        return Schema.createRecord(source.getName(), source.getDoc(), source.getNamespace(), source.isError(), fields);
    }


    private GenericRecord validRecord(String srcIp, String dstIp, long inBytes) {
        GenericData.Record record = new GenericData.Record(sourceSchema);
        record.put("IPV4_SRC_ADDR", srcIp);
        record.put("IPV4_DST_ADDR", dstIp);
        record.put("L4_SRC_PORT", 443);
        record.put("L4_DST_PORT", 51000);
        record.put("PROTOCOL", 6);
        record.put("L7_PROTO", 91.0);
        record.put("IN_BYTES", inBytes);
        record.put("OUT_BYTES", inBytes / 2);
        record.put("IN_PKTS", 10L);
        record.put("OUT_PKTS", 8L);
        record.put("FLOW_DURATION_MILLISECONDS", 1500L);
        record.put("TCP_FLAGS", 24);
        record.put("CLIENT_TCP_FLAGS", 2);
        record.put("SERVER_TCP_FLAGS", 18);
        record.put("DURATION_IN", 700L);
        record.put("DURATION_OUT", 800L);
        record.put("MIN_TTL", 60);
        record.put("MAX_TTL", 64);
        record.put("LONGEST_FLOW_PKT", 1500);
        record.put("SHORTEST_FLOW_PKT", 64);
        record.put("MIN_IP_PKT_LEN", 64);
        record.put("MAX_IP_PKT_LEN", 1500);
        record.put("SRC_TO_DST_SECOND_BYTES", 12000L);
        record.put("DST_TO_SRC_SECOND_BYTES", 6000L);
        record.put("RETRANSMITTED_IN_BYTES", 0L);
        record.put("RETRANSMITTED_IN_PKTS", 0L);
        record.put("RETRANSMITTED_OUT_BYTES", 0L);
        record.put("RETRANSMITTED_OUT_PKTS", 0L);
        record.put("SRC_TO_DST_AVG_THROUGHPUT", 96000L);
        record.put("DST_TO_SRC_AVG_THROUGHPUT", 48000L);
        record.put("NUM_PKTS_UP_TO_128_BYTES", 2L);
        record.put("NUM_PKTS_128_TO_256_BYTES", 2L);
        record.put("NUM_PKTS_256_TO_512_BYTES", 2L);
        record.put("NUM_PKTS_512_TO_1024_BYTES", 2L);
        record.put("NUM_PKTS_1024_TO_1514_BYTES", 2L);
        record.put("TCP_WIN_MAX_IN", 65535);
        record.put("TCP_WIN_MAX_OUT", 65535);
        record.put("ICMP_TYPE", 0);
        record.put("ICMP_IPV4_TYPE", 0);
        record.put("DNS_QUERY_ID", 0);
        record.put("DNS_QUERY_TYPE", 0);
        record.put("DNS_TTL_ANSWER", 0L);
        record.put("FTP_COMMAND_RET_CODE", 0);
        return record;
    }

    private byte[] toBinaryAvro(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(sourceSchema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private void publish(String topic, GenericRecord record) throws IOException {
        testProducer.send(new ProducerRecord<>(topic, null, toBinaryAvro(record)));
    }


    private void awaitCondition(java.util.function.BooleanSupplier condition, Duration timeout, String failMessage)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        fail(failMessage);
    }

    private ConsumerRecord<byte[], byte[]> pollForOne(KafkaConsumer<byte[], byte[]> consumer, Duration timeout) {
        return await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(300))
                .until(() -> {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    return records.isEmpty() ? null : records.iterator().next();
                }, Objects::nonNull);
    }

    private String headerValue(ConsumerRecord<byte[], byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}