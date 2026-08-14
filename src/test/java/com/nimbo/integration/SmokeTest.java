//package com.nimbo.integration;
//
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.BinaryEncoder;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.Encoder;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.ByteArraySerializer;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.springframework.core.io.ClassPathResource;
//import org.testcontainers.containers.GenericContainer;
//import org.testcontainers.containers.KafkaContainer;
//import org.testcontainers.containers.wait.strategy.Wait;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import org.testcontainers.utility.DockerImageName;
//import org.springframework.web.client.RestTemplate;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.time.Duration;
//import java.util.Properties;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.awaitility.Awaitility.await;
//
//@Testcontainers
//@DisplayName("Smoke Tests - End-to-End Validation")
//class SmokeTest {
//
//    private static final String NETFLOW_TOPIC = "netflow";
//    private static final String AVRO_SCHEMA_PATH = "avro/NetFlowV2Record.avsc";
//
//    // Real batch policy is rolling.max-age=2m (see application.yml); the smoke test must wait
//    // for a genuine age-based flush rather than shortening production settings for the test.
//    private static final Duration FLUSH_WAIT_TIMEOUT = Duration.ofSeconds(150);
//
//    private final RestTemplate restTemplate = new RestTemplate();
//
//    private static Schema avroSchema;
//    private static KafkaContainer kafka;
//    private static GenericContainer<?> hdfs;
//    private static GenericContainer<?> ingester;
//
//    @BeforeAll
//    static void setUpAll() throws IOException, InterruptedException {
//        avroSchema = loadAvroSchema();
//
//        // 1. Start Kafka container
//        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
//        kafka.start();
//
//        // 2. Start HDFS container
//        // Do not override the container command: the image's own entrypoint already formats
//        // and starts the namenode from these env vars (same as docker-compose.yaml), then
//        // exposes a built-in HEALTHCHECK we can wait on directly.
//        hdfs = new GenericContainer<>(DockerImageName.parse("bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8"))
//                .withExposedPorts(9000, 9870)
//                .withEnv("CLUSTER_NAME", "test")
//                .withEnv("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
//                .withEnv("HDFS_CONF_dfs_replication", "1")
//                .waitingFor(Wait.forHealthcheck())
//                .withStartupTimeout(Duration.ofSeconds(120));
//        hdfs.start();
//
//        // 3. Build and start the ingester container
//        ingester = new GenericContainer<>(DockerImageName.parse("nimbyte-ingester:latest"))
//                .withExposedPorts(8080)
//                .withEnv("SPRING_PROFILES_ACTIVE", "dev")
//                .withEnv("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers())
//                .withEnv("HDFS_URI", "hdfs://" + hdfs.getHost() + ":" + hdfs.getMappedPort(9000))
//                .withEnv("KAFKA_TOPIC", NETFLOW_TOPIC)
//                .withEnv("KAFKA_CONSUMER_GROUP", "smoke-test-group")
//                .withEnv("HDFS_BASE_PATH", "/data/test/smoke/")
//                .withEnv("HDFS_USER", "root")
//                .waitingFor(Wait.forHttp("/actuator/health").forPort(8080))
//                .withStartupTimeout(Duration.ofSeconds(120));
//        ingester.start();
//
//        // Wait for everything to settle
//        Thread.sleep(5000);
//    }
//
//    @Test
//    @DisplayName("Full end-to-end flow: Kafka -> Ingester -> HDFS")
//    void shouldProcessRecordFromKafkaToHdfs() throws Exception {
//        // Given: an Avro-encoded Netflow record matching the production schema exactly
//        byte[] avroPayload = encodeSampleRecord();
//
//        // When: producing the message to Kafka
//        produceMessage(NETFLOW_TOPIC, avroPayload);
//
//        // Then: wait for the real batch policy (up to rolling.max-age) to flush to HDFS
//        await()
//                .atMost(FLUSH_WAIT_TIMEOUT)
//                .pollInterval(Duration.ofSeconds(5))
//                .until(() -> {
//                    String hdfsUrl = "http://" + hdfs.getHost() + ":" + hdfs.getMappedPort(9870);
//                    String listUrl = hdfsUrl + "/webhdfs/v1/data/test/smoke/?op=LISTSTATUS";
//                    try {
//                        String response = restTemplate.getForObject(listUrl, String.class);
//                        return response != null && response.contains(".parquet");
//                    } catch (Exception e) {
//                        return false;
//                    }
//                });
//
//        // Verify the containerized ingester's own health endpoint is still UP
//        String health = restTemplate.getForObject(
//                "http://" + ingester.getHost() + ":" + ingester.getMappedPort(8080) + "/actuator/health",
//                String.class
//        );
//        assertThat(health).contains("\"status\":\"UP\"");
//    }
//
//    private static Schema loadAvroSchema() throws IOException {
//        try (InputStream schemaStream = new ClassPathResource(AVRO_SCHEMA_PATH).getInputStream()) {
//            return new Schema.Parser().parse(schemaStream);
//        }
//    }
//
//    private byte[] encodeSampleRecord() throws IOException {
//        GenericRecord record = new GenericData.Record(avroSchema);
//        for (Schema.Field field : avroSchema.getFields()) {
//            record.put(field.name(), sampleValueFor(field.schema()));
//        }
//
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
//        writer.write(record, encoder);
//        encoder.flush();
//        return out.toByteArray();
//    }
//
//    private Object sampleValueFor(Schema fieldSchema) {
//        return switch (fieldSchema.getType()) {
//            case STRING -> "192.168.1.100";
//            case INT -> 1;
//            case LONG -> 1L;
//            case DOUBLE -> 1.0d;
//            case FLOAT -> 1.0f;
//            case BOOLEAN -> true;
//            default -> throw new IllegalStateException(
//                    "Unsupported Avro field type in smoke test sample data: " + fieldSchema.getType());
//        };
//    }
//
//    private void produceMessage(String topic, byte[] value) {
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
//
//        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
//            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, value);
//            producer.send(record).get();
//            producer.flush();
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to produce message", e);
//        }
//    }
//}