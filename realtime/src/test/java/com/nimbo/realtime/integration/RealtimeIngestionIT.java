package com.nimbo.realtime.integration;

import com.nimbo.realtime.RealtimeApplication;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.StreamUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        classes = RealtimeApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(initializers = RealtimeIngestionIT.InfrastructureInitializer.class)
class RealtimeIngestionIT {

    private static final String TOPIC = "netflow-integration-test";
    private static final String CONSUMER_GROUP = "realtime-integration-test";
    private static final int FE_HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;
    private static final int MYSQL_PORT = 9030;

    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0"));

    private static final GenericContainer<?> STARROCKS = new GenericContainer<>(
            DockerImageName.parse("starrocks/allin1-ubuntu:3.4-latest"))
            .withExposedPorts(FE_HTTP_PORT, BE_HTTP_PORT, MYSQL_PORT)
            .waitingFor(Wait.forHttp("/api/health")
                    .forPort(FE_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(4)));

    @Test
    void shouldRunTheCompleteKafkaToStarRocksLifecycle() throws Exception {
        waitUntilConsumerHasJoinedGroup();

        Schema schema = loadAvroSchema();
        long firstTimestamp = 1_725_000_000_000L;
        GenericRecord first = validNetflowRecord(schema, "192.0.2.10", 1234L);
        GenericRecord invalid = validNetflowRecord(schema, "not-an-ip", 9999L);
        GenericRecord second = validNetflowRecord(schema, "192.0.2.11", 5678L);
        GenericRecord otherPartitionFirst = validNetflowRecord(schema, "192.0.2.20", 2000L);
        GenericRecord otherPartitionSecond = validNetflowRecord(schema, "192.0.2.21", 2100L);

        assertThat(produce(first, schema, firstTimestamp)).isZero();
        assertThat(produce(invalid, schema, firstTimestamp + 1)).isEqualTo(1L);
        assertThat(produceBytes(new byte[0], firstTimestamp + 2)).isEqualTo(2L);
        assertThat(produce(second, schema, firstTimestamp + 3)).isEqualTo(3L);
        assertThat(produce(otherPartitionFirst, schema, firstTimestamp + 10, 1)).isZero();
        assertThat(produce(otherPartitionSecond, schema, firstTimestamp + 11, 1)).isEqualTo(1L);

        awaitRowCount(4);
        awaitCommittedOffset(0, 4L);
        awaitCommittedOffset(1, 2L);
        assertPersistedRow("192.0.2.10", 0, 0L, firstTimestamp, 1234L);
        assertPersistedRow("192.0.2.11", 0, 3L, firstTimestamp + 3, 5678L);
        assertPersistedRow("192.0.2.20", 1, 0L, firstTimestamp + 10, 2000L);
        assertPersistedRow("192.0.2.21", 1, 1L, firstTimestamp + 11, 2100L);
        assertThat(countRows("not-an-ip")).isZero();

        GenericRecord third = validNetflowRecord(schema, "192.0.2.12", 9012L);
        GenericRecord otherPartitionThird = validNetflowRecord(schema, "192.0.2.22", 2200L);
        assertThat(produce(third, schema, firstTimestamp + 4)).isEqualTo(4L);
        assertThat(produce(otherPartitionThird, schema, firstTimestamp + 12, 1)).isEqualTo(2L);

        awaitRowCount(6);
        awaitCommittedOffset(0, 5L);
        awaitCommittedOffset(1, 3L);
        assertPersistedRow("192.0.2.12", 0, 4L, firstTimestamp + 4, 9012L);
        assertPersistedRow("192.0.2.22", 1, 2L, firstTimestamp + 12, 2200L);
    }

    private static long produce(GenericRecord genericRecord, Schema schema, long timestamp) throws Exception {
        return produce(genericRecord, schema, timestamp, 0);
    }

    private static long produce(GenericRecord genericRecord, Schema schema, long timestamp, int partition) throws Exception {
        return produceBytes(serialize(genericRecord, schema), timestamp, partition);
    }

    private static long produceBytes(byte[] payload, long timestamp) throws Exception {
        return produceBytes(payload, timestamp, 0);
    }

    private static long produceBytes(byte[] payload, long timestamp, int partition) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            return producer.send(new ProducerRecord<>(TOPIC, partition, timestamp, null, payload))
                    .get(30, TimeUnit.SECONDS).offset();
        }
    }

    private static void awaitRowCount(long expected) {
        awaitCondition("Expected " + expected + " rows in StarRocks", () -> countRows(null) == expected);
    }

    private static void awaitCommittedOffset(int partitionNumber, long expected) {
        TopicPartition partition = new TopicPartition(TOPIC, partitionNumber);
        awaitCondition("Expected Kafka committed offset " + expected + " for partition " + partitionNumber, () -> {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
            try (AdminClient admin = AdminClient.create(properties)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                return offsets.containsKey(partition) && offsets.get(partition).offset() == expected;
            }
        });
    }

    private static long countRows(String sourceIp) throws Exception {
        String where = sourceIp == null ? "" : " WHERE ipv4_src_addr = '" + sourceIp + "'";
        try (Connection connection = openStarRocksConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM nimbo.netflow_realtime_v3" + where)) {
            result.next();
            return result.getLong(1);
        }
    }

    private static void assertPersistedRow(String sourceIp, int expectedPartition, long expectedOffset,
                                           long expectedTimestamp, long expectedBytes) throws Exception {
        try (Connection connection = openStarRocksConnection();
             Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("""
                     SELECT kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
                            ipv4_dst_addr, in_bytes
                     FROM nimbo.netflow_realtime_v3
                     WHERE ipv4_src_addr = '%s'
                     """.formatted(sourceIp))) {
            assertThat(row.next()).isTrue();
            assertThat(row.getString("kafka_topic")).isEqualTo(TOPIC);
            assertThat(row.getInt("kafka_partition")).isEqualTo(expectedPartition);
            assertThat(row.getLong("kafka_offset")).isEqualTo(expectedOffset);
            assertThat(row.getLong("kafka_timestamp")).isEqualTo(expectedTimestamp);
            assertThat(row.getString("ipv4_dst_addr")).isEqualTo("198.51.100.20");
            assertThat(row.getLong("in_bytes")).isEqualTo(expectedBytes);
            assertThat(row.next()).isFalse();
        }
    }

    private static void awaitCondition(String failureMessage, Callable<Boolean> condition) {
        await().alias(failureMessage)
                .pollInterval(Duration.ofMillis(500))
                .atMost(Duration.ofSeconds(60))
                .until(condition);
    }

    private static void waitUntilConsumerHasJoinedGroup() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            await().alias("both realtime consumers to join their Kafka group")
                    .ignoreExceptions()
                    .pollInterval(Duration.ofMillis(250))
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> admin.describeConsumerGroups(List.of(CONSUMER_GROUP))
                            .describedGroups().get(CONSUMER_GROUP).get(5, TimeUnit.SECONDS)
                            .members().size() == 2);
        }
    }

    private static Schema loadAvroSchema() throws Exception {
        return new Schema.Parser().parse(new ClassPathResource("avro/NetFlowV2Record.avsc").getInputStream());
    }

    private static GenericRecord validNetflowRecord(Schema schema, String sourceIp, long inBytes) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = switch (field.schema().getType()) {
                case STRING -> "value";
                case INT -> 1;
                case LONG -> 1L;
                case DOUBLE -> 1.0d;
                case FLOAT -> 1.0f;
                default -> throw new IllegalArgumentException("Unsupported test field: " + field.name());
            };
            genericRecord.put(field.name(), value);
        }
        genericRecord.put("IPV4_SRC_ADDR", sourceIp);
        genericRecord.put("IPV4_DST_ADDR", "198.51.100.20");
        genericRecord.put("IN_BYTES", inBytes);
        return genericRecord;
    }

    private static byte[] serialize(GenericRecord genericRecord, Schema schema) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        new GenericDatumWriter<GenericRecord>(schema).write(genericRecord, encoder);
        encoder.flush();
        return output.toByteArray();
    }

    private static Connection openStarRocksConnection() throws Exception {
        return DriverManager.getConnection(
                "jdbc:mysql://" + STARROCKS.getHost() + ":" + STARROCKS.getMappedPort(MYSQL_PORT),
                "root", "");
    }

    static class InfrastructureInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext context) {
            Startables.deepStart(KAFKA, STARROCKS).join();
            provisionKafka();
            provisionStarRocks();

            TestPropertyValues.of(
                    "spring.datasource.url=jdbc:mysql://" + STARROCKS.getHost() + ":"
                            + STARROCKS.getMappedPort(MYSQL_PORT) + "/nimbo",
                    "spring.datasource.username=root",
                    "spring.datasource.password=",
                    "realtime.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                    "realtime.kafka.topic=" + TOPIC,
                    "realtime.kafka.consumer-group=" + CONSUMER_GROUP,
                    "realtime.kafka.client-id=realtime-integration-test",
                    "realtime.kafka.auto-offset-reset=earliest",
                    "realtime.kafka.max-poll-records=10",
                    "realtime.kafka.poll-timeout-ms=100",
                    "realtime.kafka.max-partition-fetch-bytes=1048576",
                    "realtime.kafka.consumer-count=2",
                    "realtime.starrocks.stream-load-backends[0]=http://" + STARROCKS.getHost() + ":"
                            + STARROCKS.getMappedPort(BE_HTTP_PORT),
                    "realtime.starrocks.stream-load-backends[1]=http://" + STARROCKS.getHost() + ":"
                            + STARROCKS.getMappedPort(BE_HTTP_PORT) + "/",
                    "realtime.starrocks.database=nimbo",
                    "realtime.starrocks.table=netflow_realtime_v3",
                    "realtime.starrocks.username=root",
                    "realtime.starrocks.password=",
                    "realtime.starrocks.merge-commit-enabled=false",
                    "realtime.starrocks.retry-backoff-ms=100",
                    "realtime.starrocks.merge-commit-interval-ms=100",
                    "realtime.starrocks.merge-commit-parallel=1",
                    "realtime.starrocks.max-batch-size-rows=2",
                    "realtime.starrocks.max-batch-time-ms=100",
                    "realtime.starrocks.max-in-flight-requests=2")
                    .applyTo(context.getEnvironment());
        }

        private static void provisionKafka() {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
            try (AdminClient admin = AdminClient.create(properties)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get(30, TimeUnit.SECONDS);
            } catch (Exception exception) {
                throw new IllegalStateException("Could not create the integration-test Kafka topic", exception);
            }
        }

        private static void provisionStarRocks() {
            await().alias("StarRocks BE capacity and integration-test schema")
                    .ignoreExceptions()
                    .pollInterval(Duration.ofSeconds(1))
                    .atMost(Duration.ofMinutes(2))
                    .until(() -> {
                        try (Connection connection = openStarRocksConnection();
                             Statement statement = connection.createStatement()) {
                            statement.execute("CREATE DATABASE IF NOT EXISTS nimbo");
                            statement.execute("DROP TABLE IF EXISTS nimbo.netflow_realtime_v3");
                            String sql = StreamUtils.copyToString(
                                    new ClassPathResource("sql/netflow-realtime.sql").getInputStream(),
                                    StandardCharsets.UTF_8);
                            sql = sql.replace("BUCKETS 8",
                                    "BUCKETS 1\nPROPERTIES (\"replication_num\" = \"1\")");
                            statement.execute(sql);
                            return true;
                        }
                    });
        }
    }
}
