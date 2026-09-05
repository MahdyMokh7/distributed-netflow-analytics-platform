package com.nimbo.batch.integration;

import com.nimbo.batch.BatchProcessorApplication;
import com.nimbo.batch.integration.config.TestFileSystemConfig;
import com.nimbo.batch.integration.config.TestParquetProperties;
import com.nimbo.batch.integration.config.TestPostgresProperties;
import com.nimbo.batch.job.BatchJobRunner;
import com.nimbo.batch.model.*;
import com.nimbo.batch.integration.util.TestDataGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = BatchProcessorApplication.class)
@EnableConfigurationProperties({TestParquetProperties.class, TestPostgresProperties.class})
@Testcontainers
@ActiveProfiles("test")
@Import(TestFileSystemConfig.class)
public class BatchProcessorModuleTest {

    @Autowired
    private TestPostgresProperties postgresProperties;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.2-alpine")
            .withDatabaseName("test_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    @Autowired
    private BatchJobRunner batchJobRunner;

    @Autowired
    private SparkSession spark;

    @Autowired
    private Jdbi jdbi;

    @Autowired
    private TestParquetProperties testProperties;

    @TempDir
    static java.nio.file.Path tempDir;

    private Path parquetRoot;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        String hdfsUri = "file://" + tempDir.toString().replace('\\', '/');
        registry.add("hdfs.uri", () -> hdfsUri);
        registry.add("hdfs.parquet-path", () -> "/");

        registry.add("spring.flyway.url", postgres::getJdbcUrl);
        registry.add("spring.flyway.user", postgres::getUsername);
        registry.add("spring.flyway.password", postgres::getPassword);

        registry.add("postgres.host", postgres::getHost);
        registry.add("postgres.port", () -> postgres.getMappedPort(5432).toString());
        registry.add("postgres.database", postgres::getDatabaseName);
        registry.add("postgres.username", postgres::getUsername);
        registry.add("postgres.password", postgres::getPassword);
    }

    @BeforeEach
    void setUp() {
        parquetRoot = new Path(tempDir.toString());
    }

    @Test
    void testFullBatchRun_success() {
        Instant base = testProperties.getBaseTimestamp();
        int numFiles = testProperties.getFullBatchFiles();
        int recordsPerFile = testProperties.getRecordsPerFile();
        TestDataGenerator.writeFiles(spark, parquetRoot, numFiles, recordsPerFile, base);

        batchJobRunner.runBatchJob();

        List<TrafficAggregate> traffic = jdbi.withHandle(handle ->
                handle.createQuery("SELECT * FROM traffic_aggregates ORDER BY src_ip, window_start")
                        .mapToBean(TrafficAggregate.class)
                        .list()
        );
        List<ConnectionAggregate> connections = jdbi.withHandle(handle ->
                handle.createQuery("SELECT * FROM connection_aggregates ORDER BY src_ip, window_start")
                        .mapToBean(ConnectionAggregate.class)
                        .list()
        );
        List<ProtocolAggregate> protocols = jdbi.withHandle(handle ->
                handle.createQuery("SELECT * FROM protocol_usage_aggregates ORDER BY l7_protocol, window_start")
                        .mapToBean(ProtocolAggregate.class)
                        .list()
        );
        List<TopIpAggregate> topIps = jdbi.withHandle(handle ->
                handle.createQuery("SELECT * FROM ip_bandwidth_aggregates ORDER BY ip, window_start")
                        .mapToBean(TopIpAggregate.class)
                        .list()
        );

        ExpectedAggregates expected = computeExpectedAggregates(numFiles, recordsPerFile, base);

        assertThat(traffic).hasSize(expected.trafficCount);
        assertThat(connections).hasSize(expected.connectionCount);
        assertThat(protocols).hasSize(expected.protocolCount);
        assertThat(topIps).hasSize(expected.topIpCount);

        String testIp = testProperties.getKnownIps().get(0);
        TrafficAggregate sampleTraffic = traffic.stream()
                .filter(t -> testIp.equals(t.getSrcIp()))
                .findFirst().orElseThrow();
        assertThat(sampleTraffic.getInBytes()).isEqualTo(expected.expectedInBytesForIp.get(testIp));
        assertThat(sampleTraffic.getOutBytes()).isEqualTo(expected.expectedOutBytesForIp.get(testIp));

        ConnectionAggregate sampleConn = connections.stream()
                .filter(c -> testIp.equals(c.getSrcIp()) && testProperties.getKnownIps().get(1).equals(c.getDstIp()))
                .findFirst().orElseThrow();
        assertThat(sampleConn.getInBytes()).isPositive();

        ProtocolAggregate sampleProto = protocols.stream()
                .filter(p -> p.getL7Protocol() == 6.0)
                .findFirst().orElseThrow();
        assertThat(sampleProto.getConnectionCount()).isPositive();

        TopIpAggregate sampleTop = topIps.stream()
                .filter(t -> testIp.equals(t.getIp()))
                .findFirst().orElseThrow();
        assertThat(sampleTop.getOutboundBytes()).isPositive();

        long processedFiles = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM batch_processor_state")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(processedFiles).isEqualTo(numFiles);
    }

    @Test
    void testIncrementalProcessing_skipProcessedFiles() {
        Instant base = testProperties.getBaseTimestamp();
        int firstBatchFiles = testProperties.getIncrementalFirstBatch();
        int recordsPerFile = testProperties.getRecordsPerFile() / 2;
        TestDataGenerator.writeFiles(spark, parquetRoot, firstBatchFiles, recordsPerFile, base);
        batchJobRunner.runBatchJob();

        int additionalFiles = testProperties.getIncrementalAdditional();
        Instant newBase = base.plusSeconds(120);
        TestDataGenerator.writeFiles(spark, parquetRoot, additionalFiles, recordsPerFile, newBase);
        batchJobRunner.runBatchJob();

        long totalProcessed = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM batch_processor_state")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(totalProcessed).isEqualTo(firstBatchFiles + additionalFiles);

        long distinct = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(DISTINCT file_name) FROM batch_processor_state")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(distinct).isEqualTo(firstBatchFiles + additionalFiles);
    }

    @Test
    void testMaxFilesPerBatch() {
        Instant base = testProperties.getBaseTimestamp();
        int totalFiles = testProperties.getMaxFilesTestTotal();
        int recordsPerFile = testProperties.getRecordsPerFile() / 4;
        TestDataGenerator.writeFiles(spark, parquetRoot, totalFiles, recordsPerFile, base);

        batchJobRunner.runBatchJob();

        long processed = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM batch_processor_state")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(processed).isEqualTo(5);
    }

    @Test
    void testEmptyDataset() {
        batchJobRunner.runBatchJob();

        long processed = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM batch_processor_state")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(processed).isZero();

        long trafficCount = jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM traffic_aggregates")
                        .mapTo(Long.class)
                        .one()
        );
        assertThat(trafficCount).isZero();
    }

    private ExpectedAggregates computeExpectedAggregates(int numFiles, int recordsPerFile, Instant base) {
        // Use maps to simulate Spark's grouping
        // Key: src_ip + window_start (truncated to hour)
        Map<String, TrafficAggregate> trafficMap = new HashMap<>();
        // Key: src_ip + dst_ip + src_port + dst_port + protocol + window_start
        Map<String, ConnectionAggregate> connectionMap = new HashMap<>();
        // Key: l7_protocol + window_start
        Map<String, ProtocolAggregate> protocolMap = new HashMap<>();
        // Key: ip + window_start
        Map<String, TopIpAggregate> topIpMap = new HashMap<>();

        for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
            int partition = fileIdx % 3;
            List<org.apache.spark.sql.Row> rows = TestDataGenerator.generateRowsForFile(fileIdx, recordsPerFile, base, partition);
            for (org.apache.spark.sql.Row row : rows) {
                String srcIp = row.getString(0);
                String dstIp = row.getString(1);
                double l7Proto = row.getDouble(5);
                long inBytes = row.getLong(6);
                long outBytes = row.getLong(7);
                long inPackets = row.getLong(8);
                long outPackets = row.getLong(9);
                long processingTimeMicros = row.getLong(43);

                // Convert micros to Instant and truncate to hour window
                Instant recordTime = Instant.ofEpochSecond(
                        processingTimeMicros / 1_000_000,
                        (processingTimeMicros % 1_000_000) * 1000
                );
                Instant windowStart = recordTime.truncatedTo(ChronoUnit.HOURS);
                Instant windowEnd = windowStart.plus(1, ChronoUnit.HOURS);

                // 1. Traffic Aggregation: group by (src_ip, window)
                String trafficKey = srcIp + "|" + windowStart;
                TrafficAggregate trafficAgg = trafficMap.get(trafficKey);
                if (trafficAgg == null) {
                    trafficAgg = TrafficAggregate.builder()
                            .srcIp(srcIp)
                            .windowStart(windowStart.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .windowEnd(windowEnd.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .inBytes(0)
                            .outBytes(0)
                            .inPackets(0)
                            .outPackets(0)
                            .recordCount(0)
                            .build();
                    trafficMap.put(trafficKey, trafficAgg);
                }
                trafficAgg.setInBytes(trafficAgg.getInBytes() + inBytes);
                trafficAgg.setOutBytes(trafficAgg.getOutBytes() + outBytes);
                trafficAgg.setInPackets(trafficAgg.getInPackets() + inPackets);
                trafficAgg.setOutPackets(trafficAgg.getOutPackets() + outPackets);
                trafficAgg.setRecordCount(trafficAgg.getRecordCount() + 1);

                // 2. Connection Aggregation: group by (src_ip, dst_ip, window)
                String connKey = srcIp + "|" + dstIp + "|" + windowStart;
                ConnectionAggregate connAgg = connectionMap.get(connKey);
                if (connAgg == null) {
                    String id = sha256(srcIp + "|" + dstIp + "|" + windowStart);
                    connAgg = ConnectionAggregate.builder()
                            .id(id)
                            .srcIp(srcIp)
                            .dstIp(dstIp)
                            .windowStart(windowStart.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .windowEnd(windowEnd.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .inBytes(0)
                            .outBytes(0)
                            .inPackets(0)
                            .outPackets(0)
                            .build();
                    connectionMap.put(connKey, connAgg);
                }
                connAgg.setInBytes(connAgg.getInBytes() + inBytes);
                connAgg.setOutBytes(connAgg.getOutBytes() + outBytes);
                connAgg.setInPackets(connAgg.getInPackets() + inPackets);
                connAgg.setOutPackets(connAgg.getOutPackets() + outPackets);

                // 3. Protocol Aggregation: group by (l7_protocol, window)
                String protoKey = l7Proto + "|" + windowStart;
                ProtocolAggregate protoAgg = protocolMap.get(protoKey);
                if (protoAgg == null) {
                    protoAgg = ProtocolAggregate.builder()
                            .l7Protocol(l7Proto)
                            .windowStart(windowStart.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .windowEnd(windowEnd.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .inBytes(0)
                            .outBytes(0)
                            .inPackets(0)
                            .outPackets(0)
                            .connectionCount(0)
                            .build();
                    protocolMap.put(protoKey, protoAgg);
                }
                protoAgg.setInBytes(protoAgg.getInBytes() + inBytes);
                protoAgg.setOutBytes(protoAgg.getOutBytes() + outBytes);
                protoAgg.setInPackets(protoAgg.getInPackets() + inPackets);
                protoAgg.setOutPackets(protoAgg.getOutPackets() + outPackets);
                protoAgg.setConnectionCount(protoAgg.getConnectionCount() + 1);

                // 4. Top IP Aggregation: group by (ip, window) - both src and dst perspectives
                // Source perspective
                String topIpKeySrc = srcIp + "|" + windowStart;
                TopIpAggregate topIpAgg = topIpMap.get(topIpKeySrc);
                if (topIpAgg == null) {
                    topIpAgg = TopIpAggregate.builder()
                            .ip(srcIp)
                            .windowStart(windowStart.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .windowEnd(windowEnd.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .inboundBytes(0)
                            .outboundBytes(0)
                            .inboundPackets(0)
                            .outboundPackets(0)
                            .build();
                    topIpMap.put(topIpKeySrc, topIpAgg);
                }
                topIpAgg.setOutboundBytes(topIpAgg.getOutboundBytes() + outBytes);
                topIpAgg.setOutboundPackets(topIpAgg.getOutboundPackets() + outPackets);

                // Destination perspective (inbound for dst = outbound from src perspective)
                String topIpKeyDst = dstIp + "|" + windowStart;
                TopIpAggregate topIpAggDst = topIpMap.get(topIpKeyDst);
                if (topIpAggDst == null) {
                    topIpAggDst = TopIpAggregate.builder()
                            .ip(dstIp)
                            .windowStart(windowStart.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .windowEnd(windowEnd.atZone(ZoneOffset.UTC).toLocalDateTime())
                            .inboundBytes(0)
                            .outboundBytes(0)
                            .inboundPackets(0)
                            .outboundPackets(0)
                            .build();
                    topIpMap.put(topIpKeyDst, topIpAggDst);
                }
                topIpAggDst.setInboundBytes(topIpAggDst.getInboundBytes() + inBytes);
                topIpAggDst.setInboundPackets(topIpAggDst.getInboundPackets() + inPackets);
            }
        }

        // Build the expected aggregates
        List<TrafficAggregate> trafficAggregates = new ArrayList<>(trafficMap.values());
        List<ConnectionAggregate> connectionAggregates = new ArrayList<>(connectionMap.values());
        List<ProtocolAggregate> protocolAggregates = new ArrayList<>(protocolMap.values());
        List<TopIpAggregate> topIpAggregates = new ArrayList<>(topIpMap.values());

        // For assertions, we need the counts and specific IP sums
        Map<String, Long> expectedInBytesForIp = new HashMap<>();
        Map<String, Long> expectedOutBytesForIp = new HashMap<>();

        for (TrafficAggregate agg : trafficAggregates) {
            expectedInBytesForIp.merge(agg.getSrcIp(), agg.getInBytes(), Long::sum);
            expectedOutBytesForIp.merge(agg.getSrcIp(), agg.getOutBytes(), Long::sum);
        }

        return new ExpectedAggregates(
                trafficAggregates.size(),
                connectionAggregates.size(),
                protocolAggregates.size(),
                topIpAggregates.size(),
                expectedInBytesForIp,
                expectedOutBytesForIp
        );
    }

    // Helper method for SHA-256
    private static String sha256(String input) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ExpectedAggregates {
        final int trafficCount;
        final int connectionCount;
        final int protocolCount;
        final int topIpCount;
        final Map<String, Long> expectedInBytesForIp;
        final Map<String, Long> expectedOutBytesForIp;

        ExpectedAggregates(int trafficCount, int connectionCount, int protocolCount,
                           int topIpCount, Map<String, Long> expectedInBytesForIp,
                           Map<String, Long> expectedOutBytesForIp) {
            this.trafficCount = trafficCount;
            this.connectionCount = connectionCount;
            this.protocolCount = protocolCount;
            this.topIpCount = topIpCount;
            this.expectedInBytesForIp = expectedInBytesForIp;
            this.expectedOutBytesForIp = expectedOutBytesForIp;
        }
    }
}
