package com.nimbo.batch.writer;

import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.support.PostgresContainerExtension;
import com.nimbo.batch.support.SparkSessionExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresWriterIT {

    private static final long ABOVE_INTEGER_MAX = 4_200_000_000L;

    @RegisterExtension
    static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

    @RegisterExtension
    static final SparkSessionExtension SPARK = new SparkSessionExtension();

    private SparkSession spark;
    private Jdbi jdbi;
    private PostgresWriter writer;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void resetDatabase() {
        spark = SPARK.spark();
        jdbi = POSTGRES.jdbi();
        POSTGRES.ensureDailyPartitions(LocalDate.of(2026, 8, 29));
        jdbi.useHandle(handle -> handle.execute("TRUNCATE traffic_aggregates, connection_aggregates, protocol_usage_aggregates, ip_bandwidth_aggregates"));
        meterRegistry = new SimpleMeterRegistry();
        PostgresConfig postgresConfig = postgresConfig();
        writer = new PostgresWriter(new TableRowMapper(), new BatchExecutor(),
                new JdbiDaoFactory(postgresConfig, executorPoolConfig()), postgresConfig,
                new BatchProcessorMetrics(meterRegistry), jdbi);
    }

    @AfterEach
    void closeRegistry() {
        JdbiDaoFactory.closeExecutorPools();
        meterRegistry.close();
    }

    @Test
    void givenThreeTrafficRowsAcrossTwoJdbcBatches_whenWritten_thenAllRowsArePersisted() {
        // Given: three deterministic traffic aggregates and a writer batch size of two
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000001");
        Dataset<Row> rows = trafficRows();

        // When: the traffic cube is written through Spark partitions and Jdbi batches
        long written = writer.writeTrafficAggregates(rows, batchId);

        // Then: all rows are acknowledged and their metrics survive persistence
        assertThat(written).isEqualTo(3L);
        assertThat(count("traffic_aggregates")).isEqualTo(3L);
        assertThat(value("SELECT SUM(in_bytes) FROM traffic_aggregates")).isEqualTo(600L);
        assertThat(value("SELECT MAX(in_packets) FROM traffic_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
        assertThat(value("SELECT MAX(record_count) FROM traffic_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
        assertThat(meterRegistry.get("batch.processor.output.rows").tag("cube", "traffic").counter().count())
                .isEqualTo(3.0);
        assertThat(meterRegistry.get("batch.processor.stage.duration")
                .tag("stage", "postgres_write_traffic").timer().count()).isEqualTo(1L);
    }

    @Test
    void givenTheSameConnectionBatchTwice_whenWritten_thenTheIpPairRowIsUpsertedExactlyOnce() {
        // Given: one simplified IP-pair connection aggregate and a fixed batch identity
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000002");
        Dataset<Row> rows = connectionRows();

        // When: the same aggregate is submitted twice
        writer.writeConnectionAggregates(rows, batchId);
        writer.writeConnectionAggregates(rows, batchId);

        // Then: the database contains one row with the original summed connection metrics
        assertThat(count("connection_aggregates")).isEqualTo(1L);
        assertThat(value("SELECT in_bytes FROM connection_aggregates")).isEqualTo(400L);
        assertThat(value("SELECT out_packets FROM connection_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
        assertThat(value("SELECT octet_length(id) FROM connection_aggregates")).isEqualTo(32L);
    }

    @Test
    void givenProtocolAndIpBandwidthRows_whenWritten_thenEachCubeUsesItsOwnRealDao() {
        // Given: deterministic protocol and bandwidth aggregates for one batch
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000003");

        // When: both cubes are written through their production DAO paths
        long protocolWritten = writer.writeProtocolAggregates(protocolRows(), batchId);
        long bandwidthWritten = writer.writeIpBandwidthAggregates(bandwidthRows(), batchId);

        // Then: each target table has the expected row and aggregate metric
        assertThat(protocolWritten).isEqualTo(1L);
        assertThat(bandwidthWritten).isEqualTo(1L);
        assertThat(value("SELECT in_packets FROM protocol_usage_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
        assertThat(value("SELECT connection_count FROM protocol_usage_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
        assertThat(value("SELECT inbound_bytes FROM ip_bandwidth_aggregates")).isEqualTo(900L);
        assertThat(value("SELECT outbound_packets FROM ip_bandwidth_aggregates")).isEqualTo(ABOVE_INTEGER_MAX);
    }

    private PostgresConfig postgresConfig() {
        PostgresConfig config = new PostgresConfig();
        config.setHost(POSTGRES.host());
        config.setPort(POSTGRES.port());
        config.setDatabase(POSTGRES.databaseName());
        config.setUsername(POSTGRES.username());
        config.setPassword(POSTGRES.password());
        config.setBatchSize(2);
        config.setWriterPartitions(1);
        config.getRetry().setMaxAttempts(2);
        config.getRetry().setInitialDelayMs(0);
        config.getRetry().setMaxDelayMs(0);
        return config;
    }

    private ExecutorPoolConfig executorPoolConfig() {
        ExecutorPoolConfig config = new ExecutorPoolConfig();
        config.setMaxPoolSize(1);
        config.setMinimumIdle(0);
        config.setConnectionTimeoutMs(5_000);
        config.setIdleTimeoutMs(60_000);
        config.setMaxLifetimeMs(1_800_000);
        config.setPoolName("writer-test-pool");
        return config;
    }

    private Dataset<Row> trafficRows() {
        StructType schema = new StructType().add("src_ip", DataTypes.StringType, false).add("window_start", DataTypes.TimestampType, false).add("window_end", DataTypes.TimestampType, false).add("in_bytes", DataTypes.LongType, false).add("out_bytes", DataTypes.LongType, false).add("in_packets", DataTypes.LongType, false).add("out_packets", DataTypes.LongType, false).add("record_count", DataTypes.LongType, false);
        return spark.createDataFrame(List.of(
                RowFactory.create("10.0.0.1", timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 100L, 200L, ABOVE_INTEGER_MAX, 2L, ABOVE_INTEGER_MAX),
                RowFactory.create("10.0.0.2", timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 200L, 300L, 2L, 3L, 2L),
                RowFactory.create("10.0.0.3", timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 300L, 400L, 3L, 4L, 3L)), schema).repartition(1);
    }

    private Dataset<Row> connectionRows() {
        StructType schema = new StructType().add("id", DataTypes.StringType, false).add("src_ip", DataTypes.StringType, false).add("dst_ip", DataTypes.StringType, false).add("window_start", DataTypes.TimestampType, false).add("window_end", DataTypes.TimestampType, false).add("in_bytes", DataTypes.LongType, false).add("out_bytes", DataTypes.LongType, false).add("in_packets", DataTypes.LongType, false).add("out_packets", DataTypes.LongType, false);
        return spark.createDataFrame(List.of(RowFactory.create("b".repeat(64), "10.0.0.1", "10.0.0.2", timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 400L, 600L, 4L, ABOVE_INTEGER_MAX)), schema);
    }

    private Dataset<Row> protocolRows() {
        StructType schema = new StructType().add("l7_protocol", DataTypes.DoubleType, false).add("window_start", DataTypes.TimestampType, false).add("window_end", DataTypes.TimestampType, false).add("in_bytes", DataTypes.LongType, false).add("out_bytes", DataTypes.LongType, false).add("in_packets", DataTypes.LongType, false).add("out_packets", DataTypes.LongType, false).add("connection_count", DataTypes.LongType, false);
        return spark.createDataFrame(List.of(RowFactory.create(6.0, timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 700L, 800L, ABOVE_INTEGER_MAX, 8L, ABOVE_INTEGER_MAX)), schema);
    }

    private Dataset<Row> bandwidthRows() {
        StructType schema = new StructType().add("ip", DataTypes.StringType, false).add("window_start", DataTypes.TimestampType, false).add("window_end", DataTypes.TimestampType, false).add("inbound_bytes", DataTypes.LongType, false).add("outbound_bytes", DataTypes.LongType, false).add("inbound_packets", DataTypes.LongType, false).add("outbound_packets", DataTypes.LongType, false);
        return spark.createDataFrame(List.of(RowFactory.create("10.0.0.2", timestamp("2026-08-29T08:00:00Z"), timestamp("2026-08-29T09:00:00Z"), 900L, 1000L, 9L, ABOVE_INTEGER_MAX)), schema);
    }

    private Timestamp timestamp(String value) {
        return Timestamp.from(Instant.parse(value));
    }

    private long count(String table) {
        return value("SELECT COUNT(*) FROM " + table);
    }

    private long value(String sql) {
        return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Long.class).one());
    }
}
