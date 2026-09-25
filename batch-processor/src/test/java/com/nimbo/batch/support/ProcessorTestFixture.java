package com.nimbo.batch.support;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.writer.BatchExecutor;
import com.nimbo.batch.writer.JdbiDaoFactory;
import com.nimbo.batch.writer.PostgresWriter;
import com.nimbo.batch.writer.TableRowMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

public final class ProcessorTestFixture {

    private static final StructType NETFLOW_SCHEMA = new StructType()
            .add("IPV4_SRC_ADDR", DataTypes.StringType, false)
            .add("IPV4_DST_ADDR", DataTypes.StringType, false)
            .add("L4_SRC_PORT", DataTypes.IntegerType, false)
            .add("L4_DST_PORT", DataTypes.IntegerType, false)
            .add("PROTOCOL", DataTypes.IntegerType, false)
            .add("L7_PROTO", DataTypes.DoubleType, false)
            .add("IN_BYTES", DataTypes.LongType, false)
            .add("OUT_BYTES", DataTypes.LongType, false)
            .add("IN_PKTS", DataTypes.LongType, false)
            .add("OUT_PKTS", DataTypes.LongType, false)
            .add("PROCESS_TIME", DataTypes.LongType, false);

    private final PostgresContainerExtension postgres;
    private final SparkSession spark;
    private final Jdbi jdbi;

    public ProcessorTestFixture(PostgresContainerExtension postgres, SparkSession spark) {
        this.postgres = postgres;
        this.spark = spark;
        this.jdbi = postgres.jdbi();
    }

    public void resetDatabase() {
        postgres.ensureDailyPartitions(LocalDate.of(2026, 8, 29));
        jdbi.useHandle(handle -> handle.execute(
                "TRUNCATE traffic_aggregates, connection_aggregates, protocol_usage_aggregates, ip_bandwidth_aggregates"));
    }

    public Dataset<Row> standardFlows() {
        return spark.createDataFrame(List.of(
                flow("10.0.0.1", "10.0.0.2", 12345, 443, 6, 6.0, 100, 200, 1, 2, "2026-08-29T08:15:00Z"),
                flow("10.0.0.1", "10.0.0.2", 54321, 8443, 17, 6.0, 300, 400, 3, 4, "2026-08-29T08:45:00Z"),
                flow("10.0.0.1", "10.0.0.3", 12346, 53, 17, 17.0, 50, 70, 5, 7, "2026-08-29T08:30:00Z"),
                flow("10.0.0.4", "10.0.0.1", 1111, 22, 6, 6.0, 20, 30, 2, 3, "2026-08-29T09:05:00Z")
        ), NETFLOW_SCHEMA);
    }

    public Dataset<Row> emptyFlows() {
        return spark.createDataFrame(List.<Row>of(), NETFLOW_SCHEMA);
    }

    public Dataset<Row> flowsMissingRequiredColumns() {
        return spark.createDataFrame(
                List.of(RowFactory.create("10.0.0.1")),
                new StructType().add("IPV4_SRC_ADDR", DataTypes.StringType, false));
    }

    public BatchConfig batchConfig() {
        BatchConfig config = new BatchConfig();
        config.setWindowDuration("1 hour");
        return config;
    }

    public OutputConfig databaseOutput() {
        OutputConfig config = new OutputConfig();
        config.setMode(OutputConfig.Mode.DATABASE);
        return config;
    }

    public PostgresWriter postgresWriter() {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost(postgres.host());
        postgresConfig.setPort(postgres.port());
        postgresConfig.setDatabase(postgres.databaseName());
        postgresConfig.setUsername(postgres.username());
        postgresConfig.setPassword(postgres.password());
        postgresConfig.setBatchSize(2);
        postgresConfig.setWriterPartitions(1);
        postgresConfig.getRetry().setMaxAttempts(2);
        postgresConfig.getRetry().setInitialDelayMs(1);
        postgresConfig.getRetry().setMaxDelayMs(2);

        ExecutorPoolConfig executorPool = new ExecutorPoolConfig();
        executorPool.setMaxPoolSize(1);
        executorPool.setMinimumIdle(0);
        executorPool.setConnectionTimeoutMs(5_000);
        executorPool.setIdleTimeoutMs(60_000);
        executorPool.setMaxLifetimeMs(1_800_000);
        executorPool.setPoolName("processor-test-executor-pool");

        return new PostgresWriter(
                new TableRowMapper(),
                new BatchExecutor(),
                new JdbiDaoFactory(postgresConfig, executorPool),
                postgresConfig,
                new BatchProcessorMetrics(new SimpleMeterRegistry()),
                jdbi);
    }

    public long countRows(String table) {
        return queryLong("SELECT COUNT(*) FROM " + table);
    }

    public long queryLong(String sql) {
        return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Long.class).one());
    }

    public String queryString(String sql) {
        return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(String.class).one());
    }

    private Row flow(
            String srcIp,
            String dstIp,
            int srcPort,
            int dstPort,
            int protocol,
            double l7Protocol,
            long inBytes,
            long outBytes,
            long inPackets,
            long outPackets,
            String timestamp) {
        return RowFactory.create(
                srcIp,
                dstIp,
                srcPort,
                dstPort,
                protocol,
                l7Protocol,
                inBytes,
                outBytes,
                inPackets,
                outPackets,
                Instant.parse(timestamp).toEpochMilli() * 1_000);
    }
}
