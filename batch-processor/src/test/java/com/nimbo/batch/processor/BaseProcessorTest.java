package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.writer.BatchExecutor;
import com.nimbo.batch.writer.JdbiDaoFactory;
import com.nimbo.batch.writer.PostgresWriter;
import com.nimbo.batch.writer.TableRowMapper;
import com.nimbo.batch.util.TestPostgresExtension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Instant;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class BaseProcessorTest {

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
            .add("PROCESSING_TIME", DataTypes.LongType, false);

    @RegisterExtension
    static final TestPostgresExtension POSTGRES = new TestPostgresExtension();

    protected SparkSession spark;
    protected Jdbi jdbi;

    @BeforeAll
    void startSparkAndCreateSchema() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName(getClass().getSimpleName())
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        jdbi = POSTGRES.jdbi();
        createSchema();
    }

    @BeforeEach
    void resetDatabase() {
        jdbi.useHandle(handle -> handle.execute("TRUNCATE traffic_aggregates, connection_aggregates, protocol_usage_aggregates, ip_bandwidth_aggregates"));
    }

    @AfterAll
    void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    protected Dataset<Row> standardFlows() {
        return spark.createDataFrame(List.of(
                flow("10.0.0.1", "10.0.0.2", 12345, 443, 6, 6.0, 100, 200, 1, 2, "2026-08-29T08:15:00Z"),
                flow("10.0.0.1", "10.0.0.2", 54321, 8443, 17, 6.0, 300, 400, 3, 4, "2026-08-29T08:45:00Z"),
                flow("10.0.0.1", "10.0.0.3", 12346, 53, 17, 17.0, 50, 70, 5, 7, "2026-08-29T08:30:00Z"),
                flow("10.0.0.4", "10.0.0.1", 1111, 22, 6, 6.0, 20, 30, 2, 3, "2026-08-29T09:05:00Z")
        ), NETFLOW_SCHEMA);
    }

    protected Dataset<Row> emptyFlows() {
        return spark.createDataFrame(List.<Row>of(), NETFLOW_SCHEMA);
    }

    protected Dataset<Row> flowsMissingRequiredColumns() {
        return spark.createDataFrame(List.of(RowFactory.create("10.0.0.1")),
                new StructType().add("IPV4_SRC_ADDR", DataTypes.StringType, false));
    }

    protected BatchConfig batchConfig() {
        BatchConfig config = new BatchConfig();
        config.setWindowDuration("1 hour");
        return config;
    }

    protected OutputConfig databaseOutput() {
        OutputConfig config = new OutputConfig();
        config.setMode(OutputConfig.Mode.DATABASE);
        return config;
    }

    protected PostgresWriter postgresWriter() {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost(POSTGRES.host());
        postgresConfig.setPort(POSTGRES.port());
        postgresConfig.setDatabase(POSTGRES.database());
        postgresConfig.setUsername(POSTGRES.username());
        postgresConfig.setPassword(POSTGRES.password());
        postgresConfig.setBatchSize(2);
        postgresConfig.getRetry().setMaxAttempts(2);
        postgresConfig.getRetry().setInitialDelayMs(1);
        postgresConfig.getRetry().setMaxDelayMs(2);

        ExecutorPoolConfig executorPool = new ExecutorPoolConfig();
        executorPool.setMaxPoolSize(2);
        executorPool.setConnectionTimeoutMs(5_000);
        executorPool.setIdleTimeoutMs(60_000);
        executorPool.setMaxLifetimeMs(120_000);
        executorPool.setPoolName("processor-test-executor-pool");

        return new PostgresWriter(new TableRowMapper(), new BatchExecutor(),
                new JdbiDaoFactory(postgresConfig, executorPool), postgresConfig);
    }

    protected long countRows(String table) {
        return jdbi.withHandle(handle -> handle.createQuery("SELECT COUNT(*) FROM " + table)
                .mapTo(Long.class)
                .one()
                .longValue());
    }

    protected long queryLong(String sql) {
        return jdbi.withHandle(handle -> handle.createQuery(sql)
                .mapTo(Long.class)
                .one()
                .longValue());
    }

    protected String queryString(String sql) {
        return jdbi.withHandle(handle -> handle.createQuery(sql)
                .mapTo(String.class)
                .one());
    }

    private Row flow(String srcIp, String dstIp, int srcPort, int dstPort, int protocol, double l7Protocol,
                     long inBytes, long outBytes, long inPackets, long outPackets, String timestamp) {
        return RowFactory.create(srcIp, dstIp, srcPort, dstPort, protocol, l7Protocol, inBytes, outBytes,
                inPackets, outPackets, Instant.parse(timestamp).toEpochMilli() * 1_000);
    }

    private void createSchema() {
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS traffic_aggregates (src_ip VARCHAR NOT NULL, window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL, in_bytes BIGINT NOT NULL, out_bytes BIGINT NOT NULL, in_packets BIGINT NOT NULL, out_packets BIGINT NOT NULL, record_count BIGINT NOT NULL, batch_id UUID NOT NULL, ingested_at TIMESTAMP NOT NULL, PRIMARY KEY (src_ip, window_start, batch_id))");
            handle.execute("CREATE TABLE IF NOT EXISTS connection_aggregates (id VARCHAR NOT NULL, src_ip VARCHAR NOT NULL, dst_ip VARCHAR NOT NULL, window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL, in_bytes BIGINT NOT NULL, out_bytes BIGINT NOT NULL, in_packets BIGINT NOT NULL, out_packets BIGINT NOT NULL, batch_id UUID NOT NULL, ingested_at TIMESTAMP NOT NULL, PRIMARY KEY (id, window_start, batch_id))");
            handle.execute("CREATE TABLE IF NOT EXISTS protocol_usage_aggregates (l7_protocol DOUBLE PRECISION NOT NULL, window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL, in_bytes BIGINT NOT NULL, out_bytes BIGINT NOT NULL, in_packets BIGINT NOT NULL, out_packets BIGINT NOT NULL, connection_count BIGINT NOT NULL, batch_id UUID NOT NULL, ingested_at TIMESTAMP NOT NULL, PRIMARY KEY (l7_protocol, window_start, batch_id))");
            handle.execute("CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates (ip VARCHAR NOT NULL, window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL, inbound_bytes BIGINT NOT NULL, outbound_bytes BIGINT NOT NULL, inbound_packets BIGINT NOT NULL, outbound_packets BIGINT NOT NULL, batch_id UUID NOT NULL, ingested_at TIMESTAMP NOT NULL, PRIMARY KEY (ip, window_start, batch_id))");
        });
    }
}
