package com.nimbo.batch.writer;

import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.TrafficAggregateDao;
import com.nimbo.batch.support.PostgresContainerExtension;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionWriterIT {

    @RegisterExtension
    static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

    private static final UUID BATCH_ID = UUID.fromString("50000000-0000-0000-0000-000000000001");
    private static final Timestamp WINDOW_START = Timestamp.from(Instant.parse("2026-08-29T08:00:00Z"));
    private static final Timestamp WINDOW_END = Timestamp.from(Instant.parse("2026-08-29T09:00:00Z"));
    private static final StructType TRAFFIC_SCHEMA = new StructType()
            .add("src_ip", DataTypes.StringType)
            .add("window_start", DataTypes.TimestampType)
            .add("window_end", DataTypes.TimestampType)
            .add("in_bytes", DataTypes.LongType)
            .add("out_bytes", DataTypes.LongType)
            .add("in_packets", DataTypes.LongType)
            .add("out_packets", DataTypes.LongType)
            .add("record_count", DataTypes.LongType);

    private Jdbi jdbi;

    @BeforeEach
    void resetDatabase() {
        jdbi = POSTGRES.jdbi();
        jdbi.useHandle(handle -> handle.execute("TRUNCATE traffic_aggregates"));
    }

    @Test
    void givenValidRows_whenPartitionIsWritten_thenRowsPersistAndExecutorConnectionsClose() {
        // Given: two valid aggregates and a real executor-side database pool
        long connectionsBefore = executorConnections();
        LongAccumulator written = new LongAccumulator();
        PartitionWriter<com.nimbo.batch.model.TrafficAggregate, TrafficAggregateDao> partitionWriter =
                partitionWriter(new JdbiDaoFactory(postgresConfig(), executorPoolConfig()), written);

        // When: the partition writes both rows through the real DAO
        partitionWriter.call(List.of(trafficRow("10.0.0.1", 100L), trafficRow("10.0.0.2", 200L)).iterator());

        // Then: data is committed and no executor pool connections remain open
        assertThat(written.value()).isEqualTo(2L);
        assertThat(trafficRowCount()).isEqualTo(2L);
        assertThat(executorConnections()).isEqualTo(connectionsBefore);
    }

    @Test
    void givenDatabaseRejectsRow_whenRetriesFail_thenExecutorConnectionsClose() {
        // Given: an aggregate that violates the nonnegative byte constraint
        long connectionsBefore = executorConnections();
        LongAccumulator retries = new LongAccumulator();
        PartitionWriter<com.nimbo.batch.model.TrafficAggregate, TrafficAggregateDao> partitionWriter =
                partitionWriter(new JdbiDaoFactory(postgresConfig(), executorPoolConfig()), new LongAccumulator(), retries);

        // When: the real DAO retries and the database rejects both attempts
        assertThatThrownBy(() -> partitionWriter.call(List.of(trafficRow("10.0.0.3", -1L)).iterator()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Batch execution failed after 2 attempts");

        // Then: the failed row is absent and the executor pool has released its connections
        assertThat(retries.value()).isEqualTo(1L);
        assertThat(trafficRowCount()).isZero();
        assertThat(executorConnections()).isEqualTo(connectionsBefore);
    }

    private PartitionWriter<com.nimbo.batch.model.TrafficAggregate, TrafficAggregateDao> partitionWriter(
            JdbiDaoFactory daoFactory, LongAccumulator written) {
        return partitionWriter(daoFactory, written, new LongAccumulator());
    }

    private PartitionWriter<com.nimbo.batch.model.TrafficAggregate, TrafficAggregateDao> partitionWriter(
            JdbiDaoFactory daoFactory, LongAccumulator written, LongAccumulator retries) {
        return new PartitionWriter<>(
                "traffic_aggregates", 2, 2, 0, 0,
                daoFactory,
                JdbiDaoFactory::createTrafficDao,
                TrafficAggregateDao::upsertBatch,
                new TableRowMapper()::toTrafficAggregate,
                BATCH_ID,
                Timestamp.from(Instant.parse("2026-08-29T10:00:00Z")),
                new BatchExecutor(),
                written,
                retries);
    }

    private Row trafficRow(String sourceIp, long inBytes) {
        return new GenericRowWithSchema(
                new Object[]{sourceIp, WINDOW_START, WINDOW_END, inBytes, 50L, 1L, 1L, 1L},
                TRAFFIC_SCHEMA);
    }

    private long executorConnections() {
        return jdbi.withHandle(handle -> handle.createQuery("SELECT COUNT(*) FROM pg_stat_activity WHERE usename = :username AND application_name = 'PostgreSQL JDBC Driver'")
                .bind("username", POSTGRES.username())
                .mapTo(Long.class)
                .one());
    }

    private long trafficRowCount() {
        return jdbi.withHandle(handle -> handle.createQuery("SELECT COUNT(*) FROM traffic_aggregates")
                .mapTo(Long.class)
                .one());
    }

    private PostgresConfig postgresConfig() {
        PostgresConfig config = new PostgresConfig();
        config.setHost(POSTGRES.host());
        config.setPort(POSTGRES.port());
        config.setDatabase(POSTGRES.databaseName());
        config.setUsername(POSTGRES.username());
        config.setPassword(POSTGRES.password());
        return config;
    }

    private ExecutorPoolConfig executorPoolConfig() {
        ExecutorPoolConfig config = new ExecutorPoolConfig();
        config.setMaxPoolSize(2);
        config.setConnectionTimeoutMs(5_000);
        config.setIdleTimeoutMs(60_000);
        config.setMaxLifetimeMs(120_000);
        config.setPoolName("partition-writer-test-pool");
        return config;
    }
}
