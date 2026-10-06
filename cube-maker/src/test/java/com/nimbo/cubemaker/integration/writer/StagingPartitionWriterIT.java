package com.nimbo.cubemaker.writer;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.cubemaker.config.ExecutorPoolConfig;
import com.nimbo.cubemaker.config.PostgresConfig;
import com.nimbo.cubemaker.support.PostgresContainerExtension;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class StagingPartitionWriterIT {

  private static final UUID BATCH_ID = UUID.fromString("60000000-0000-0000-0000-000000000001");
  private static final StructType SCHEMA =
      new StructType()
          .add("src_ip", DataTypes.StringType)
          .add("window_start", DataTypes.TimestampType)
          .add("window_end", DataTypes.TimestampType)
          .add("in_bytes", DataTypes.LongType)
          .add("out_bytes", DataTypes.LongType)
          .add("in_packets", DataTypes.LongType)
          .add("out_packets", DataTypes.LongType)
          .add("record_count", DataTypes.LongType);

  @RegisterExtension
  static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

  private Jdbi jdbi;
  private JdbiDaoFactory daoFactory;

  @BeforeEach
  void resetDatabase() {
    jdbi = POSTGRES.jdbi();
    POSTGRES.ensureDailyPartitions(LocalDate.of(2026, 9, 25));
    jdbi.useHandle(
        handle -> handle.execute("TRUNCATE traffic_aggregates, traffic_aggregates_staging"));
    daoFactory = new JdbiDaoFactory(postgresConfig(), executorPoolConfig());
  }

  @AfterEach
  void closePools() {
    JdbiDaoFactory.closeExecutorPools();
  }

  @Test
  void givenARetriedSparkPartition_whenCopiedAndMerged_thenOnlyTheLatestAttemptIsUpserted() {
    // Given: an initial partition result followed by a deterministic retry for the same partition
    StagingPartitionWriter writer =
        new StagingPartitionWriter(
            StagingTable.TRAFFIC,
            daoFactory,
            BATCH_ID,
            Timestamp.from(Instant.parse("2026-09-25T10:00:00Z")));
    writer.call(List.of(trafficRow("10.0.0.1", 100L), trafficRow("10.0.0.2", 200L)).iterator());

    // When: the partition is replaced and merged with the existing upsert semantics
    writer.call(List.of(trafficRow("10.0.0.1", 300L)).iterator());
    int merged =
        jdbi.inTransaction(
            handle -> {
              int count =
                  handle
                      .createUpdate(StagingTable.TRAFFIC.mergeSql())
                      .bind("batchId", BATCH_ID)
                      .execute();
              handle
                  .createUpdate("DELETE FROM traffic_aggregates_staging WHERE batch_id = :batchId")
                  .bind("batchId", BATCH_ID)
                  .execute();
              return count;
            });

    // Then: binary COPY preserves values and retry replacement prevents duplicate rows
    assertThat(merged).isEqualTo(1);
    assertThat(queryLong("SELECT COUNT(*) FROM traffic_aggregates")).isEqualTo(1L);
    assertThat(queryLong("SELECT in_bytes FROM traffic_aggregates")).isEqualTo(300L);
    assertThat(queryLong("SELECT COUNT(*) FROM traffic_aggregates_staging")).isZero();
  }

  private Row trafficRow(String ip, long inBytes) {
    return new GenericRowWithSchema(
        new Object[] {
            ip,
            Timestamp.from(Instant.parse("2026-09-25T08:00:00Z")),
            Timestamp.from(Instant.parse("2026-09-25T09:00:00Z")),
            inBytes,
            50L,
            4_200_000_000L,
            2L,
            1L
        },
        SCHEMA);
  }

  private long queryLong(String sql) {
    return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Long.class).one());
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
    config.setMaxPoolSize(1);
    config.setMinimumIdle(0);
    config.setConnectionTimeoutMs(5_000);
    config.setIdleTimeoutMs(60_000);
    config.setMaxLifetimeMs(1_800_000);
    config.setPoolName("staging-writer-test-pool");
    return config;
  }
}
