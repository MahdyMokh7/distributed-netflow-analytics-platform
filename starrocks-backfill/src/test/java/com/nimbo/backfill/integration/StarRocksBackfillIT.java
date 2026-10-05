package com.nimbo.backfill.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.BackfillMode;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.repository.StarRocksBackfillRepository;
import com.nimbo.backfill.service.BackfillPlanner;
import com.nimbo.backfill.service.BackfillService;
import com.nimbo.backfill.support.BackfillParquetTestData;
import com.nimbo.backfill.support.BackfillParquetTestData.SourceRow;
import com.nimbo.backfill.support.StarRocksContainerExtension;

class StarRocksBackfillIT {

    private static final Instant FROM = Instant.parse("2026-09-26T10:15:00.000Z");
    private static final Instant TO = Instant.parse("2026-09-26T10:45:00.000Z");

    @RegisterExtension
    static final StarRocksContainerExtension STARROCKS = new StarRocksContainerExtension();

    @TempDir
    Path temporaryDirectory;

    @Test
    void givenParquetRecords_whenPlanning_thenValidatesWithoutChangingStarRocks() throws Exception {
        String sourceName = "plan-source";
        installSource(sourceName);

        execute(BackfillMode.PLAN, sourceName);

        assertThat(tableRowCount()).isZero();
        assertThat(temporaryPartitionCount()).isZero();
    }

    @Test
    void givenParquetRecords_whenExecutingTwice_thenPublishesExactIntervalOnce() throws Exception {
        String sourceName = "execute-source";
        installSource(sourceName);

        execute(BackfillMode.EXECUTE, sourceName);
        execute(BackfillMode.EXECUTE, sourceName);

        assertThat(readPersistedRows())
                .extracting(PersistedRow::sourceIp, PersistedRow::timestamp, PersistedRow::topic,
                        PersistedRow::partition, PersistedRow::offset, PersistedRow::inBytes,
                        PersistedRow::outBytes, PersistedRow::inPackets, PersistedRow::outPackets)
                .containsExactly(
                        tuple("192.0.2.10", FROM.toEpochMilli(), "netflow-source", 3, 10L,
                                100L, 10L, 2L, 1L),
                        tuple("192.0.2.11", TO.minusMillis(1).toEpochMilli(), "netflow-source", 3, 11L,
                                200L, 20L, 4L, 2L));
        assertThat(temporaryPartitionCount()).isZero();
    }

    private void installSource(String sourceName) throws Exception {
        Path parquetFile = temporaryDirectory.resolve(sourceName + ".parquet");
        try (Connection connection = STARROCKS.connection()) {
            BackfillParquetTestData.write(connection, parquetFile, List.of(
                    new SourceRow("192.0.2.9", FROM.minusMillis(1).toEpochMilli(), 9L,
                            900L, 90L, 9L, 9L),
                    new SourceRow("192.0.2.10", FROM.toEpochMilli(), 10L,
                            100L, 10L, 2L, 1L),
                    new SourceRow("192.0.2.11", TO.minusMillis(1).toEpochMilli(), 11L,
                            200L, 20L, 4L, 2L),
                    new SourceRow("192.0.2.12", TO.toEpochMilli(), 12L,
                            1_200L, 120L, 12L, 12L)));
        }
        STARROCKS.replaceHourlyParquet(sourceName, FROM.atZone(ZoneOffset.UTC).toLocalDate(),
                FROM.atZone(ZoneOffset.UTC).getHour(), parquetFile);
    }

    private void execute(BackfillMode mode, String sourceName) throws Exception {
        BackfillConfig config = new BackfillConfig(STARROCKS.jdbcUrl(), "root", "test-password", "nimbo",
                "netflow", STARROCKS.sourceBasePath(sourceName), "hadoop", mode, FROM, TO);
        DailyBackfillPlan plan = new BackfillPlanner().createPlans(config).get(0);

        try (Connection connection = STARROCKS.connection()) {
            StarRocksBackfillRepository repository = new StarRocksBackfillRepository(connection, config);
            repository.initializeSession();
            new BackfillService(repository).run(plan);
        }
    }

    private long tableRowCount() throws SQLException {
        try (Connection connection = STARROCKS.connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM nimbo.netflow")) {
            result.next();
            return result.getLong(1);
        }
    }

    private int temporaryPartitionCount() throws SQLException {
        int count = 0;
        try (Connection connection = STARROCKS.connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SHOW TEMPORARY PARTITIONS FROM nimbo.netflow")) {
            while (result.next()) {
                count++;
            }
        }
        return count;
    }

    private List<PersistedRow> readPersistedRows() throws SQLException {
        List<PersistedRow> rows = new ArrayList<>();
        try (Connection connection = STARROCKS.connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("""
                     SELECT ipv4_src_addr, PROCESS_TIME, kafka_topic, kafka_partition, kafka_offset,
                            in_bytes, out_bytes, in_pkts, out_pkts
                     FROM nimbo.netflow
                     ORDER BY PROCESS_TIME
                     """)) {
            while (result.next()) {
                rows.add(new PersistedRow(result.getString("ipv4_src_addr"),
                        result.getLong("PROCESS_TIME"), result.getString("kafka_topic"),
                        result.getInt("kafka_partition"), result.getLong("kafka_offset"),
                        result.getLong("in_bytes"), result.getLong("out_bytes"),
                        result.getLong("in_pkts"), result.getLong("out_pkts")));
            }
        }
        return rows;
    }

    private record PersistedRow(String sourceIp, long timestamp, String topic, int partition, long offset,
            long inBytes, long outBytes, long inPackets, long outPackets) {
    }
}
