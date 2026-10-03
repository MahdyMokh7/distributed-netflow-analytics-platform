package com.nimbo.backfill.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillMode;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.BackfillProjection.HdfsPathProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.repository.BackfillRepository;

class BackfillServiceTest {

    @Test
    void planModeValidatesButDoesNotMutateStarRocks() throws Exception {
        FakeRepository repository = new FakeRepository();
        repository.temporaryPartitionExists = true;

        new BackfillService(repository).run(plan(BackfillMode.PLAN));

        assertThat(repository.mutations()).isZero();
        assertThat(repository.validateCalls).isEqualTo(1);
    }

    @Test
    void matchingIntervalMetricsSkipTheBackfill() throws Exception {
        FakeRepository repository = new FakeRepository();
        repository.starRocksIntervalMetrics = repository.hdfsIntervalMetrics;

        new BackfillService(repository).run(plan(BackfillMode.EXECUTE));

        assertThat(repository.mutations()).isZero();
    }

    @Test
    void matchingExecuteDropsAStaleTemporaryPartition() throws Exception {
        FakeRepository repository = new FakeRepository();
        repository.starRocksIntervalMetrics = repository.hdfsIntervalMetrics;
        repository.temporaryPartitionExists = true;

        new BackfillService(repository).run(plan(BackfillMode.EXECUTE));

        assertThat(repository.dropCalls).isEqualTo(1);
        assertThat(repository.createCalls).isZero();
    }

    @Test
    void stagesAndReloadsOnlyTheRequestedInterval() throws Exception {
        FakeRepository repository = new FakeRepository();

        new BackfillService(repository).run(plan(BackfillMode.EXECUTE));

        assertThat(repository.createCalls).isEqualTo(1);
        assertThat(repository.loadCalls).isEqualTo(1);
        assertThat(repository.deleteCalls).isEqualTo(1);
        assertThat(repository.copyCalls).isEqualTo(1);
        assertThat(repository.dropCalls).isEqualTo(1);
        assertThat(repository.events).containsSubsequence("load-stage", "measure-stage", "delete", "insert-stage");
    }

    @Test
    void temporaryMetricMismatchDoesNotPublishAndCleansUp() {
        FakeRepository repository = new FakeRepository();
        repository.stagedIntervalMetrics = metrics(4, 10);

        assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
                .isInstanceOf(BackfillException.class)
                .hasMessageContaining("metrics differ");

        assertThat(repository.deleteCalls).isZero();
        assertThat(repository.copyCalls).isZero();
        assertThat(repository.dropCalls).isEqualTo(1);
    }

    @Test
    void insertFailureAfterDeleteAlsoCleansUpTheTemporaryPartition() {
        FakeRepository repository = new FakeRepository();
        repository.copyFailure = new SQLException("insert failed");

        assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
                .isInstanceOf(SQLException.class).hasMessageContaining("insert failed");

        assertThat(repository.deleteCalls).isEqualTo(1);
        assertThat(repository.dropCalls).isEqualTo(1);
    }

    @Test
    void failsWhenPublishedIntervalDoesNotMatchHdfs() {
        FakeRepository repository = new FakeRepository();
        repository.publishedStarRocksMetrics = metrics(6, 10);

        assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
                .isInstanceOf(BackfillException.class)
                .hasMessageContaining("metrics differ");

        assertThat(repository.dropCalls).isEqualTo(1);
    }

    @Test
    void refusesToMutateWhenHdfsHasNoRows() {
        FakeRepository repository = new FakeRepository();
        repository.hdfsIntervalMetrics = BackfillMetrics.empty();

        assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
                .isInstanceOf(BackfillException.class)
                .hasMessageContaining("No HDFS records");

        assertThat(repository.mutations()).isZero();
        assertThat(repository.validateCalls).isZero();
    }

    private DailyBackfillPlan plan(BackfillMode mode) {
        Instant from = Instant.parse("2026-09-26T10:00:00Z");
        Instant to = Instant.parse("2026-09-26T12:00:00Z");
        return new DailyBackfillPlan("bf-test", mode, LocalDate.parse("2026-09-26"), from, to, "tmp_bf_test",
                List.of("hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet"));
    }

    private static BackfillMetrics metrics(long rows, long base) {
        return new BackfillMetrics(rows, BigDecimal.valueOf(base), BigDecimal.valueOf(base * 2),
                BigDecimal.valueOf(base * 3), BigDecimal.valueOf(base * 4), 1_000L, 2_000L);
    }

    private static final class FakeRepository implements BackfillRepository {

        private BackfillMetrics hdfsIntervalMetrics = metrics(5, 10);
        private BackfillMetrics starRocksIntervalMetrics = BackfillMetrics.empty();
        private BackfillMetrics publishedStarRocksMetrics = hdfsIntervalMetrics;
        private BackfillMetrics stagedIntervalMetrics = hdfsIntervalMetrics;
        private boolean mutated;
        private boolean temporaryPartitionExists;
        private SQLException copyFailure;
        private final BackfillProjection projection = new BackfillProjection(List.of("kafka_timestamp"),
                List.of(new HdfsPathProjection("hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet",
                        List.of("CAST(PROCESS_TIME / 1000 AS BIGINT)"), "mapping")),
                List.of());

        private int validateCalls;
        private int createCalls;
        private int copyCalls;
        private int loadCalls;
        private int deleteCalls;
        private int dropCalls;
        private final List<String> events = new ArrayList<>();

        int mutations() {
            return createCalls + copyCalls + loadCalls + deleteCalls + dropCalls;
        }

        @Override
        public boolean temporaryPartitionExists(DailyBackfillPlan plan) {
            return temporaryPartitionExists;
        }

        @Override
        public BackfillProjection prepareProjection(DailyBackfillPlan plan) {
            return projection;
        }

        @Override
        public BackfillMetrics readHdfsIntervalMetrics(DailyBackfillPlan plan, BackfillProjection projection) {
            return hdfsIntervalMetrics;
        }

        @Override
        public void validateHdfsProjection(DailyBackfillPlan plan, BackfillProjection projection) {
            validateCalls++;
        }

        @Override
        public BackfillMetrics readFormalIntervalMetrics(DailyBackfillPlan plan) {
            if (mutated) {
                return publishedStarRocksMetrics;
            }
            return starRocksIntervalMetrics;
        }

        @Override
        public BackfillMetrics readTemporaryIntervalMetrics(DailyBackfillPlan plan) {
            events.add("measure-stage");
            return stagedIntervalMetrics;
        }

        @Override
        public void dropTemporaryPartition(DailyBackfillPlan plan) {
            dropCalls++;
        }

        @Override
        public void createTemporaryPartition(DailyBackfillPlan plan) {
            createCalls++;
        }

        @Override
        public void loadHdfs(DailyBackfillPlan plan, BackfillProjection projection) {
            loadCalls++;
            events.add("load-stage");
        }

        @Override
        public void deleteFormalInterval(DailyBackfillPlan plan) {
            deleteCalls++;
            events.add("delete");
        }

        @Override
        public void copyTemporaryIntervalIntoFormal(DailyBackfillPlan plan, BackfillProjection projection)
                throws SQLException {
            copyCalls++;
            events.add("insert-stage");
            if (copyFailure != null) {
                throw copyFailure;
            }
            mutated = true;
        }

    }
}
