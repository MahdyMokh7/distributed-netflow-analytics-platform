package com.nimbo.backfill.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillMode;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.model.HdfsPathColumnMapping;
import com.nimbo.backfill.repository.BackfillRepository;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackfillServiceTest {

  @Test
  void planModeValidatesButDoesNotMutateStarRocks() throws Exception {
    FakeRepository repository = new FakeRepository();
    repository.stagingPartitionExists = true;

    new BackfillService(repository).run(plan(BackfillMode.PLAN));

    assertThat(repository.mutations()).isZero();
    assertThat(repository.validateCalls).isEqualTo(1);
  }

  @Test
  void matchingIntervalMetricsSkipTheBackfill() throws Exception {
    FakeRepository repository = new FakeRepository();
    repository.currentTargetIntervalMetrics = repository.sourceIntervalMetrics;

    new BackfillService(repository).run(plan(BackfillMode.EXECUTE));

    assertThat(repository.mutations()).isZero();
  }

  @Test
  void matchingExecuteDropsAStaleTemporaryPartition() throws Exception {
    FakeRepository repository = new FakeRepository();
    repository.currentTargetIntervalMetrics = repository.sourceIntervalMetrics;
    repository.stagingPartitionExists = true;

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
    assertThat(repository.publishCalls).isEqualTo(1);
    assertThat(repository.dropCalls).isEqualTo(1);
    assertThat(repository.events)
        .containsSubsequence("load-stage", "read-stage", "delete", "insert-stage");
  }

  @Test
  void stagingMetricMismatchDoesNotPublishAndCleansUp() {
    FakeRepository repository = new FakeRepository();
    repository.stagingIntervalMetrics = metrics(4, 10);

    assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("metrics differ");

    assertThat(repository.deleteCalls).isZero();
    assertThat(repository.publishCalls).isZero();
    assertThat(repository.dropCalls).isEqualTo(1);
  }

  @Test
  void insertFailureAfterDeleteAlsoCleansUpTheTemporaryPartition() {
    FakeRepository repository = new FakeRepository();
    repository.publishFailure = new SQLException("insert failed");

    assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("insert failed");

    assertThat(repository.deleteCalls).isEqualTo(1);
    assertThat(repository.dropCalls).isEqualTo(1);
  }

  @Test
  void failsWhenPublishedIntervalDoesNotMatchHdfs() {
    FakeRepository repository = new FakeRepository();
    repository.publishedTargetMetrics = metrics(6, 10);

    assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("metrics differ");

    assertThat(repository.dropCalls).isEqualTo(1);
  }

  @Test
  void refusesToMutateWhenHdfsHasNoRows() {
    FakeRepository repository = new FakeRepository();
    repository.sourceIntervalMetrics = BackfillMetrics.empty();

    assertThatThrownBy(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)))
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("No HDFS records");

    assertThat(repository.mutations()).isZero();
    assertThat(repository.validateCalls).isZero();
  }

  @Test
  void preservesCleanupFailureAsSuppressedOnOriginalFailure() {
    FakeRepository repository = new FakeRepository();
    repository.stagingIntervalMetrics = metrics(4, 10);
    repository.dropFailure = new SQLException("cleanup failed");

    Throwable failure =
        catchThrowable(() -> new BackfillService(repository).run(plan(BackfillMode.EXECUTE)));

    assertThat(failure)
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("metrics differ");
    assertThat(failure.getSuppressed()).hasSize(1);
    assertThat(failure.getSuppressed()[0])
        .isInstanceOf(SQLException.class)
        .hasMessage("cleanup failed");
  }

  @Test
  void printsSkippedPathsAsPartOfPlanWithoutMutating() throws Exception {
    FakeRepository repository = new FakeRepository();
    repository.projection =
        new BackfillProjection(
            List.of("PROCESS_TIME"),
            List.of(
                new HdfsPathColumnMapping(
                    "hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet",
                    List.of("CAST(PROCESS_TIME AS BIGINT)"),
                    "mapping")),
            List.of("hdfs://team1/netflow/parquetFiles/2026-09-26/11/*.parquet"));

    new BackfillService(repository).run(plan(BackfillMode.PLAN));

    assertThat(repository.validateCalls).isEqualTo(1);
    assertThat(repository.mutations()).isZero();
  }

  private DailyBackfillPlan plan(BackfillMode mode) {
    Instant from = Instant.parse("2026-09-26T10:00:00Z");
    Instant to = Instant.parse("2026-09-26T12:00:00Z");
    return new DailyBackfillPlan(
        "bf-test",
        mode,
        LocalDate.parse("2026-09-26"),
        from,
        to,
        "tmp_bf_test",
        List.of("hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet"));
  }

  private static BackfillMetrics metrics(long rows, long base) {
    return new BackfillMetrics(
        rows,
        BigDecimal.valueOf(base),
        BigDecimal.valueOf(base * 2),
        BigDecimal.valueOf(base * 3),
        BigDecimal.valueOf(base * 4),
        1_000L,
        2_000L);
  }

  private static final class FakeRepository implements BackfillRepository {

    private BackfillMetrics sourceIntervalMetrics = metrics(5, 10);
    private BackfillMetrics currentTargetIntervalMetrics = BackfillMetrics.empty();
    private BackfillMetrics publishedTargetMetrics = sourceIntervalMetrics;
    private BackfillMetrics stagingIntervalMetrics = sourceIntervalMetrics;
    private boolean stagingPartitionExists;
    private SQLException publishFailure;
    private SQLException dropFailure;
    private BackfillProjection projection =
        new BackfillProjection(
            List.of("PROCESS_TIME"),
            List.of(
                new HdfsPathColumnMapping(
                    "hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet",
                    List.of("CAST(PROCESS_TIME AS BIGINT)"),
                    "mapping")),
            List.of());

    private int validateCalls;
    private int createCalls;
    private int publishCalls;
    private int loadCalls;
    private int deleteCalls;
    private int dropCalls;
    private final List<String> events = new ArrayList<>();

    int mutations() {
      return createCalls + publishCalls + loadCalls + deleteCalls + dropCalls;
    }

    @Override
    public boolean stagingPartitionExists(DailyBackfillPlan plan) {
      return stagingPartitionExists;
    }

    @Override
    public BackfillProjection buildBackfillProjection(DailyBackfillPlan plan) {
      return projection;
    }

    @Override
    public BackfillMetrics readSourceIntervalMetrics(
        DailyBackfillPlan plan, BackfillProjection projection) {
      return sourceIntervalMetrics;
    }

    @Override
    public void validateSourceProjections(DailyBackfillPlan plan, BackfillProjection projection) {
      validateCalls++;
    }

    @Override
    public BackfillMetrics readTargetIntervalMetrics(DailyBackfillPlan plan) {
      return currentTargetIntervalMetrics;
    }

    @Override
    public BackfillMetrics readStagingIntervalMetrics(DailyBackfillPlan plan) {
      events.add("read-stage");
      return stagingIntervalMetrics;
    }

    @Override
    public void dropStagingPartition(DailyBackfillPlan plan) throws SQLException {
      dropCalls++;
      if (dropFailure != null) {
        throw dropFailure;
      }
    }

    @Override
    public void createStagingPartition(DailyBackfillPlan plan) {
      createCalls++;
    }

    @Override
    public void loadSourceIntoStaging(DailyBackfillPlan plan, BackfillProjection projection) {
      loadCalls++;
      events.add("load-stage");
    }

    @Override
    public void deleteTargetInterval(DailyBackfillPlan plan) {
      deleteCalls++;
      events.add("delete");
    }

    @Override
    public void publishStagedInterval(DailyBackfillPlan plan, BackfillProjection projection)
        throws SQLException {
      publishCalls++;
      events.add("insert-stage");
      if (publishFailure != null) {
        throw publishFailure;
      }
      currentTargetIntervalMetrics = publishedTargetMetrics;
    }
  }
}
