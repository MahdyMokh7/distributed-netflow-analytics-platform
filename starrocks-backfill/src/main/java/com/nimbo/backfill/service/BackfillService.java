package com.nimbo.backfill.service;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillMode;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.model.HdfsPathColumnMapping;
import com.nimbo.backfill.repository.BackfillRepository;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackfillService {

  private static final Logger LOGGER = LoggerFactory.getLogger(BackfillService.class);

  private final BackfillRepository repository;

  public BackfillService(BackfillRepository repository) {
    this.repository = repository;
  }

  public void run(DailyBackfillPlan plan) throws SQLException {
    BackfillProjection projection = repository.buildBackfillProjection(plan);
    BackfillMetrics sourceIntervalMetrics = repository.readSourceIntervalMetrics(plan, projection);

    if (sourceIntervalMetrics.rowCount() == 0) {
      throw new BackfillException("No HDFS records were found in the requested interval");
    }
    repository.validateSourceProjections(plan, projection);

    BackfillMetrics targetIntervalMetrics = repository.readTargetIntervalMetrics(plan);
    printPlan(plan, projection, sourceIntervalMetrics, targetIntervalMetrics);
    if (plan.mode() == BackfillMode.EXECUTE && repository.stagingPartitionExists(plan)) {
      LOGGER.info("Dropping stale temporary partition: {}", plan.stagingPartitionName());
      repository.dropStagingPartition(plan);
    }
    if (sourceIntervalMetrics.sameDataAs(targetIntervalMetrics)) {
      LOGGER.info("SKIP: HDFS and StarRocks interval metrics already match; no data was changed");
      return;
    }
    if (plan.mode() == BackfillMode.PLAN) {
      LOGGER.info("PLAN completed: no data was changed");
      return;
    }

    BackfillMetrics publishedTargetMetrics =
        stageAndReplaceInterval(plan, projection, sourceIntervalMetrics);
    LOGGER.info("EXECUTE completed successfully: {}", publishedTargetMetrics.summary());
  }

  private BackfillMetrics stageAndReplaceInterval(
      DailyBackfillPlan plan, BackfillProjection projection, BackfillMetrics sourceIntervalMetrics)
      throws SQLException {
    boolean stagingPartitionCreated = false;
    boolean targetDeleteStarted = false;
    try {
      repository.createStagingPartition(plan);
      stagingPartitionCreated = true;
      repository.loadSourceIntoStaging(plan, projection);

      BackfillMetrics stagingIntervalMetrics = repository.readStagingIntervalMetrics(plan);
      verifyMetrics(
          "HDFS and temporary partition interval", sourceIntervalMetrics, stagingIntervalMetrics);

      LOGGER.info("Staged interval validated; deleting and inserting only the requested interval");
      targetDeleteStarted = true;
      repository.deleteTargetInterval(plan);
      repository.publishStagedInterval(plan, projection);

      BackfillMetrics publishedTargetMetrics = repository.readTargetIntervalMetrics(plan);
      verifyMetrics(
          "HDFS and published StarRocks interval", sourceIntervalMetrics, publishedTargetMetrics);
      repository.dropStagingPartition(plan);
      stagingPartitionCreated = false;
      return publishedTargetMetrics;
    } catch (BackfillException | SQLException exception) {
      if (targetDeleteStarted) {
        LOGGER.warn("The interval update is non-atomic; rerun EXECUTE if this run did not finish");
      }
      if (stagingPartitionCreated) {
        dropStagingAfterFailure(plan, exception);
      }
      throw exception;
    }
  }

  private void dropStagingAfterFailure(DailyBackfillPlan plan, Exception original) {
    try {
      repository.dropStagingPartition(plan);
      LOGGER.info("Dropped incomplete temporary partition: {}", plan.stagingPartitionName());
    } catch (SQLException cleanupFailure) {
      original.addSuppressed(cleanupFailure);
      LOGGER.error(
          "Could not drop incomplete temporary partition {}",
          plan.stagingPartitionName(),
          cleanupFailure);
    }
  }

  private void printPlan(
      DailyBackfillPlan plan,
      BackfillProjection projection,
      BackfillMetrics sourceIntervalMetrics,
      BackfillMetrics targetIntervalMetrics) {
    LOGGER.info("Backfill run: {}", plan.runId());
    LOGGER.info("Mode: {}", plan.mode());
    LOGGER.info("Interval: [{}, {})", plan.from(), plan.to());
    LOGGER.info("Temporary partition: {}", plan.stagingPartitionName());
    LOGGER.info(
        "HDFS paths: {}",
        projection.hdfsColumnMappings().stream()
            .map(HdfsPathColumnMapping::path)
            .collect(java.util.stream.Collectors.joining(", ")));
    if (!projection.skippedPaths().isEmpty()) {
      LOGGER.info("Skipped empty HDFS paths: {}", String.join(", ", projection.skippedPaths()));
    }
    for (HdfsPathColumnMapping hdfsColumnMapping : projection.hdfsColumnMappings()) {
      LOGGER.info(
          "Schema mapping for {}: {}",
          hdfsColumnMapping.path(),
          hdfsColumnMapping.mappingSummary());
    }
    LOGGER.info("HDFS interval: {}", sourceIntervalMetrics.summary());
    LOGGER.info("Current StarRocks interval: {}", targetIntervalMetrics.summary());
  }

  private void verifyMetrics(String description, BackfillMetrics expected, BackfillMetrics actual) {
    if (!expected.sameDataAs(actual)) {
      throw new BackfillException(
          description
              + " metrics differ; expected={"
              + expected.summary()
              + "}, actual={"
              + actual.summary()
              + "}");
    }
  }
}
