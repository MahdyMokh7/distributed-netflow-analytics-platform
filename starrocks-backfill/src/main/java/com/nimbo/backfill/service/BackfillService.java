package com.nimbo.backfill.service;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillMode;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.repository.BackfillRepository;

public final class BackfillService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackfillService.class);

    private final BackfillRepository repository;

    public BackfillService(BackfillRepository repository) {
        this.repository = repository;
    }

    public void run(DailyBackfillPlan plan) throws SQLException {
        BackfillProjection projection = repository.prepareProjection(plan);
        BackfillMetrics hdfsIntervalMetrics = repository.readHdfsIntervalMetrics(plan, projection);

        if (hdfsIntervalMetrics.rowCount() == 0) {
            throw new BackfillException("No HDFS records were found in the requested interval");
        }
        repository.validateHdfsProjection(plan, projection);

        BackfillMetrics starRocksIntervalMetrics = repository.readFormalIntervalMetrics(plan);
        printPlan(plan, projection, hdfsIntervalMetrics, starRocksIntervalMetrics);
        if (plan.mode() == BackfillMode.EXECUTE && repository.temporaryPartitionExists(plan)) {
            LOGGER.info("Dropping stale temporary partition: {}", plan.temporaryPartition());
            repository.dropTemporaryPartition(plan);
        }
        if (hdfsIntervalMetrics.sameDataAs(starRocksIntervalMetrics)) {
            LOGGER.info("SKIP: HDFS and StarRocks interval metrics already match; no data was changed");
            return;
        }
        if (plan.mode() == BackfillMode.PLAN) {
            LOGGER.info("PLAN completed: no data was changed");
            return;
        }

        BackfillMetrics publishedStarRocksMetrics = stageAndReplaceInterval(plan, projection, hdfsIntervalMetrics);
        LOGGER.info("EXECUTE completed successfully: {}", publishedStarRocksMetrics.summary());
    }

    private BackfillMetrics stageAndReplaceInterval(DailyBackfillPlan plan, BackfillProjection projection,
        BackfillMetrics hdfsIntervalMetrics) throws SQLException {
        boolean temporaryCreated = false;
        boolean deleteStarted = false;
        try {
            repository.createTemporaryPartition(plan);
            temporaryCreated = true;
            repository.loadHdfs(plan, projection);

            BackfillMetrics stagedIntervalMetrics = repository.readTemporaryIntervalMetrics(plan);
            verifyMetrics("HDFS and temporary partition interval", hdfsIntervalMetrics, stagedIntervalMetrics);

            LOGGER.info("Staged interval validated; deleting and inserting only the requested interval");
            deleteStarted = true;
            repository.deleteFormalInterval(plan);
            repository.copyTemporaryIntervalIntoFormal(plan, projection);

            BackfillMetrics publishedStarRocksMetrics = repository.readFormalIntervalMetrics(plan);
            verifyMetrics("HDFS and published StarRocks interval", hdfsIntervalMetrics,
                    publishedStarRocksMetrics);
            repository.dropTemporaryPartition(plan);
            temporaryCreated = false;
            return publishedStarRocksMetrics;
        } catch (BackfillException | SQLException exception) {
            if (deleteStarted) {
                LOGGER.warn("The interval update is non-atomic; rerun EXECUTE if this run did not finish");
            }
            if (temporaryCreated) {
                dropTemporaryAfterFailure(plan, exception);
            }
            throw exception;
        }
    }

    private void dropTemporaryAfterFailure(DailyBackfillPlan plan, Exception original) {
        try {
            repository.dropTemporaryPartition(plan);
            LOGGER.info("Dropped incomplete temporary partition: {}", plan.temporaryPartition());
        } catch (SQLException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
            LOGGER.error("Could not drop incomplete temporary partition {}", plan.temporaryPartition(),
                    cleanupFailure);
        }
    }

    private void printPlan(DailyBackfillPlan plan, BackfillProjection projection,
            BackfillMetrics hdfsIntervalMetrics, BackfillMetrics starRocksIntervalMetrics) {
        LOGGER.info("Backfill run: {}", plan.runId());
        LOGGER.info("Mode: {}", plan.mode());
        LOGGER.info("Interval: [{}, {})", plan.from(), plan.to());
        LOGGER.info("Temporary partition: {}", plan.temporaryPartition());
        LOGGER.info("HDFS paths: {}", projection.pathProjections().stream()
                .map(BackfillProjection.HdfsPathProjection::path)
                .collect(java.util.stream.Collectors.joining(", ")));
        if (!projection.skippedPaths().isEmpty()) {
            LOGGER.info("Skipped empty HDFS paths: {}", String.join(", ", projection.skippedPaths()));
        }
        for (BackfillProjection.HdfsPathProjection pathProjection : projection.pathProjections()) {
            LOGGER.info("Schema mapping for {}: {}", pathProjection.path(), pathProjection.summary());
        }
        LOGGER.info("HDFS interval: {}", hdfsIntervalMetrics.summary());
        LOGGER.info("Current StarRocks interval: {}", starRocksIntervalMetrics.summary());
    }

    private void verifyMetrics(String description, BackfillMetrics expected, BackfillMetrics actual) {
        if (!expected.sameDataAs(actual)) {
            throw new BackfillException(description + " metrics differ; expected={" + expected.summary() + "}, actual={"
                    + actual.summary() + "}");
        }
    }

}
