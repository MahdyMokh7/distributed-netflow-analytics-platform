package com.nimbo.backfill.repository;

import java.sql.SQLException;

import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;

public interface BackfillRepository {

    boolean stagingPartitionExists(DailyBackfillPlan plan) throws SQLException;

    BackfillProjection buildBackfillProjection(DailyBackfillPlan plan) throws SQLException;

    BackfillMetrics readSourceIntervalMetrics(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException;

    void validateSourceProjections(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

    BackfillMetrics readTargetIntervalMetrics(DailyBackfillPlan plan) throws SQLException;

    BackfillMetrics readStagingIntervalMetrics(DailyBackfillPlan plan) throws SQLException;

    void dropStagingPartition(DailyBackfillPlan plan) throws SQLException;

    void createStagingPartition(DailyBackfillPlan plan) throws SQLException;

    void loadSourceIntoStaging(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

    void deleteTargetInterval(DailyBackfillPlan plan) throws SQLException;

    void publishStagedInterval(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

}
