package com.nimbo.backfill.repository;

import java.sql.SQLException;

import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;

public interface BackfillRepository {

    boolean temporaryPartitionExists(DailyBackfillPlan plan) throws SQLException;

    BackfillProjection prepareProjection(DailyBackfillPlan plan) throws SQLException;

    BackfillMetrics readHdfsIntervalMetrics(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException;

    void validateHdfsProjection(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

    BackfillMetrics readFormalIntervalMetrics(DailyBackfillPlan plan) throws SQLException;

    BackfillMetrics readTemporaryIntervalMetrics(DailyBackfillPlan plan) throws SQLException;

    void dropTemporaryPartition(DailyBackfillPlan plan) throws SQLException;

    void createTemporaryPartition(DailyBackfillPlan plan) throws SQLException;

    void loadHdfs(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

    void deleteFormalInterval(DailyBackfillPlan plan) throws SQLException;

    void copyTemporaryIntervalIntoFormal(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException;

}
