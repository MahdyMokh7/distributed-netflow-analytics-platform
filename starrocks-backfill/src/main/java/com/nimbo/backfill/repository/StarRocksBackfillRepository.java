package com.nimbo.backfill.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.model.HdfsPathColumnMapping;
import com.nimbo.backfill.model.HdfsSchema;
import com.nimbo.backfill.model.SchemaColumn;
import com.nimbo.backfill.service.BackfillProjectionBuilder;
import com.nimbo.backfill.sql.NetflowBackfillSql;

public final class StarRocksBackfillRepository implements BackfillRepository {

    private final StarRocksJdbcClient jdbcClient;
    private final BackfillConfig config;

    public StarRocksBackfillRepository(Connection connection, BackfillConfig config) {
        this.jdbcClient = new StarRocksJdbcClient(connection);
        this.config = config;
    }

    public void initializeSession() throws SQLException {
        jdbcClient.execute("SET time_zone = 'UTC'");
        jdbcClient.execute("SET query_timeout = 3600");
        jdbcClient.execute("SET insert_timeout = 3600");
    }

    @Override
    public boolean stagingPartitionExists(DailyBackfillPlan plan) throws SQLException {
        return jdbcClient.hasRow(NetflowBackfillSql.stagingPartitionExists(config, plan));
    }

    @Override
    public BackfillProjection buildBackfillProjection(DailyBackfillPlan plan) throws SQLException {
        List<SchemaColumn> starRocksColumns = jdbcClient.readSchema(NetflowBackfillSql.describeTable(config));
        List<HdfsSchema> hdfsSchemas = new ArrayList<>();
        List<String> skippedPaths = new ArrayList<>();

        for (String path : plan.hdfsPaths()) {
            try {
                hdfsSchemas.add(
                        new HdfsSchema(path, jdbcClient.readSchema(NetflowBackfillSql.describeHdfs(config, path))));
            } catch (SQLException exception) {
                if (noFilesFound(exception)) {
                    skippedPaths.add(path);
                    continue;
                }
                throw new BackfillException("Cannot inspect HDFS path " + path + ": " + exception.getMessage(),
                        exception);
            }
        }

        return new BackfillProjectionBuilder().build(NetflowBackfillSql.starRocksTable(config), starRocksColumns,
                hdfsSchemas, skippedPaths);
    }

    @Override
    public BackfillMetrics readSourceIntervalMetrics(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException {
        BackfillMetrics total = BackfillMetrics.empty();
        for (HdfsPathColumnMapping hdfsColumnMapping : projection.hdfsColumnMappings()) {
            try {
                total = total.combine(jdbcClient.queryMetrics(
                        NetflowBackfillSql.sourceIntervalMetrics(config, plan, hdfsColumnMapping.path())));
            } catch (SQLException exception) {
                throw new BackfillException("Cannot read HDFS path " + hdfsColumnMapping.path() + ": "
                        + exception.getMessage(), exception);
            }
        }
        return total;
    }

    @Override
    public void validateSourceProjections(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException {
        for (HdfsPathColumnMapping hdfsColumnMapping : projection.hdfsColumnMappings()) {
            try {
                jdbcClient.execute(NetflowBackfillSql.validateSourceProjection(config, plan, hdfsColumnMapping));
            } catch (SQLException exception) {
                throw new BackfillException(
                        "Dynamic projection for " + hdfsColumnMapping.path() + " is invalid: "
                                + exception.getMessage(), exception);
            }
        }
    }

    @Override
    public BackfillMetrics readTargetIntervalMetrics(DailyBackfillPlan plan) throws SQLException {
        return jdbcClient.queryMetrics(NetflowBackfillSql.targetIntervalMetrics(config, plan));
    }

    @Override
    public BackfillMetrics readStagingIntervalMetrics(DailyBackfillPlan plan) throws SQLException {
        return jdbcClient.queryMetrics(NetflowBackfillSql.stagingIntervalMetrics(config, plan));
    }

    @Override
    public void dropStagingPartition(DailyBackfillPlan plan) throws SQLException {
        jdbcClient.execute(NetflowBackfillSql.dropStagingPartition(config, plan));
    }

    @Override
    public void createStagingPartition(DailyBackfillPlan plan) throws SQLException {
        jdbcClient.execute(NetflowBackfillSql.createStagingPartition(config, plan));
    }

    @Override
    public void loadSourceIntoStaging(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException {
        jdbcClient.execute(NetflowBackfillSql.loadSourceIntoStaging(config, plan, projection));
    }

    @Override
    public void deleteTargetInterval(DailyBackfillPlan plan) throws SQLException {
        jdbcClient.execute(NetflowBackfillSql.deleteTargetInterval(config, plan));
    }

    @Override
    public void publishStagedInterval(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException {
        jdbcClient.execute(NetflowBackfillSql.publishStagedInterval(config, plan, projection));
    }

    private boolean noFilesFound(SQLException exception) {
        SQLException current = exception;
        while (current != null) {
            String message = current.getMessage();
            if (current.getErrorCode() == 5600 && message != null
                    && message.toLowerCase(Locale.ROOT).contains("no files were found")) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

}
