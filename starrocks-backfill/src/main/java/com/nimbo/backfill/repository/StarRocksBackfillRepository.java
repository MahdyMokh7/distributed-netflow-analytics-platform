package com.nimbo.backfill.repository;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.model.HdfsSchema;
import com.nimbo.backfill.model.SchemaColumn;
import com.nimbo.backfill.service.BackfillSchemaMapper;
import com.nimbo.backfill.sql.NetflowBackfillSql;

public final class StarRocksBackfillRepository implements BackfillRepository {

    private final Connection connection;
    private final BackfillConfig config;

    public StarRocksBackfillRepository(Connection connection, BackfillConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void initializeSession() throws SQLException {
        execute("SET time_zone = 'UTC'");
        execute("SET query_timeout = 3600");
        execute("SET insert_timeout = 3600");
    }

    @Override
    public boolean temporaryPartitionExists(DailyBackfillPlan plan) throws SQLException {
        return hasRow(NetflowBackfillSql.temporaryPartitionExists(config, plan));
    }

    @Override
    public BackfillProjection prepareProjection(DailyBackfillPlan plan) throws SQLException {
        List<SchemaColumn> tableColumns = readSchema(NetflowBackfillSql.describeTable(config));
        List<HdfsSchema> hdfsSchemas = new ArrayList<>();
        List<String> skippedPaths = new ArrayList<>();

        for (String path : plan.hdfsPaths()) {
            try {
                hdfsSchemas.add(new HdfsSchema(path, readSchema(NetflowBackfillSql.describeHdfs(config, path))));
            } catch (SQLException exception) {
                if (noFilesFound(exception)) {
                    skippedPaths.add(path);
                    continue;
                }
                throw new BackfillException("Cannot inspect HDFS path " + path + ": " + exception.getMessage(),
                        exception);
            }
        }

        return new BackfillSchemaMapper().map(NetflowBackfillSql.qualifiedTable(config), tableColumns, hdfsSchemas,
                skippedPaths);
    }

    @Override
    public BackfillMetrics readHdfsIntervalMetrics(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException {
        BackfillMetrics total = BackfillMetrics.empty();
        for (BackfillProjection.HdfsPathProjection pathProjection : projection.pathProjections()) {
            try {
                total = total.combine(
                        queryMetrics(NetflowBackfillSql.sourceMetrics(config, plan, pathProjection.path())));
            } catch (SQLException exception) {
                throw new BackfillException(
                        "Cannot read HDFS path " + pathProjection.path() + ": " + exception.getMessage(), exception);
            }
        }
        return total;
    }

    @Override
    public void validateHdfsProjection(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException {
        for (BackfillProjection.HdfsPathProjection pathProjection : projection.pathProjections()) {
            try {
                execute(NetflowBackfillSql.validateHdfsProjection(config, plan, pathProjection));
            } catch (SQLException exception) {
                throw new BackfillException(
                        "Dynamic projection for " + pathProjection.path() + " is invalid: " + exception.getMessage(),
                        exception);
            }
        }
    }

    @Override
    public BackfillMetrics readFormalIntervalMetrics(DailyBackfillPlan plan) throws SQLException {
        return queryMetrics(NetflowBackfillSql.formalIntervalMetrics(config, plan));
    }

    @Override
    public BackfillMetrics readTemporaryIntervalMetrics(DailyBackfillPlan plan) throws SQLException {
        return queryMetrics(NetflowBackfillSql.temporaryIntervalMetrics(config, plan));
    }

    @Override
    public void dropTemporaryPartition(DailyBackfillPlan plan) throws SQLException {
        execute(NetflowBackfillSql.dropTemporaryPartition(config, plan));
    }

    @Override
    public void createTemporaryPartition(DailyBackfillPlan plan) throws SQLException {
        execute(NetflowBackfillSql.createTemporaryPartition(config, plan));
    }

    @Override
    public void loadHdfs(DailyBackfillPlan plan, BackfillProjection projection) throws SQLException {
        execute(NetflowBackfillSql.loadHdfs(config, plan, projection));
    }

    @Override
    public void deleteFormalInterval(DailyBackfillPlan plan) throws SQLException {
        execute(NetflowBackfillSql.deleteFormalInterval(config, plan));
    }

    @Override
    public void copyTemporaryIntervalIntoFormal(DailyBackfillPlan plan, BackfillProjection projection)
            throws SQLException {
        execute(NetflowBackfillSql.copyTemporaryIntervalIntoFormal(config, plan, projection));
    }

    private List<SchemaColumn> readSchema(String sql) throws SQLException {
        List<SchemaColumn> columns = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                columns.add(new SchemaColumn(result.getString("Field"), result.getString("Type"),
                        "YES".equalsIgnoreCase(result.getString("Null"))));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Schema query returned no columns");
        }
        return columns;
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

    private BackfillMetrics queryMetrics(String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new SQLException("Metrics query returned no row");
            }
            return new BackfillMetrics(result.getLong("row_count"), decimal(result, "in_bytes"),
                    decimal(result, "out_bytes"), decimal(result, "in_packets"), decimal(result, "out_packets"),
                    nullableLong(result, "minimum_timestamp"), nullableLong(result, "maximum_timestamp"));
        }
    }

    private BigDecimal decimal(ResultSet result, String column) throws SQLException {
        BigDecimal value = result.getBigDecimal(column);
        return value == null ? BigDecimal.ZERO : value;
    }

    private Long nullableLong(ResultSet result, String column) throws SQLException {
        long value = result.getLong(column);
        return result.wasNull() ? null : value;
    }

    private boolean hasRow(String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            return result.next();
        }
    }

    private void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
