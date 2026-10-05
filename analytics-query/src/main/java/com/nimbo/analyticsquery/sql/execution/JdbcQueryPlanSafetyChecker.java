package com.nimbo.analyticsquery.sql.execution;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Objects;

import javax.sql.DataSource;

import com.nimbo.analyticsquery.config.QueryPlanSafetyProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class JdbcQueryPlanSafetyChecker
        implements QueryPlanSafetyChecker {

    private final AnalysisDataSource analysisDataSource;
    private final DataSource dataSource;
    protected final QueryPlanSafetyProperties properties;

    protected JdbcQueryPlanSafetyChecker(
            AnalysisDataSource analysisDataSource,
            DataSource dataSource,
            QueryPlanSafetyProperties properties) {

        this.analysisDataSource = Objects.requireNonNull(
                analysisDataSource,
                "analysisDataSource must not be null");
        this.dataSource = Objects.requireNonNull(
                dataSource,
                "dataSource must not be null");
        this.properties = Objects.requireNonNull(
                properties,
                "queryPlanSafetyProperties must not be null");
    }

    @Override
    public final AnalysisDataSource dataSource() {
        return analysisDataSource;
    }

    @Override
    public final void check(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(
                    "sql must not be blank");
        }

        try (Connection connection = dataSource.getConnection()) {

            connection.setReadOnly(true);

            try (PreparedStatement statement =
                         connection.prepareStatement(explainSql(sql))) {

                statement.setQueryTimeout(timeoutSeconds());

                try (ResultSet resultSet = statement.executeQuery()) {
                    inspect(resultSet);
                }
            }

        } catch (QueryPlanSafetyException exception) {
            log.warn(
                    "Rejected {} query plan: {}",
                    analysisDataSource,
                    exception.reason());
            throw exception;

        } catch (SQLException exception) {
            QueryPlanSafetyException.Reason reason =
                    isTimeout(exception)
                            ? QueryPlanSafetyException.Reason.EXPLAIN_TIMEOUT
                            : QueryPlanSafetyException.Reason.EXPLAIN_FAILED;

            log.warn(
                    "Could not validate {} query plan: {}",
                    analysisDataSource,
                    reason);

            throw new QueryPlanSafetyException(
                    reason,
                    "Query plan could not be validated safely",
                    exception);
        }
    }

    protected abstract String explainSql(String sql);

    protected abstract void inspect(ResultSet resultSet)
            throws SQLException;

    protected final void reject(
            QueryPlanSafetyException.Reason reason) {

        throw new QueryPlanSafetyException(
                reason,
                "Query plan exceeds the configured safety policy");
    }

    private int timeoutSeconds() {
        long milliseconds = properties.explainTimeout().toMillis();
        return Math.toIntExact(
                Math.max(1, (milliseconds + 999) / 1000));
    }

    private boolean isTimeout(SQLException exception) {
        if (exception instanceof SQLTimeoutException) {
            return true;
        }

        String sqlState = exception.getSQLState();
        return "57014".equals(sqlState)
                || "HYT00".equals(sqlState)
                || "HYT01".equals(sqlState);
    }
}
