package com.nimbo.analyticsquery.sql.execution;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.nimbo.analyticsquery.config.QueryPlanSafetyProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

@Component
@ConditionalOnProperty(
        prefix = "analytics-query.sql-execution.postgresql",
        name = "enabled",
        havingValue = "true")
public class PostgresQueryPlanSafetyChecker
        extends JdbcQueryPlanSafetyChecker {

    private final ObjectMapper objectMapper;

    public PostgresQueryPlanSafetyChecker(
            @Qualifier("postgresSqlExecutionDataSource")
            DataSource dataSource,
            ObjectMapper objectMapper,
            QueryPlanSafetyProperties properties) {

        super(AnalysisDataSource.POSTGRESQL, dataSource, properties);
        this.objectMapper = objectMapper;
    }

    @Override
    protected String explainSql(String sql) {
        return "EXPLAIN (FORMAT JSON) " + sql;
    }

    @Override
    protected void inspect(ResultSet resultSet)
            throws SQLException {

        if (!resultSet.next()) {
            reject(QueryPlanSafetyException.Reason.EXPLAIN_FAILED);
        }

        String json = resultSet.getString(1);

        if (json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                > properties.maxExplainSize().toBytes()) {

            reject(QueryPlanSafetyException.Reason.PLAN_OUTPUT_TOO_LARGE);
        }

        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode plan = root.path(0).path("Plan");

            if (plan.isMissingNode()) {
                reject(QueryPlanSafetyException.Reason.EXPLAIN_FAILED);
            }

            if (plan.path("Total Cost").asDouble()
                    > properties.postgresMaxTotalCost()) {

                reject(QueryPlanSafetyException.Reason.COST_LIMIT_EXCEEDED);
            }

            if (maximumPlanRows(plan)
                    > properties.maxEstimatedRows()) {

                reject(QueryPlanSafetyException.Reason.ROW_ESTIMATE_LIMIT_EXCEEDED);
            }

        } catch (IOException exception) {
            throw new QueryPlanSafetyException(
                    QueryPlanSafetyException.Reason.EXPLAIN_FAILED,
                    "PostgreSQL returned an unreadable query plan",
                    exception);
        }
    }

    private long maximumPlanRows(JsonNode plan) {
        long maximum = plan.path("Plan Rows").asLong(0);

        for (JsonNode child : plan.path("Plans")) {
            maximum = Math.max(maximum, maximumPlanRows(child));
        }

        return maximum;
    }
}
