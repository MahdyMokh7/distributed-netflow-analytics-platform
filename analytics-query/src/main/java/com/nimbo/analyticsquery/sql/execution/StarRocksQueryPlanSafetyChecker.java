package com.nimbo.analyticsquery.sql.execution;

import com.nimbo.analyticsquery.config.QueryPlanSafetyProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
    prefix = "analytics-query.sql-execution.starrocks",
    name = "enabled",
    havingValue = "true")
public class StarRocksQueryPlanSafetyChecker extends JdbcQueryPlanSafetyChecker {

  private static final Pattern CARDINALITY =
      Pattern.compile("cardinality\\s*[:=]\\s*(-?\\d+)", Pattern.CASE_INSENSITIVE);

  public StarRocksQueryPlanSafetyChecker(
      @Qualifier("starRocksSqlExecutionDataSource") DataSource dataSource,
      QueryPlanSafetyProperties properties) {

    super(AnalysisDataSource.STARROCKS, dataSource, properties);
  }

  @Override
  protected String explainSql(String sql) {
    return "EXPLAIN " + sql;
  }

  @Override
  protected void inspect(ResultSet resultSet) throws SQLException {

    long size = 0;
    long maximumCardinality = 0;
    boolean hasPlan = false;

    while (resultSet.next()) {
      hasPlan = true;
      String line = resultSet.getString(1);
      size += line.getBytes(StandardCharsets.UTF_8).length;

      if (size > properties.maxExplainSize().toBytes()) {
        reject(QueryPlanSafetyException.Reason.PLAN_OUTPUT_TOO_LARGE);
      }

      Matcher matcher = CARDINALITY.matcher(line);

      while (matcher.find()) {
        long cardinality = Long.parseLong(matcher.group(1));

        if (cardinality >= 0) {
          maximumCardinality = Math.max(maximumCardinality, cardinality);
        }
      }
    }

    if (!hasPlan) {
      reject(QueryPlanSafetyException.Reason.EXPLAIN_FAILED);
    }

    if (maximumCardinality > properties.maxEstimatedRows()) {
      reject(QueryPlanSafetyException.Reason.ROW_ESTIMATE_LIMIT_EXCEEDED);
    }
  }
}
