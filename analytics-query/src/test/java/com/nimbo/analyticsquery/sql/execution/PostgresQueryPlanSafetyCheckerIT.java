package com.nimbo.analyticsquery.sql.execution;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.QueryPlanSafetyProperties;
import com.nimbo.analyticsquery.support.PostgresSqlExecutionTestExtension;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.util.unit.DataSize;

@ExtendWith(PostgresSqlExecutionTestExtension.class)
class PostgresQueryPlanSafetyCheckerIT {

  @Test
  void givenBoundedPostgresPlanWhenCheckedThenItIsAccepted(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: PostgreSQL limits above the expected bounded plan
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresQueryPlanSafetyChecker checker = checker(dataSource, 10_000, 10_000);

      // When: the approved query plan is checked
      var invocation =
          assertThatCode(() -> checker.check("SELECT src_ip FROM traffic_aggregates LIMIT 10"));

      // Then: the plan is accepted
      invocation.doesNotThrowAnyException();
    }
  }

  @Test
  void givenLargePostgresEstimateWhenCheckedThenItIsRejected(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: a PostgreSQL row-estimate limit below the planned scan
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresQueryPlanSafetyChecker checker = checker(dataSource, 10, 10_000);

      // When: a plan estimates more rows than allowed
      var invocation =
          assertThatThrownBy(
              () -> checker.check("SELECT value FROM generate_series(1, 100) AS value"));

      // Then: the controlled row-estimate reason is returned
      invocation
          .isInstanceOf(QueryPlanSafetyException.class)
          .extracting(exception -> ((QueryPlanSafetyException) exception).reason())
          .isEqualTo(QueryPlanSafetyException.Reason.ROW_ESTIMATE_LIMIT_EXCEEDED);
    }
  }

  @Test
  void givenLowPostgresCostLimitWhenCheckedThenExpensivePlanIsRejected(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: a PostgreSQL cost limit below the planned query cost
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresQueryPlanSafetyChecker checker = checker(dataSource, 1_000, 0.01);

      // When: a plan exceeds the configured PostgreSQL cost limit
      var invocation =
          assertThatThrownBy(() -> checker.check("SELECT src_ip FROM traffic_aggregates LIMIT 10"));

      // Then: the controlled datasource-specific cost reason is returned
      invocation
          .isInstanceOf(QueryPlanSafetyException.class)
          .extracting(exception -> ((QueryPlanSafetyException) exception).reason())
          .isEqualTo(QueryPlanSafetyException.Reason.COST_LIMIT_EXCEEDED);
    }
  }

  private static PostgresQueryPlanSafetyChecker checker(
      HikariDataSource dataSource, long maxEstimatedRows, double maxTotalCost) {

    QueryPlanSafetyProperties properties =
        new QueryPlanSafetyProperties(
            Duration.ofSeconds(5), DataSize.ofKilobytes(64), maxEstimatedRows, maxTotalCost);

    return new PostgresQueryPlanSafetyChecker(dataSource, new ObjectMapper(), properties);
  }

  private static HikariDataSource dataSource(PostgresSqlExecutionTestExtension postgres) {

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(postgres.jdbcUrl());
    config.setUsername(postgres.username());
    config.setPassword(postgres.password());
    config.setMaximumPoolSize(1);
    config.setReadOnly(true);

    return new HikariDataSource(config);
  }
}
