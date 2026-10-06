package com.nimbo.analyticsquery.sql.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.SqlExecutionProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.support.PostgresSqlExecutionTestExtension;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.util.unit.DataSize;

@ExtendWith(PostgresSqlExecutionTestExtension.class)
class PostgresSqlExecutorIT {

  @Test
  void givenApprovedSelectWhenExecutedThenRowsAreNormalized(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: a read-only executor connected with the restricted identity
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresSqlExecutor executor =
          executor(dataSource, DataSize.ofMegabytes(2), Duration.ofSeconds(5));

      // When: an approved query is executed
      AnalyticsResult result =
          executor.execute(
              """
              SELECT src_ip, in_bytes
              FROM traffic_aggregates
              ORDER BY src_ip
              LIMIT 10
              """);

      // Then: the JDBC result is normalized without changing its values
      assertThat(result.dataSource()).isEqualTo(AnalysisDataSource.POSTGRESQL);
      assertThat(result.columns()).containsExactly("src_ip", "in_bytes");
      assertThat(result.rows()).hasSize(2);
      assertThat(result.rows().get(0)).containsEntry("src_ip", "10.0.0.1");
    }
  }

  @Test
  void givenRestrictedIdentityWhenWriteIsAttemptedThenDatabaseRejectsIt(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: a PostgreSQL executor using the SELECT-only login
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresSqlExecutor executor =
          executor(dataSource, DataSize.ofMegabytes(2), Duration.ofSeconds(5));

      // When: a write reaches the database defense-in-depth boundary
      var invocation = assertThatThrownBy(() -> executor.execute("DELETE FROM traffic_aggregates"));

      // Then: execution fails without modifying the protected table
      invocation
          .isInstanceOf(SqlExecutionException.class)
          .extracting(exception -> ((SqlExecutionException) exception).kind())
          .isEqualTo(SqlExecutionException.Kind.DATASOURCE_ERROR);
    }
  }

  @Test
  void givenSlowQueryWhenExecutedThenStatementTimeoutIsReported(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: an executor with a one-second statement timeout
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresSqlExecutor executor =
          executor(dataSource, DataSize.ofMegabytes(2), Duration.ofSeconds(1));

      // When: a query exceeds the configured timeout
      var invocation = assertThatThrownBy(() -> executor.execute("SELECT pg_sleep(3)"));

      // Then: the failure is classified as a timeout
      invocation
          .isInstanceOf(SqlExecutionException.class)
          .extracting(exception -> ((SqlExecutionException) exception).kind())
          .isEqualTo(SqlExecutionException.Kind.TIMEOUT);
    }
  }

  @Test
  void givenOversizedResultWhenExecutedThenCompletePayloadIsRejected(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: an executor with a result limit smaller than its normalized response
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresSqlExecutor executor =
          executor(dataSource, DataSize.ofBytes(80), Duration.ofSeconds(5));

      // When: a result exceeds the complete serialized payload limit
      var invocation =
          assertThatThrownBy(
              () -> executor.execute("SELECT src_ip, in_bytes FROM traffic_aggregates LIMIT 10"));

      // Then: the result is rejected before it leaves the executor
      invocation
          .isInstanceOf(SqlExecutionException.class)
          .extracting(exception -> ((SqlExecutionException) exception).kind())
          .isEqualTo(SqlExecutionException.Kind.RESULT_TOO_LARGE);
    }
  }

  @Test
  void givenMoreRowsThanPolicyWhenExecutedThenDriverCapsTheResult(
      PostgresSqlExecutionTestExtension postgres) {
    // Given: a query capable of returning more rows than the V1 policy
    try (HikariDataSource dataSource = dataSource(postgres)) {
      PostgresSqlExecutor executor =
          executor(dataSource, DataSize.ofMegabytes(2), Duration.ofSeconds(5));

      // When: the query is executed through the bounded statement
      AnalyticsResult result =
          executor.execute("SELECT value FROM generate_series(1, 1100) AS value");

      // Then: no more than the policy maximum is materialized
      assertThat(result.rowCount()).isEqualTo(1000);
    }
  }

  private static PostgresSqlExecutor executor(
      HikariDataSource dataSource, DataSize maxResultSize, Duration statementTimeout) {

    return new PostgresSqlExecutor(
        dataSource,
        new ObjectMapper().findAndRegisterModules(),
        properties(maxResultSize, statementTimeout));
  }

  private static SqlExecutionProperties properties(
      DataSize maxResultSize, Duration statementTimeout) {

    SqlExecutionProperties.JdbcTarget disabled =
        new SqlExecutionProperties.JdbcTarget(false, "", "", "", 1, Duration.ofSeconds(1));

    return new SqlExecutionProperties(statementTimeout, maxResultSize, disabled, disabled);
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
