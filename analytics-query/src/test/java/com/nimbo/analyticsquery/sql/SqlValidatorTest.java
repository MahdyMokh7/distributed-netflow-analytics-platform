package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SqlValidatorTest {

  private final SqlAstParser parser = new SqlAstParser();
  private final SqlValidator validator = new SqlValidator();

  @Test
  void givenApprovedPostgresqlQueryWhenValidatedThenParsedSqlIsAccepted() {
    // Given: a bounded aggregate query using approved PostgreSQL syntax
    SqlGenerationContext context =
        context(AnalysisDataSource.POSTGRESQL, "traffic_aggregates", "window_start", "src_ip");
    ParsedSql parsedSql =
        parse(
            AnalysisDataSource.POSTGRESQL,
            """
            SELECT src_ip, SUM(in_bytes) AS total_bytes
            FROM traffic_aggregates
            WHERE window_start >= TIMESTAMP '2026-10-03 08:00:00'
              AND window_start < TIMESTAMP '2026-10-03 09:00:00'
            GROUP BY src_ip
            ORDER BY total_bytes DESC
            LIMIT 10
            """);

    // When: full V1 SQL policy validation runs
    ParsedSql validatedSql = validator.validate(parsedSql, context);

    // Then: the approved query is accepted unchanged
    assertThat(validatedSql).isSameAs(parsedSql);
  }

  @Test
  void givenApprovedStarRocksQueryWhenValidatedThenParsedSqlIsAccepted() {
    // Given: a bounded query using approved StarRocks columns and functions
    SqlGenerationContext context =
        context(AnalysisDataSource.STARROCKS, "nimbo.netflow", "process_time", "ipv4_src_addr");
    ParsedSql parsedSql =
        parse(
            AnalysisDataSource.STARROCKS,
            """
            SELECT ipv4_src_addr, COUNT(*) AS flow_count
            FROM nimbo.netflow
            WHERE process_time BETWEEN 1791014400000 AND 1791018000000
            GROUP BY ipv4_src_addr
            ORDER BY flow_count DESC
            LIMIT 10
            """);

    // When: full V1 SQL policy validation runs
    ParsedSql validatedSql = validator.validate(parsedSql, context);

    // Then: the routed StarRocks query is accepted
    assertThat(validatedSql).isSameAs(parsedSql);
  }

  @Test
  void givenMultipleStatementsWhenValidatedThenStatementCountIsRejected() {
    // Given: two syntactically valid SELECT statements
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, "SELECT src_ip FROM traffic_aggregates; SELECT 1");

    // When: the statements are validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the controlled statement-count policy reason is returned
    assertViolation(assertion, SqlPolicyViolation.STATEMENT_COUNT);
  }

  @Test
  void givenMutatingStatementWhenValidatedThenReadOnlyPolicyRejectsIt() {
    // Given: one syntactically valid DELETE statement
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql = parse(AnalysisDataSource.POSTGRESQL, "DELETE FROM traffic_aggregates");

    // When: the statement is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the read-only policy rejects the statement
    assertViolation(assertion, SqlPolicyViolation.READ_ONLY_REQUIRED);
  }

  @Test
  void givenUnknownOrCrossDatasourceTableWhenValidatedThenTablePolicyRejectsIt() {
    // Given: PostgreSQL-routed SQL referencing the StarRocks table
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, "SELECT ipv4_src_addr FROM nimbo.netflow LIMIT 10");

    // When: the table reference is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: cross-datasource table access is rejected
    assertViolation(assertion, SqlPolicyViolation.TABLE_NOT_ALLOWED);
  }

  @Test
  void givenSystemCatalogTableWhenValidatedThenTablePolicyRejectsIt() {
    // Given: a query attempting to inspect the PostgreSQL catalog
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, "SELECT tablename FROM pg_catalog.pg_tables LIMIT 10");

    // When: the system table reference is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: catalog access is rejected by the table allowlist
    assertViolation(assertion, SqlPolicyViolation.TABLE_NOT_ALLOWED);
  }

  @Test
  void givenUnknownColumnWhenValidatedThenColumnPolicyRejectsIt() {
    // Given: an otherwise bounded query referencing an unapproved column
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, boundedPostgresQuery("secret_column", "LIMIT 10"));

    // When: column references are validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the unapproved column is rejected
    assertViolation(assertion, SqlPolicyViolation.COLUMN_NOT_ALLOWED);
  }

  @Test
  void givenWildcardProjectionWhenValidatedThenColumnPolicyRejectsIt() {
    // Given: a bounded query requesting every physical column
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, boundedPostgresQuery("*", "LIMIT 10"));

    // When: the projection is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: wildcard projection is rejected
    assertViolation(assertion, SqlPolicyViolation.COLUMN_NOT_ALLOWED);
  }

  @Test
  void givenUnapprovedFunctionWhenValidatedThenFunctionPolicyRejectsIt() {
    // Given: a bounded SELECT invoking an administrative PostgreSQL function
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, boundedPostgresQuery("PG_SLEEP(1)", "LIMIT 10"));

    // When: function use is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the function allowlist rejects it
    assertViolation(assertion, SqlPolicyViolation.FUNCTION_NOT_ALLOWED);
  }

  @Test
  void givenJoinWhenValidatedThenJoinPolicyRejectsIt() {
    // Given: a query joining two analytics tables without an approved join contract
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(
            AnalysisDataSource.POSTGRESQL,
            """
            SELECT t.src_ip
            FROM traffic_aggregates t
            JOIN connection_aggregates c ON c.src_ip = t.src_ip
            WHERE t.window_start >= TIMESTAMP '2026-10-03 08:00:00'
              AND t.window_start < TIMESTAMP '2026-10-03 09:00:00'
            LIMIT 10
            """);

    // When: join use is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the V1 fail-closed join policy rejects it
    assertViolation(assertion, SqlPolicyViolation.JOIN_NOT_ALLOWED);
  }

  @Test
  void givenTimeBoundsOnlyInsideOrWhenValidatedThenTimePolicyRejectsIt() {
    // Given: apparent time bounds that can be bypassed through an OR branch
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(
            AnalysisDataSource.POSTGRESQL,
            """
            SELECT src_ip
            FROM traffic_aggregates
            WHERE window_start >= TIMESTAMP '2026-10-03 08:00:00'
               OR window_start < TIMESTAMP '2026-10-03 09:00:00'
            LIMIT 10
            """);

    // When: time bounds are validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: non-conjunctive bounds are not treated as a bounded scan
    assertViolation(assertion, SqlPolicyViolation.TIME_BOUND_REQUIRED);
  }

  @Test
  void givenExcessiveLimitWhenValidatedThenLimitPolicyRejectsIt() {
    // Given: a bounded query exceeding the V1 result limit
    SqlGenerationContext context = postgresContext();
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, boundedPostgresQuery("src_ip", "LIMIT 1001"));

    // When: the result bound is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the oversized result limit is rejected
    assertViolation(assertion, SqlPolicyViolation.RESULT_LIMIT_REQUIRED);
  }

  @Test
  void givenDifferentSelectedDatasourceWhenValidatedThenDatasourcePolicyRejectsIt() {
    // Given: parsed PostgreSQL SQL paired with a StarRocks context
    SqlGenerationContext context =
        context(AnalysisDataSource.STARROCKS, "nimbo.netflow", "process_time", "ipv4_src_addr");
    ParsedSql parsedSql =
        parse(AnalysisDataSource.POSTGRESQL, boundedPostgresQuery("src_ip", "LIMIT 10"));

    // When: datasource consistency is validated
    var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

    // Then: the mismatch is rejected before AST policy evaluation
    assertViolation(assertion, SqlPolicyViolation.DATASOURCE_MISMATCH);
  }

  private ParsedSql parse(AnalysisDataSource dataSource, String sql) {
    return parser.parse(new GeneratedSqlCandidate(dataSource, sql));
  }

  private static SqlGenerationContext postgresContext() {
    return context(AnalysisDataSource.POSTGRESQL, "traffic_aggregates", "window_start", "src_ip");
  }

  private static SqlGenerationContext context(
      AnalysisDataSource dataSource, String tableName, String timeColumn, String dimensionColumn) {

    SemanticColumn timestamp =
        new SemanticColumn(
            timeColumn,
            "TIMESTAMP",
            "UTC event or window timestamp.",
            Set.of(SemanticColumn.Role.TIME),
            false);
    SemanticColumn dimension =
        new SemanticColumn(
            dimensionColumn,
            "VARCHAR(64)",
            "Approved NetFlow dimension.",
            Set.of(SemanticColumn.Role.DIMENSION),
            false,
            SemanticFieldNames.SOURCE_IP,
            Set.of(Aggregation.COUNT_DISTINCT));
    SemanticColumn metric =
        new SemanticColumn(
            "in_bytes",
            "BIGINT",
            "Inbound bytes.",
            Set.of(SemanticColumn.Role.METRIC),
            false,
            SemanticFieldNames.IN_BYTES,
            Set.of(Aggregation.SUM));

    return new SqlGenerationContext(
        dataSource,
        List.of(
            new SqlGenerationContext.Table(
                tableName,
                "NetFlow analytics data.",
                "analytics row",
                new SqlGenerationContext.TimeSemantics(
                    timeColumn, ZoneOffset.UTC, Duration.ofHours(1)),
                List.of(timestamp, dimension, metric),
                List.of())),
        List.of("Generate one read-only SELECT statement."));
  }

  private static String boundedPostgresQuery(String projection, String limit) {
    return """
           SELECT %s
           FROM traffic_aggregates
           WHERE window_start >= TIMESTAMP '2026-10-03 08:00:00'
             AND window_start < TIMESTAMP '2026-10-03 09:00:00'
           %s
           """
        .formatted(projection, limit);
  }

  private static void assertViolation(
      org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertion,
      SqlPolicyViolation expected) {

    assertion.isInstanceOfSatisfying(
        SqlValidationException.class,
        exception -> assertThat(exception.violation()).isEqualTo(expected));
  }
}
