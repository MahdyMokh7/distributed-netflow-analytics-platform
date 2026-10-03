package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;

class SqlValidatorTest {

    private final SqlAstParser parser = new SqlAstParser();
    private final SqlValidator validator = new SqlValidator();

    @Test
    void givenApprovedPostgresqlSelectWhenValidatedThenParsedSqlIsAccepted() {
        // Given: one PostgreSQL SELECT referencing its routed table
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "window_start");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "SELECT src_ip FROM traffic_aggregates LIMIT 10");

        // When: first-stage SQL validation runs
        ParsedSql validatedSql = validator.validate(parsedSql, context);

        // Then: the parsed SQL is accepted unchanged
        assertThat(validatedSql).isSameAs(parsedSql);
    }

    @Test
    void givenApprovedStarRocksSelectWhenValidatedThenQualifiedTableIsAccepted() {
        // Given: one StarRocks SELECT referencing the qualified routed table
        SqlGenerationContext context = context(
                AnalysisDataSource.STARROCKS,
                "nimbo.netflow",
                "kafka_timestamp");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.STARROCKS,
                "SELECT ipv4_src_addr FROM nimbo.netflow LIMIT 10");

        // When: first-stage SQL validation runs
        ParsedSql validatedSql = validator.validate(parsedSql, context);

        // Then: the qualified StarRocks table is accepted
        assertThat(validatedSql).isSameAs(parsedSql);
    }

    @Test
    void givenMultipleStatementsWhenValidatedThenSqlIsRejected() {
        // Given: two syntactically valid SELECT statements
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "window_start");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "SELECT src_ip FROM traffic_aggregates; "
                        + "SELECT dst_ip FROM connection_aggregates");

        // When: first-stage SQL validation runs
        var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

        // Then: multiple statements are rejected
        assertion
                .isInstanceOf(SqlValidationException.class)
                .hasMessage("Generated SQL must contain exactly one statement");
    }

    @Test
    void givenNonSelectStatementWhenValidatedThenSqlIsRejected() {
        // Given: one syntactically valid mutating statement
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "window_start");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "DELETE FROM traffic_aggregates");

        // When: first-stage SQL validation runs
        var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

        // Then: unsupported statement types are rejected
        assertion
                .isInstanceOf(SqlValidationException.class)
                .hasMessage("Generated SQL must be a SELECT statement");
    }

    @Test
    void givenUnknownTableWhenValidatedThenSqlIsRejected() {
        // Given: a SELECT referencing a table outside the routed context
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "window_start");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "SELECT ipv4_src_addr FROM nimbo.netflow");

        // When: first-stage SQL validation runs
        var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

        // Then: the unapproved table is rejected
        assertion
                .isInstanceOf(SqlValidationException.class)
                .hasMessageContaining("nimbo.netflow");
    }

    @Test
    void givenDifferentSelectedDatasourceWhenValidatedThenSqlIsRejected() {
        // Given: parsed PostgreSQL SQL with a StarRocks generation context
        SqlGenerationContext context = context(
                AnalysisDataSource.STARROCKS,
                "nimbo.netflow",
                "kafka_timestamp");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "SELECT src_ip FROM traffic_aggregates");

        // When: first-stage SQL validation runs
        var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

        // Then: the datasource mismatch is rejected
        assertion
                .isInstanceOf(SqlValidationException.class)
                .hasMessage("Generated SQL datasource does not match the selected datasource");
    }

    @Test
    void givenSelectWithoutTableWhenValidatedThenSqlIsRejected() {
        // Given: a SELECT that does not use analytics data
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "window_start");
        ParsedSql parsedSql = parse(
                AnalysisDataSource.POSTGRESQL,
                "SELECT 1");

        // When: first-stage SQL validation runs
        var assertion = assertThatThrownBy(() -> validator.validate(parsedSql, context));

        // Then: a query without an approved table is rejected
        assertion
                .isInstanceOf(SqlValidationException.class)
                .hasMessage("Generated SQL must reference an approved table");
    }

    private ParsedSql parse(
            AnalysisDataSource dataSource,
            String sql) {

        return parser.parse(new GeneratedSqlCandidate(dataSource, sql));
    }

    private static SqlGenerationContext context(
            AnalysisDataSource dataSource,
            String tableName,
            String timeColumn) {

        SemanticColumn timestamp = new SemanticColumn(
                timeColumn,
                "TIMESTAMP",
                "UTC event or window timestamp.",
                Set.of(SemanticColumn.Role.TIME),
                false);

        return new SqlGenerationContext(
                dataSource,
                List.of(new SqlGenerationContext.Table(
                        tableName,
                        "NetFlow analytics data.",
                        "analytics row",
                        new SqlGenerationContext.TimeSemantics(
                                timeColumn,
                                ZoneOffset.UTC,
                                Duration.ofHours(1)),
                        List.of(timestamp),
                        List.of())),
                List.of("Generate one read-only SELECT statement."));
    }
}
