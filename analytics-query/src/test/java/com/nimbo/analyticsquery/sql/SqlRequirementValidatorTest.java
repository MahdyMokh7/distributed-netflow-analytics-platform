package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.PostgresSemanticCatalogSource;
import com.nimbo.analyticsquery.schema.SemanticTable;

class SqlRequirementValidatorTest {

    private static final Instant START = Instant.parse("2026-10-05T10:00:00Z");
    private static final Instant END = Instant.parse("2026-10-05T11:00:00Z");

    private final SqlAstParser parser = new SqlAstParser();
    private final SqlRequirementValidator validator = new SqlRequirementValidator();
    private final SqlGenerationContext context = context();

    @Test
    void givenAlignedSqlWhenRequirementsAreValidatedThenSqlIsAccepted() {
        // Given: SQL preserving the validated metric, IP, time, grouping and limit
        QueryRequirements requirements = requirements();
        ParsedSql sql = parse(
                "SELECT src_ip, SUM(in_bytes + out_bytes) FROM traffic_aggregates "
                        + "WHERE window_start >= '2026-10-05T10:00:00Z' "
                        + "AND window_start < '2026-10-05T11:00:00Z' "
                        + "AND src_ip = '10.0.0.1' GROUP BY src_ip LIMIT 10");

        // When: semantic conformance is validated
        var validation = assertThatCode(() -> validator.validate(sql, requirements, context));

        // Then: the aligned SQL is accepted
        validation.doesNotThrowAnyException();
    }

    @Test
    void givenSqlOmittingIpValueWhenRequirementsAreValidatedThenSqlIsRejected() {
        // Given: safe SQL that omits one validated requirement
        QueryRequirements requirements = requirements();
        ParsedSql sql = parse(
                "SELECT src_ip, SUM(in_bytes + out_bytes) FROM traffic_aggregates "
                        + "WHERE window_start >= '2026-10-05T10:00:00Z' "
                        + "AND window_start < '2026-10-05T11:00:00Z' "
                        + "GROUP BY src_ip LIMIT 10");

        // When: semantic conformance is validated
        var validation = assertThatThrownBy(() -> validator.validate(sql, requirements, context));

        // Then: the omitted requirement is rejected
        validation
                .isInstanceOf(SqlValidationException.class)
                .hasMessageContaining("IP value");
    }

    private ParsedSql parse(String sql) {
        return parser.parse(new GeneratedSqlCandidate(AnalysisDataSource.POSTGRESQL, sql));
    }

    private static QueryRequirements requirements() {
        return new QueryRequirements(
                List.of("SOURCE_IP"),
                List.of(
                        new QueryRequirements.MetricRequirement(
                                "TOTAL_BYTES", QueryRequirements.Aggregation.SUM)),
                List.of(),
                List.of(
                        new QueryRequirements.IpRequirement(
                                QueryRequirements.IpRole.SOURCE,
                                QueryRequirements.IpMatchType.EXACT,
                                List.of("10.0.0.1"))),
                QueryRequirements.TrafficDirection.UNSPECIFIED,
                new QueryRequirements.TimeRange(START, END),
                List.of("SOURCE_IP"),
                null,
                List.of(),
                10,
                false);
    }

    private static SqlGenerationContext context() {
        SemanticTable traffic = new PostgresSemanticCatalogSource().tables().stream()
                .filter(table -> table.name().equals("traffic_aggregates"))
                .findFirst()
                .orElseThrow();

        return new SqlGenerationContextFactory()
                .create(new DataSourceRoute(AnalysisDataSource.POSTGRESQL, traffic));
    }
}
