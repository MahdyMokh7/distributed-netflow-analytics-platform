package com.nimbo.analyticsquery.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import com.nimbo.analyticsquery.schema.PostgresSemanticCatalogSource;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SqlFallbackEligibilityPolicyTest {

  private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
  private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

  private final SqlFallbackEligibilityPolicy policy =
      new SqlFallbackEligibilityPolicy(
          new CuratedSemanticSchemaCatalog(List.of(new PostgresSemanticCatalogSource())));

  @Test
  void givenRequirementsCoveredByOneTableWhenEvaluatedThenFallbackIsEligible() {
    // Given: source traffic requirements available from one approved table
    QueryRequirements requirements =
        requirements(
            List.of(SemanticFieldNames.SOURCE_IP),
            List.of(metric(SemanticFieldNames.RECORD_COUNT)),
            List.of());

    // When: SQL fallback eligibility is evaluated
    SqlFallbackEligibility result = policy.evaluate(requirements);

    // Then: the request may continue to deterministic routing
    assertThat(result.eligible()).isTrue();
    assertThat(result.status()).isEqualTo(AnalysisStatus.SUCCESS);
    assertThat(result.message()).isNull();
  }

  @Test
  void givenUnavailableInformationWhenEvaluatedThenFallbackIsDenied() {
    // Given: requirements containing information absent from the approved catalog
    QueryRequirements requirements =
        requirements(List.of("UNAVAILABLE_FIELD"), List.of(), List.of());

    // When: SQL fallback eligibility is evaluated
    SqlFallbackEligibility result = policy.evaluate(requirements);

    // Then: unavailable information is rejected before routing
    assertThat(result.eligible()).isFalse();
    assertThat(result.status()).isEqualTo(AnalysisStatus.UNSUPPORTED_ANALYSIS);
    assertThat(result.message()).contains("not available");
  }

  @Test
  void givenRequirementsSplitAcrossPostgresTablesWhenEvaluatedThenFallbackIsDenied() {
    // Given: one analysis requiring fields held only by different aggregate tables
    QueryRequirements requirements =
        requirements(
            List.of(SemanticFieldNames.DESTINATION_IP),
            List.of(metric(SemanticFieldNames.RECORD_COUNT)),
            List.of());

    // When: SQL fallback eligibility is evaluated
    SqlFallbackEligibility result = policy.evaluate(requirements);

    // Then: unsupported V1 multi-table SQL is rejected
    assertThat(result.eligible()).isFalse();
    assertThat(result.status()).isEqualTo(AnalysisStatus.UNSUPPORTED_ANALYSIS);
    assertThat(result.message()).contains("multiple semantic tables");
  }

  @Test
  void givenEitherIpRequirementWhenUnifiedIpTableExistsThenFallbackIsEligible() {
    // Given: an either-side IP filter satisfiable by the IP bandwidth cube
    QueryRequirements requirements =
        requirements(
            List.of(SemanticFieldNames.IP_ADDRESS),
            List.of(metric(SemanticFieldNames.IN_BYTES)),
            List.of(
                new QueryRequirements.IpRequirement(
                    QueryRequirements.IpRole.EITHER,
                    QueryRequirements.IpMatchType.EXACT,
                    List.of("10.0.0.1"))));

    // When: SQL fallback eligibility is evaluated
    SqlFallbackEligibility result = policy.evaluate(requirements);

    // Then: unified IP semantics satisfy the requirement
    assertThat(result.eligible()).isTrue();
  }

  private static QueryRequirements requirements(
      List<String> dimensions,
      List<QueryRequirements.MetricRequirement> metrics,
      List<QueryRequirements.IpRequirement> ipRequirements) {

    return new QueryRequirements(
        dimensions,
        metrics,
        List.of(),
        ipRequirements,
        QueryRequirements.TrafficDirection.UNSPECIFIED,
        new QueryRequirements.TimeRange(START, END),
        dimensions,
        null,
        List.of(),
        null,
        false);
  }

  private static QueryRequirements.MetricRequirement metric(String field) {

    return new QueryRequirements.MetricRequirement(field, QueryRequirements.Aggregation.SUM);
  }
}
