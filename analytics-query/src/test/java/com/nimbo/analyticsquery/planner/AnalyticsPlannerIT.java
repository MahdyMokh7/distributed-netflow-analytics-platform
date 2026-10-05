package com.nimbo.analyticsquery.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.intent.model.ClarificationRequirement;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.support.RestApiTestServerExtension;

@SpringBootTest
@ActiveProfiles("test")
class AnalyticsPlannerIT {

    private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
    private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

    @RegisterExtension
    static final RestApiTestServerExtension REST_API = new RestApiTestServerExtension();

    @DynamicPropertySource
    static void dependencies(DynamicPropertyRegistry registry) {
        registry.add("rest-api.base-url", () -> REST_API.baseUrl().toString());
        registry.add("rest-api.timeout.read", () -> "250ms");
        registry.add("llm.base-url", () -> REST_API.originUrl().toString());
    }

    @Autowired private AnalyticsPlanner planner;

    @ParameterizedTest
    @EnumSource(
            value = AnalysisStatus.class,
            names = {"NEEDS_CLARIFICATION", "UNSUPPORTED_ANALYSIS", "OUT_OF_SCOPE", "REJECTED"})
    void givenNonSuccessfulIntentWhenPlannedThenExecutionStopsBeforeRestApi(AnalysisStatus status) {

        // Given: a validated non-successful analysis intent
        AnalysisIntent intent = terminalIntent(status);

        // When: the intent is planned
        PlannerResult result = planner.plan("question", intent);

        // Then: its status is preserved and no downstream request occurs
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.result()).isNull();
        assertThat(result.message())
                .isEqualTo(
                        status == AnalysisStatus.NEEDS_CLARIFICATION
                                ? "What time range should I use?"
                                : "Analytics request cannot be executed");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenSupportedTrafficIntentWhenPlannedThenToolResultIsReturned() {
        // Given: a supported traffic intent and a successful REST response
        REST_API.respondWith(200, trafficResponse());
        AnalysisIntent intent = successfulIntent(trafficRequirements());

        // When: the intent is planned and executed
        PlannerResult result = planner.plan("Show traffic", intent);

        // Then: normalized traffic data is returned as a successful tool result
        assertThat(result.status()).isEqualTo(AnalysisStatus.SUCCESS);
        assertThat(result.result()).isNotNull();
        assertThat(result.result().rowCount()).isEqualTo(1);
        assertThat(result.result().rows().get(0))
                .containsEntry("srcIp", "10.0.0.1")
                .containsEntry("recordCount", java.math.BigInteger.valueOf(4));
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenSelectedToolWithNoDataWhenPlannedThenSuccessfulEmptyResultIsReturned() {
        // Given: a supported traffic intent and a REST no-data response
        REST_API.respondWith(
                404,
                """
                {
                  "success": false,
                  "message": "No traffic data found"
                }
                """);
        AnalysisIntent intent = successfulIntent(trafficRequirements());

        // When: the selected tool is executed
        PlannerResult result = planner.plan("Show traffic", intent);

        // Then: zero rows remain a successful result without fallback
        assertThat(result.status()).isEqualTo(AnalysisStatus.SUCCESS);
        assertThat(result.result().rows()).isEmpty();
        assertThat(result.result().rowCount()).isZero();
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenSelectedToolWithServerFailureWhenPlannedThenDependencyFailureIsReturned() {
        // Given: a supported traffic intent and an unavailable REST dependency
        REST_API.respondWith(
                503,
                """
                {
                  "success": false,
                  "message": "REST API unavailable"
                }
                """);
        AnalysisIntent intent = successfulIntent(trafficRequirements());

        // When: the selected tool encounters the server failure
        PlannerResult result = planner.plan("Show traffic", intent);

        // Then: a controlled dependency failure is returned without fallback
        assertThat(result.status()).isEqualTo(AnalysisStatus.DEPENDENCY_UNAVAILABLE);
        assertThat(result.result()).isNull();
        assertThat(result.message()).isEqualTo("Analytics data service is temporarily unavailable");
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenSelectedToolWithMalformedResponseWhenPlannedThenExecutionFailureIsReturned() {
        // Given: a supported traffic intent and malformed downstream JSON
        REST_API.respondWith(200, "{not-json");
        AnalysisIntent intent = successfulIntent(trafficRequirements());

        // When: the selected tool processes the malformed response
        PlannerResult result = planner.plan("Show traffic", intent);

        // Then: a controlled execution failure is returned without response details
        assertThat(result.status()).isEqualTo(AnalysisStatus.EXECUTION_FAILED);
        assertThat(result.result()).isNull();
        assertThat(result.message()).isEqualTo("Analytics tool execution failed");
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenSelectedToolWithReadTimeoutWhenPlannedThenDependencyFailureIsReturned() {
        // Given: a supported traffic intent and a downstream response held past the read timeout
        REST_API.respondWith(200, trafficResponse());
        REST_API.holdResponse();
        AnalysisIntent intent = successfulIntent(trafficRequirements());

        // When: the selected tool reaches its configured read timeout
        PlannerResult result;
        try {
            result = planner.plan("Show traffic", intent);
        } finally {
            REST_API.releaseResponse();
        }

        // Then: a controlled dependency failure is returned without fallback
        assertThat(result.status()).isEqualTo(AnalysisStatus.DEPENDENCY_UNAVAILABLE);
        assertThat(result.result()).isNull();
        assertThat(result.message()).isEqualTo("Analytics data service is temporarily unavailable");
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    private static AnalysisIntent successfulIntent(QueryRequirements requirements) {
        return new AnalysisIntent(
                AnalysisStatus.SUCCESS, requirements, ClarificationRequirement.none());
    }

    private static AnalysisIntent terminalIntent(AnalysisStatus status) {
        ClarificationRequirement clarification =
                status == AnalysisStatus.NEEDS_CLARIFICATION
                        ? new ClarificationRequirement(
                                true,
                                List.of(ClarificationRequirement.MissingField.TIME_RANGE),
                                "What time range should I use?")
                        : ClarificationRequirement.none();

        return new AnalysisIntent(status, QueryRequirements.empty(), clarification);
    }

    private static QueryRequirements trafficRequirements() {
        return new QueryRequirements(
                List.of(SemanticFieldNames.SOURCE_IP),
                List.of(
                        new QueryRequirements.MetricRequirement(
                                SemanticFieldNames.RECORD_COUNT,
                                QueryRequirements.Aggregation.NONE)),
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.BOTH,
                new QueryRequirements.TimeRange(START, END),
                List.of(),
                null,
                List.of(),
                10,
                false);
    }

    private static String trafficResponse() {
        return """
                {
                  "success": true,
                  "data": [
                    {
                      "srcIp": "10.0.0.1",
                      "windowStart": "2026-10-01T08:00:00Z",
                      "windowEnd": "2026-10-01T09:00:00Z",
                      "inBytes": 10,
                      "outBytes": 20,
                      "totalBytes": 30,
                      "inPackets": 1,
                      "outPackets": 2,
                      "totalPackets": 3,
                      "recordCount": 4,
                      "batchId": "11111111-1111-1111-1111-111111111111",
                      "ingestedAt": "2026-10-01T09:00:00Z"
                    }
                  ],
                  "timestamp": "2026-10-01T09:00:00Z"
                }
                """;
    }
}
