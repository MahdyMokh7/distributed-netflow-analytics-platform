package com.nimbo.analyticsquery.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToolMatcherTest {

    private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
    private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

    private final RestApiClient restApiClient = new RestApiClient(
            RestClient.builder()
                    .baseUrl("http://127.0.0.1:65535/api")
                    .build(),
            new ObjectMapper());

    private final AnalyticsTool trafficTool = new TrafficWindowsTool(restApiClient);
    private final AnalyticsTool connectionTool = new ConnectionWindowsTool(restApiClient);
    private final AnalyticsTool bandwidthTool = new TopBandwidthIpsTool(restApiClient);
    private final AnalyticsTool protocolTool = new ProtocolUsageWindowsTool(restApiClient);

    @Test
    void givenEachSupportedGrainWhenMatchedThenExactlyOneToolIsReturned() {
        // Given: one valid requirement set for every registered endpoint grain
        List<AnalyticsTool> tools = List.of(
                connectionTool,
                bandwidthTool,
                protocolTool,
                trafficTool);
        ToolMatcher matcher = new ToolMatcher(new ToolRegistry(tools));
        List<MatchCase> cases = List.of(
                new MatchCase(
                        requirements(
                                List.of(SemanticFieldNames.SOURCE_IP),
                                SemanticFieldNames.IN_BYTES,
                                QueryRequirements.Aggregation.NONE,
                                List.of()),
                        trafficTool),
                new MatchCase(
                        requirements(
                                List.of(
                                        SemanticFieldNames.SOURCE_IP,
                                        SemanticFieldNames.DESTINATION_IP),
                                SemanticFieldNames.IN_BYTES,
                                QueryRequirements.Aggregation.NONE,
                                List.of()),
                        connectionTool),
                new MatchCase(
                        requirements(
                                List.of(SemanticFieldNames.L7_PROTOCOL),
                                SemanticFieldNames.IN_BYTES,
                                QueryRequirements.Aggregation.NONE,
                                List.of()),
                        protocolTool),
                new MatchCase(
                        bandwidthRequirements(),
                        bandwidthTool));

        // When: every requirement set is evaluated against all tools
        List<List<AnalyticsTool>> matches = cases.stream()
                .map(matchCase -> tools.stream()
                        .filter(tool -> tool.supports(matchCase.requirements()))
                        .toList())
                .toList();

        // Then: each endpoint grain has exactly its intended tool
        for (int index = 0; index < cases.size(); index++) {
            MatchCase matchCase = cases.get(index);
            assertThat(matches.get(index)).containsExactly(matchCase.expectedTool());
            assertThat(matcher.findTool(matchCase.requirements()))
                    .containsSame(matchCase.expectedTool());
        }
    }

    @Test
    void givenSharedFieldsWhenMatchedThenEndpointGrainSelectsOneTool() {
        // Given: traffic requirements using fields also present in connection rows
        List<AnalyticsTool> tools = List.of(
                connectionTool,
                trafficTool,
                protocolTool,
                bandwidthTool);
        ToolMatcher matcher = new ToolMatcher(new ToolRegistry(tools));
        QueryRequirements requirements = requirements(
                List.of(SemanticFieldNames.SOURCE_IP),
                SemanticFieldNames.IN_BYTES,
                QueryRequirements.Aggregation.NONE,
                List.of());

        // When: supporting tools and the selected tool are evaluated
        List<AnalyticsTool> supportingTools = tools.stream()
                .filter(tool -> tool.supports(requirements))
                .toList();
        Optional<AnalyticsTool> result = matcher.findTool(requirements);

        // Then: only the traffic-grain tool is eligible
        assertThat(supportingTools).containsExactly(trafficTool);
        assertThat(result).containsSame(trafficTool);
    }

    @Test
    void givenNoSupportingToolWhenMatchedThenNoToolIsReturned() {
        // Given: requirements that no current REST tool can satisfy
        ToolMatcher matcher = matcher(
                trafficTool,
                connectionTool,
                bandwidthTool,
                protocolTool);
        QueryRequirements requirements = requirements(
                List.of(SemanticFieldNames.SOURCE_IP),
                SemanticFieldNames.IN_BYTES,
                QueryRequirements.Aggregation.SUM,
                List.of(SemanticFieldNames.SOURCE_IP));

        // When: the unsupported requirements are matched
        Optional<AnalyticsTool> result = matcher.findTool(requirements);

        // Then: matching returns empty for later fallback eligibility
        assertThat(result).isEmpty();
    }

    @Test
    void givenDuplicateToolNamesWhenRegisteredThenRegistrationIsRejected() {
        // Given: the same approved tool registered twice
        List<AnalyticsTool> tools = List.of(trafficTool, trafficTool);

        // When: the registry is created
        var assertion = assertThatThrownBy(() -> new ToolRegistry(tools));

        // Then: duplicate registrations fail immediately
        assertion
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate analytics tool names detected");
    }

    @Test
    void givenMutableRegistrationListWhenRegistryIsCreatedThenRegistryIsIsolated() {
        // Given: a mutable list containing one approved tool
        List<AnalyticsTool> tools = new ArrayList<>();
        tools.add(trafficTool);

        // When: the registry is created and the source list is later changed
        ToolRegistry registry = new ToolRegistry(tools);
        tools.add(connectionTool);

        // Then: the registered tool set remains unchanged and immutable
        assertThat(registry.getTools()).containsExactly(trafficTool);
        assertThatThrownBy(() -> registry.getTools().add(connectionTool))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private ToolMatcher matcher(AnalyticsTool... tools) {
        return new ToolMatcher(new ToolRegistry(List.of(tools)));
    }

    private QueryRequirements requirements(
            List<String> dimensions,
            String metric,
            QueryRequirements.Aggregation aggregation,
            List<String> groupBy) {

        return new QueryRequirements(
                dimensions,
                List.of(new QueryRequirements.MetricRequirement(metric, aggregation)),
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.BOTH,
                new QueryRequirements.TimeRange(START, END),
                groupBy,
                null,
                List.of(),
                null,
                false);
    }

    private QueryRequirements bandwidthRequirements() {
        return new QueryRequirements(
                List.of(SemanticFieldNames.IP_ADDRESS),
                List.of(new QueryRequirements.MetricRequirement(
                        SemanticFieldNames.TOTAL_BYTES,
                        QueryRequirements.Aggregation.SUM)),
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.BOTH,
                new QueryRequirements.TimeRange(START, END),
                List.of(SemanticFieldNames.IP_ADDRESS),
                null,
                List.of(new QueryRequirements.SortRequirement(
                        SemanticFieldNames.TOTAL_BYTES,
                        QueryRequirements.Aggregation.SUM,
                        QueryRequirements.SortDirection.DESC)),
                10,
                false);
    }

    private record MatchCase(
            QueryRequirements requirements,
            AnalyticsTool expectedTool) {
    }
}
