package com.nimbo.analyticsquery.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.config.RestApiClientConfiguration;
import com.nimbo.analyticsquery.config.RestApiProperties;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.support.RestApiTestServerExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.client.RestClient;

class ConnectionWindowsToolTest {

  private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
  private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

  @RegisterExtension
  static final RestApiTestServerExtension REST_API = new RestApiTestServerExtension();

  @Test
  void givenSourceAndDestinationRequirementsWhenExecutedThenFilteredEndpointIsUsed() {
    // Given: exact source and destination requirements with one downstream row
    REST_API.respondWith(200, successfulResponse());
    ConnectionWindowsTool tool = new ConnectionWindowsTool(client());
    QueryRequirements requirements =
        requirements(
            List.of(
                ip(QueryRequirements.IpRole.SOURCE, "10.0.0.1"),
                ip(QueryRequirements.IpRole.DESTINATION, "10.0.0.2")));

    // When: the approved connection tool executes
    var result = tool.execute(requirements);

    // Then: both filters are sent and the row is normalized
    assertThat(result.rows()).hasSize(1);
    assertThat(result.rows().get(0)).containsEntry("id", "connection-id");
    assertThat(REST_API.lastRequestUri().getPath()).isEqualTo("/api/connections");
    assertThat(REST_API.lastRequestUri().getRawQuery())
        .contains("srcIp=10.0.0.1", "dstIp=10.0.0.2");
  }

  @Test
  void givenEitherSideRequirementWhenExecutedThenIpPathEndpointIsUsed() {
    // Given: one exact either-side IP requirement
    REST_API.respondWith(200, successfulResponse());
    ConnectionWindowsTool tool = new ConnectionWindowsTool(client());
    QueryRequirements requirements =
        requirements(List.of(ip(QueryRequirements.IpRole.EITHER, "10.0.0.1")));

    // When: the connection tool executes the either-side query
    var result = tool.execute(requirements);

    // Then: the dedicated path endpoint is selected
    assertThat(result.rows()).hasSize(1);
    assertThat(REST_API.lastRequestUri().getPath()).isEqualTo("/api/connections/10.0.0.1");
  }

  @Test
  void givenNoDataResponseWhenExecutedThenEmptySuccessIsReturned() {
    // Given: the REST API reports no connection data
    REST_API.respondWith(404, "{\"success\":false,\"message\":\"No data\"}");
    ConnectionWindowsTool tool = new ConnectionWindowsTool(client());

    // When: supported unfiltered requirements are executed
    var result = tool.execute(requirements(List.of()));

    // Then: no data is a successful empty analytics result
    assertThat(result.rows()).isEmpty();
    assertThat(result.columns()).contains("srcIp", "dstIp", "windowStart");
  }

  @Test
  void givenUnavailableResponseWhenExecutedThenControlledToolFailureIsRaised() {
    // Given: the REST API is temporarily unavailable
    REST_API.respondWith(503, "{\"success\":false,\"message\":\"Unavailable\"}");
    ConnectionWindowsTool tool = new ConnectionWindowsTool(client());

    // When: supported requirements are executed
    var failure = assertThatThrownBy(() -> tool.execute(requirements(List.of())));

    // Then: the dependency failure remains controlled
    failure
        .isInstanceOf(ToolExecutionException.class)
        .hasMessage("Analytics data service is temporarily unavailable");
  }

  private RestApiClient client() {
    RestApiProperties properties =
        new RestApiProperties(
            REST_API.baseUrl(),
            new RestApiProperties.Timeout(Duration.ofSeconds(1), Duration.ofSeconds(1)));
    RestClient restClient =
        new RestApiClientConfiguration()
            .restApiRestClient(RestClient.builder(), properties);
    return new RestApiClient(restClient, new ObjectMapper().findAndRegisterModules());
  }

  private QueryRequirements requirements(List<QueryRequirements.IpRequirement> ipRequirements) {
    return new QueryRequirements(
        List.of(SemanticFieldNames.SOURCE_IP, SemanticFieldNames.DESTINATION_IP),
        List.of(
            new QueryRequirements.MetricRequirement(
                SemanticFieldNames.IN_BYTES, QueryRequirements.Aggregation.NONE)),
        List.of(),
        ipRequirements,
        QueryRequirements.TrafficDirection.BOTH,
        new QueryRequirements.TimeRange(START, END),
        List.of(),
        null,
        List.of(),
        10,
        false);
  }

  private QueryRequirements.IpRequirement ip(QueryRequirements.IpRole role, String value) {
    return new QueryRequirements.IpRequirement(
        role, QueryRequirements.IpMatchType.EXACT, List.of(value));
  }

  private String successfulResponse() {
    return """
        {
          "success": true,
          "data": [{
            "id": "connection-id",
            "srcIp": "10.0.0.1",
            "dstIp": "10.0.0.2",
            "windowStart": "2026-10-01T08:00:00Z",
            "windowEnd": "2026-10-01T09:00:00Z",
            "inBytes": 10,
            "outBytes": 20,
            "totalBytes": 30,
            "inPackets": 1,
            "outPackets": 2,
            "totalPackets": 3,
            "batchId": "11111111-1111-1111-1111-111111111111",
            "ingestedAt": "2026-10-01T09:00:00Z"
          }],
          "pagination": {
            "totalElements": 1,
            "totalPages": 1,
            "page": 0,
            "size": 10,
            "first": true,
            "last": true,
            "numberOfElements": 1
          },
          "timestamp": "2026-10-01T09:00:00Z"
        }
        """;
  }
}
