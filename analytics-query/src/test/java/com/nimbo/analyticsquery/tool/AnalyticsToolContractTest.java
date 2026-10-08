package com.nimbo.analyticsquery.tool;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.analyticsquery.client.restapi.RestApiResponse;
import com.nimbo.analyticsquery.client.restapi.dto.BandwidthRow;
import com.nimbo.analyticsquery.client.restapi.dto.ConnectionRow;
import com.nimbo.analyticsquery.client.restapi.dto.ProtocolRow;
import com.nimbo.analyticsquery.client.restapi.dto.TrafficRow;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AnalyticsToolContractTest {

  private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
  private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

  @Test
  void givenTrafficWindowRequirementsWhenCheckedThenTrafficToolSupportsThem() {
    // Given: source traffic requirements supported by the REST endpoint
    QueryRequirements requirements =
        windowRequirements(List.of(SemanticFieldNames.SOURCE_IP), SemanticFieldNames.IN_BYTES);

    // When: the traffic tool checks its finite capability
    boolean supported = TrafficWindowsTool.supportsRequirements(requirements);

    // Then: the requirements are accepted
    assertThat(supported).isTrue();
  }

  @Test
  void givenConnectionWindowRequirementsWhenCheckedThenConnectionToolSupportsThem() {
    // Given: connection requirements supported by the REST endpoint
    QueryRequirements requirements =
        windowRequirements(
            List.of(SemanticFieldNames.SOURCE_IP, SemanticFieldNames.DESTINATION_IP),
            SemanticFieldNames.OUT_PACKETS);

    // When: the connection tool checks its finite capability
    boolean supported = ConnectionWindowsTool.supportsRequirements(requirements);

    // Then: the requirements are accepted
    assertThat(supported).isTrue();
  }

  @Test
  void givenProtocolWindowRequirementsWhenCheckedThenProtocolToolSupportsThem() {
    // Given: protocol requirements supported by the REST endpoint
    QueryRequirements requirements =
        windowRequirements(
            List.of(SemanticFieldNames.L7_PROTOCOL), SemanticFieldNames.CONNECTION_COUNT);

    // When: the protocol tool checks its finite capability
    boolean supported = ProtocolUsageWindowsTool.supportsRequirements(requirements);

    // Then: the requirements are accepted
    assertThat(supported).isTrue();
  }

  @Test
  void givenRankedBandwidthRequirementsWhenCheckedThenBandwidthToolSupportsThem() {
    // Given: ranked bandwidth requirements supported by the REST endpoint
    QueryRequirements requirements =
        new QueryRequirements(
            List.of(SemanticFieldNames.IP_ADDRESS),
            List.of(metric(SemanticFieldNames.TOTAL_BYTES, QueryRequirements.Aggregation.SUM)),
            List.of(),
            List.of(),
            QueryRequirements.TrafficDirection.BOTH,
            new QueryRequirements.TimeRange(START, END),
            List.of(SemanticFieldNames.IP_ADDRESS),
            null,
            List.of(
                new QueryRequirements.SortRequirement(
                    SemanticFieldNames.TOTAL_BYTES,
                    QueryRequirements.Aggregation.SUM,
                    QueryRequirements.SortDirection.DESC)),
            10,
            false);

    // When: the bandwidth tool checks its finite capability
    boolean supported = TopBandwidthIpsTool.supportsRequirements(requirements);

    // Then: the requirements are accepted
    assertThat(supported).isTrue();
  }

  @Test
  void givenConnectionDimensionsInDifferentOrderWhenCheckedThenExactGrainIsSupported() {
    // Given: the complete connection grain in a different dimension order
    QueryRequirements requirements =
        windowRequirements(
            List.of(SemanticFieldNames.DESTINATION_IP, SemanticFieldNames.SOURCE_IP),
            SemanticFieldNames.IN_BYTES);

    // When: the connection tool checks its endpoint grain
    boolean supported = ConnectionWindowsTool.supportsRequirements(requirements);

    // Then: dimension order does not change the semantic grain
    assertThat(supported).isTrue();
  }

  @Test
  void givenPartialOrMissingGrainWhenCheckedThenWindowToolsRejectIt() {
    // Given: requirements that omit each endpoint's complete semantic grain
    QueryRequirements sourceOnly =
        windowRequirements(List.of(SemanticFieldNames.SOURCE_IP), SemanticFieldNames.IN_BYTES);
    QueryRequirements noDimensions = windowRequirements(List.of(), SemanticFieldNames.IN_BYTES);

    // When: tools for other grains evaluate the incomplete requirements
    boolean connectionSupportsSourceOnly = ConnectionWindowsTool.supportsRequirements(sourceOnly);
    boolean trafficSupportsNoDimensions = TrafficWindowsTool.supportsRequirements(noDimensions);
    boolean protocolSupportsNoDimensions =
        ProtocolUsageWindowsTool.supportsRequirements(noDimensions);

    // Then: subset relationships do not make the tools interchangeable
    assertThat(connectionSupportsSourceOnly).isFalse();
    assertThat(trafficSupportsNoDimensions).isFalse();
    assertThat(protocolSupportsNoDimensions).isFalse();
  }

  @Test
  void givenRangeLongerThanSevenDaysWhenCheckedThenRestToolsRejectIt() {
    // Given: endpoint-shaped requirements exceeding the REST API range
    QueryRequirements requirements =
        new QueryRequirements(
            List.of(SemanticFieldNames.SOURCE_IP),
            List.of(metric(SemanticFieldNames.IN_BYTES, QueryRequirements.Aggregation.NONE)),
            List.of(),
            List.of(),
            QueryRequirements.TrafficDirection.BOTH,
            new QueryRequirements.TimeRange(START, START.plusSeconds(8 * 24 * 60 * 60)),
            List.of(),
            null,
            List.of(),
            null,
            false);

    // When: the traffic tool checks the downstream endpoint limit
    boolean supported = TrafficWindowsTool.supportsRequirements(requirements);

    // Then: the incapable REST tool is not selected
    assertThat(supported).isFalse();
  }

  @Test
  void givenTrafficResponseWhenNormalizedThenColumnOrderAndValuesArePreserved() {
    // Given: one traffic row returned by the existing REST API
    TrafficRow row =
        new TrafficRow(
            "10.0.0.1",
            START,
            END,
            number(10),
            number(20),
            number(30),
            number(1),
            number(2),
            number(3),
            number(4),
            END);

    // When: the traffic tool normalizes the downstream response
    AnalyticsResult result = TrafficWindowsTool.normalize(response(row));

    // Then: normalized columns and values remain aligned
    assertThat(result.rows())
        .containsExactly(
            Map.ofEntries(
                Map.entry("srcIp", "10.0.0.1"),
                Map.entry("windowStart", START),
                Map.entry("windowEnd", END),
                Map.entry("inBytes", number(10)),
                Map.entry("outBytes", number(20)),
                Map.entry("totalBytes", number(30)),
                Map.entry("inPackets", number(1)),
                Map.entry("outPackets", number(2)),
                Map.entry("totalPackets", number(3)),
                Map.entry("recordCount", number(4)),
                Map.entry("ingestedAt", END)));
    assertThat(result.columns()).containsExactlyElementsOf(result.rows().get(0).keySet());
  }

  @Test
  void givenConnectionResponseWhenNormalizedThenColumnOrderAndValuesArePreserved() {
    // Given: one connection row returned by the existing REST API
    ConnectionRow row =
        new ConnectionRow(
            "connection-id",
            "10.0.0.1",
            "10.0.0.2",
            START,
            END,
            number(10),
            number(20),
            number(30),
            number(1),
            number(2),
            number(3),
            END);

    // When: the connection tool normalizes the downstream response
    AnalyticsResult result = ConnectionWindowsTool.normalize(response(row));

    // Then: normalized columns and values remain aligned
    assertThat(result.rows().get(0))
        .containsEntry("id", "connection-id")
        .containsEntry("srcIp", "10.0.0.1")
        .containsEntry("dstIp", "10.0.0.2")
        .containsEntry("totalBytes", number(30));
    assertThat(result.columns()).containsExactlyElementsOf(result.rows().get(0).keySet());
  }

  @Test
  void givenProtocolResponseWhenNormalizedThenColumnOrderAndValuesArePreserved() {
    // Given: one protocol row returned by the existing REST API
    ProtocolRow row =
        new ProtocolRow(
            6.0,
            START,
            END,
            number(10),
            number(20),
            number(30),
            number(1),
            number(2),
            number(3),
            4L,
            END);

    // When: the protocol tool normalizes the downstream response
    AnalyticsResult result = ProtocolUsageWindowsTool.normalize(response(row));

    // Then: normalized columns and values remain aligned
    assertThat(result.rows().get(0))
        .containsEntry("l7Protocol", 6.0)
        .containsEntry("connectionCount", 4L)
        .containsEntry("totalPackets", number(3));
    assertThat(result.columns()).containsExactlyElementsOf(result.rows().get(0).keySet());
  }

  @Test
  void givenBandwidthResponseWhenNormalizedThenColumnOrderAndValuesArePreserved() {
    // Given: one bandwidth row returned by the existing REST API
    BandwidthRow row =
        new BandwidthRow(
            "10.0.0.1",
            number(10),
            number(20),
            number(30),
            number(1),
            number(2),
            number(3));

    // When: the bandwidth tool normalizes the downstream response
    AnalyticsResult result = TopBandwidthIpsTool.normalize(response(row));

    // Then: normalized columns and values remain aligned
    assertThat(result.rows().get(0))
        .containsEntry("ip", "10.0.0.1")
        .containsEntry("totalBytes", number(30))
        .containsEntry("totalPackets", number(3));
    assertThat(result.columns()).containsExactlyElementsOf(result.rows().get(0).keySet());
  }

  private static QueryRequirements windowRequirements(List<String> dimensions, String metric) {

    return new QueryRequirements(
        dimensions,
        List.of(metric(metric, QueryRequirements.Aggregation.NONE)),
        List.of(),
        List.of(),
        QueryRequirements.TrafficDirection.BOTH,
        new QueryRequirements.TimeRange(START, END),
        List.of(),
        null,
        List.of(),
        null,
        false);
  }

  private static QueryRequirements.MetricRequirement metric(
      String field, QueryRequirements.Aggregation aggregation) {

    return new QueryRequirements.MetricRequirement(field, aggregation);
  }

  private static BigInteger number(long value) {
    return BigInteger.valueOf(value);
  }

  private static <T> RestApiResponse<List<T>> response(T row) {
    return new RestApiResponse<>(true, null, List.of(row), null, END);
  }
}
