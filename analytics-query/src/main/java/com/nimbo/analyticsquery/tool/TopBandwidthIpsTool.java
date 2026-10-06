package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.client.restapi.RestApiResponse;
import com.nimbo.analyticsquery.client.restapi.dto.BandwidthRow;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class TopBandwidthIpsTool implements AnalyticsTool {

  private static final Set<String> METRICS =
      Set.of(
          SemanticFieldNames.IN_BYTES,
          SemanticFieldNames.OUT_BYTES,
          SemanticFieldNames.TOTAL_BYTES,
          SemanticFieldNames.IN_PACKETS,
          SemanticFieldNames.OUT_PACKETS,
          SemanticFieldNames.TOTAL_PACKETS);

  private static final List<String> COLUMNS =
      List.of(
          "ip",
          "inboundBytes",
          "outboundBytes",
          "totalBytes",
          "inboundPackets",
          "outboundPackets",
          "totalPackets");

  private final RestApiClient restApiClient;

  public TopBandwidthIpsTool(RestApiClient restApiClient) {

    this.restApiClient = restApiClient;
  }

  @Override
  public String name() {
    return "top-bandwidth-ips";
  }

  @Override
  public boolean supports(QueryRequirements requirements) {

    return supportsRequirements(requirements);
  }

  @Override
  public AnalyticsResult execute(QueryRequirements requirements) {

    requireSupported(requirements);

    QueryRequirements.TimeRange timeRange = requirements.timeRange();

    try {
      RestApiResponse<List<BandwidthRow>> response =
          restApiClient.getTopBandwidthIps(
              timeRange.start(), timeRange.end(), requirements.limit());

      return normalize(response);

    } catch (RestApiClientException exception) {
      if (exception.kind() == RestApiClientException.Kind.NO_DATA) {

        return AnalyticsResult.emptyPostgresTool(COLUMNS);
      }

      throw ToolExecutionException.from(exception);
    }
  }

  static boolean supportsRequirements(QueryRequirements requirements) {

    if (requirements == null
        || !RestToolCapabilities.supportsTimeRange(requirements.timeRange())
        || requirements.distinct()
        || requirements.granularity() != null
        || !requirements.filters().isEmpty()
        || !requirements.ipRequirements().isEmpty()) {

      return false;
    }

    if (!requirements.dimensions().equals(List.of(SemanticFieldNames.IP_ADDRESS))) {

      return false;
    }

    if (!requirements.groupBy().equals(List.of(SemanticFieldNames.IP_ADDRESS))) {

      return false;
    }

    if (requirements.metrics().isEmpty()
        || requirements.metrics().stream()
            .anyMatch(
                metric ->
                    !METRICS.contains(metric.field())
                        || metric.aggregation() != QueryRequirements.Aggregation.SUM)) {

      return false;
    }

    if (requirements.limit() == null || requirements.limit() < 1 || requirements.limit() > 100) {

      return false;
    }

    if (requirements.trafficDirection() != QueryRequirements.TrafficDirection.UNSPECIFIED
        && requirements.trafficDirection() != QueryRequirements.TrafficDirection.BOTH) {

      return false;
    }

    if (requirements.sort().size() != 1) {
      return false;
    }

    QueryRequirements.SortRequirement sort = requirements.sort().get(0);

    return SemanticFieldNames.TOTAL_BYTES.equals(sort.field())
        && sort.aggregation() == QueryRequirements.Aggregation.SUM
        && sort.direction() == QueryRequirements.SortDirection.DESC;
  }

  static AnalyticsResult normalize(RestApiResponse<List<BandwidthRow>> response) {

    List<Map<String, Object>> rows =
        response.data().stream().map(TopBandwidthIpsTool::toRow).toList();

    return AnalyticsResult.postgresTool(COLUMNS, rows, Map.of());
  }

  private static Map<String, Object> toRow(BandwidthRow row) {

    return ToolResultRows.fromColumns(
        COLUMNS,
        row.ip(),
        row.inboundBytes(),
        row.outboundBytes(),
        row.totalBytes(),
        row.inboundPackets(),
        row.outboundPackets(),
        row.totalPackets());
  }
}
