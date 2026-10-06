package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.client.restapi.RestApiResponse;
import com.nimbo.analyticsquery.client.restapi.dto.ProtocolRow;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class ProtocolUsageWindowsTool implements AnalyticsTool {

  private static final Set<String> DIMENSIONS = Set.of(SemanticFieldNames.L7_PROTOCOL);

  private static final Set<String> METRICS =
      Set.of(
          SemanticFieldNames.IN_BYTES,
          SemanticFieldNames.OUT_BYTES,
          SemanticFieldNames.TOTAL_BYTES,
          SemanticFieldNames.IN_PACKETS,
          SemanticFieldNames.OUT_PACKETS,
          SemanticFieldNames.TOTAL_PACKETS,
          SemanticFieldNames.CONNECTION_COUNT);

  private static final List<String> COLUMNS =
      List.of(
          "l7Protocol",
          "windowStart",
          "windowEnd",
          "inBytes",
          "outBytes",
          "totalBytes",
          "inPackets",
          "outPackets",
          "totalPackets",
          "connectionCount",
          "batchId",
          "ingestedAt");

  private final RestApiClient restApiClient;

  public ProtocolUsageWindowsTool(RestApiClient restApiClient) {

    this.restApiClient = restApiClient;
  }

  @Override
  public String name() {
    return "protocol-usage-windows";
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
      RestApiResponse<List<ProtocolRow>> response =
          restApiClient.getProtocolUsageWindows(timeRange.start(), timeRange.end());

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
        || !requirements.ipRequirements().isEmpty()
        || !requirements.groupBy().isEmpty()
        || !requirements.sort().isEmpty()
        || requirements.limit() != null) {

      return false;
    }

    if (!Set.copyOf(requirements.dimensions()).equals(DIMENSIONS)) {

      return false;
    }

    if (requirements.metrics().stream()
        .anyMatch(
            metric ->
                !METRICS.contains(metric.field())
                    || metric.aggregation() != QueryRequirements.Aggregation.NONE)) {

      return false;
    }

    return supportsTrafficDirection(requirements);
  }

  static AnalyticsResult normalize(RestApiResponse<List<ProtocolRow>> response) {

    List<Map<String, Object>> rows =
        response.data().stream().map(ProtocolUsageWindowsTool::toRow).toList();

    return AnalyticsResult.postgresTool(COLUMNS, rows, Map.of());
  }

  private static boolean supportsTrafficDirection(QueryRequirements requirements) {

    return switch (requirements.trafficDirection()) {
      case UNSPECIFIED, BOTH -> true;

      case INBOUND ->
          requirements.metrics().stream()
              .allMatch(metric -> SemanticFieldNames.INBOUND_METRICS.contains(metric.field()));

      case OUTBOUND ->
          requirements.metrics().stream()
              .allMatch(metric -> SemanticFieldNames.OUTBOUND_METRICS.contains(metric.field()));
    };
  }

  private static Map<String, Object> toRow(ProtocolRow row) {

    return ToolResultRows.fromColumns(
        COLUMNS,
        row.l7Protocol(),
        row.windowStart(),
        row.windowEnd(),
        row.inBytes(),
        row.outBytes(),
        row.totalBytes(),
        row.inPackets(),
        row.outPackets(),
        row.totalPackets(),
        row.connectionCount(),
        row.batchId(),
        row.ingestedAt());
  }
}
