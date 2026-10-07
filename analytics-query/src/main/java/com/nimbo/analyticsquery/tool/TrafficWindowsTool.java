package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.client.restapi.RestApiPageRequest;
import com.nimbo.analyticsquery.client.restapi.RestApiPagination;
import com.nimbo.analyticsquery.client.restapi.RestApiResponse;
import com.nimbo.analyticsquery.client.restapi.TrafficSortField;
import com.nimbo.analyticsquery.client.restapi.dto.TrafficRow;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class TrafficWindowsTool implements AnalyticsTool {

  private static final int DEFAULT_PAGE_SIZE = 100;

  private static final Set<String> DIMENSIONS = Set.of(SemanticFieldNames.SOURCE_IP);

  private static final Set<String> METRICS =
      Set.of(
          SemanticFieldNames.IN_BYTES,
          SemanticFieldNames.OUT_BYTES,
          SemanticFieldNames.TOTAL_BYTES,
          SemanticFieldNames.IN_PACKETS,
          SemanticFieldNames.OUT_PACKETS,
          SemanticFieldNames.TOTAL_PACKETS,
          SemanticFieldNames.RECORD_COUNT);

  private static final Set<String> SORTABLE_FIELDS =
      Set.of(
          SemanticFieldNames.SOURCE_IP,
          SemanticFieldNames.IN_BYTES,
          SemanticFieldNames.OUT_BYTES,
          SemanticFieldNames.IN_PACKETS,
          SemanticFieldNames.OUT_PACKETS,
          SemanticFieldNames.RECORD_COUNT);

  private static final List<String> COLUMNS =
      List.of(
          "srcIp",
          "windowStart",
          "windowEnd",
          "inBytes",
          "outBytes",
          "totalBytes",
          "inPackets",
          "outPackets",
          "totalPackets",
          "recordCount",
          "ingestedAt");

  private final RestApiClient restApiClient;

  public TrafficWindowsTool(RestApiClient restApiClient) {

    this.restApiClient = restApiClient;
  }

  @Override
  public String name() {
    return "traffic-windows";
  }

  @Override
  public boolean supports(QueryRequirements requirements) {

    return supportsRequirements(requirements);
  }

  @Override
  public AnalyticsResult execute(QueryRequirements requirements) {

    requireSupported(requirements);

    QueryRequirements.TimeRange timeRange = requirements.timeRange();

    String sourceIp = sourceIp(requirements);

    try {
      RestApiResponse<List<TrafficRow>> response =
          restApiClient.getTrafficWindows(
              sourceIp, timeRange.start(), timeRange.end(), pageRequest(requirements));

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
        || !requirements.groupBy().isEmpty()) {

      return false;
    }

    if (!Set.copyOf(requirements.dimensions()).equals(DIMENSIONS)) {

      return false;
    }

    if (!supportsMetrics(requirements)
        || !supportsIpRequirements(requirements)
        || !supportsSort(requirements)
        || !supportsLimit(requirements.limit())) {

      return false;
    }

    return supportsTrafficDirection(requirements);
  }

  static AnalyticsResult normalize(RestApiResponse<List<TrafficRow>> response) {

    List<Map<String, Object>> rows =
        response.data().stream().map(TrafficWindowsTool::toRow).toList();

    return AnalyticsResult.postgresTool(COLUMNS, rows, metadata(response));
  }

  private static boolean supportsMetrics(QueryRequirements requirements) {

    return requirements.metrics().stream()
        .allMatch(
            metric ->
                METRICS.contains(metric.field())
                    && metric.aggregation() == QueryRequirements.Aggregation.NONE);
  }

  private static boolean supportsIpRequirements(QueryRequirements requirements) {

    if (requirements.ipRequirements().isEmpty()) {
      return true;
    }

    if (requirements.ipRequirements().size() != 1) {
      return false;
    }

    QueryRequirements.IpRequirement requirement = requirements.ipRequirements().get(0);

    return requirement.role() == QueryRequirements.IpRole.SOURCE
        && requirement.matchType() == QueryRequirements.IpMatchType.EXACT
        && requirement.values().size() == 1;
  }

  private static boolean supportsSort(QueryRequirements requirements) {

    if (requirements.sort().isEmpty()) {
      return true;
    }

    if (requirements.sort().size() != 1) {
      return false;
    }

    QueryRequirements.SortRequirement sort = requirements.sort().get(0);

    if (!SORTABLE_FIELDS.contains(sort.field())) {
      return false;
    }

    if (SemanticFieldNames.SOURCE_IP.equals(sort.field())) {
      return sort.aggregation() == null;
    }

    return sort.aggregation() == QueryRequirements.Aggregation.NONE;
  }

  private static boolean supportsLimit(Integer limit) {
    return limit == null || limit >= 1 && limit <= 100;
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

  private String sourceIp(QueryRequirements requirements) {

    if (requirements.ipRequirements().isEmpty()) {
      return null;
    }

    return requirements.ipRequirements().get(0).values().get(0);
  }

  private RestApiPageRequest<TrafficSortField> pageRequest(QueryRequirements requirements) {

    int size = requirements.limit() != null ? requirements.limit() : DEFAULT_PAGE_SIZE;

    if (requirements.sort().isEmpty()) {
      return new RestApiPageRequest<>(
          0, size, TrafficSortField.WINDOW_START, RestApiPageRequest.SortDirection.DESC);
    }

    QueryRequirements.SortRequirement sort = requirements.sort().get(0);

    return new RestApiPageRequest<>(
        0,
        size,
        toTrafficSortField(sort.field()),
        RestApiPageRequest.SortDirection.valueOf(sort.direction().name()));
  }

  private TrafficSortField toTrafficSortField(String semanticField) {

    return switch (semanticField) {
      case SemanticFieldNames.SOURCE_IP -> TrafficSortField.SOURCE_IP;
      case SemanticFieldNames.IN_BYTES -> TrafficSortField.IN_BYTES;
      case SemanticFieldNames.OUT_BYTES -> TrafficSortField.OUT_BYTES;
      case SemanticFieldNames.IN_PACKETS -> TrafficSortField.IN_PACKETS;
      case SemanticFieldNames.OUT_PACKETS -> TrafficSortField.OUT_PACKETS;
      case SemanticFieldNames.RECORD_COUNT -> TrafficSortField.RECORD_COUNT;

      default ->
          throw new IllegalArgumentException("Unsupported traffic sort field: " + semanticField);
    };
  }

  private static Map<String, Object> toRow(TrafficRow row) {

    return ToolResultRows.fromColumns(
        COLUMNS,
        row.srcIp(),
        row.windowStart(),
        row.windowEnd(),
        row.inBytes(),
        row.outBytes(),
        row.totalBytes(),
        row.inPackets(),
        row.outPackets(),
        row.totalPackets(),
        row.recordCount(),
        row.ingestedAt());
  }

  private static Map<String, Object> metadata(RestApiResponse<?> response) {

    RestApiPagination pagination = response.pagination();

    if (pagination == null) {
      return Map.of();
    }

    Map<String, Object> metadata = new LinkedHashMap<>();

    metadata.put("totalElements", pagination.totalElements());
    metadata.put("totalPages", pagination.totalPages());
    metadata.put("page", pagination.page());
    metadata.put("size", pagination.size());
    metadata.put("first", pagination.first());
    metadata.put("last", pagination.last());

    return metadata;
  }
}
