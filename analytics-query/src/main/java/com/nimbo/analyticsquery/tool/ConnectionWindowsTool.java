package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.ConnectionSortField;
import com.nimbo.analyticsquery.client.restapi.RestApiClient;
import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.client.restapi.RestApiPageRequest;
import com.nimbo.analyticsquery.client.restapi.RestApiPagination;
import com.nimbo.analyticsquery.client.restapi.RestApiResponse;
import com.nimbo.analyticsquery.client.restapi.dto.ConnectionRow;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class ConnectionWindowsTool implements AnalyticsTool {

    private static final int DEFAULT_PAGE_SIZE = 100;

    private static final Set<String> DIMENSIONS =
            Set.of(
                    SemanticFieldNames.SOURCE_IP,
                    SemanticFieldNames.DESTINATION_IP);

    private static final Set<String> METRICS =
            Set.of(
                    SemanticFieldNames.IN_BYTES,
                    SemanticFieldNames.OUT_BYTES,
                    SemanticFieldNames.TOTAL_BYTES,
                    SemanticFieldNames.IN_PACKETS,
                    SemanticFieldNames.OUT_PACKETS,
                    SemanticFieldNames.TOTAL_PACKETS);

    private static final Set<String> SORTABLE_FIELDS =
            Set.of(
                    SemanticFieldNames.SOURCE_IP,
                    SemanticFieldNames.DESTINATION_IP,
                    SemanticFieldNames.IN_BYTES,
                    SemanticFieldNames.OUT_BYTES,
                    SemanticFieldNames.IN_PACKETS,
                    SemanticFieldNames.OUT_PACKETS);

    private static final List<String> COLUMNS =
            List.of(
                    "id",
                    "srcIp",
                    "dstIp",
                    "windowStart",
                    "windowEnd",
                    "inBytes",
                    "outBytes",
                    "totalBytes",
                    "inPackets",
                    "outPackets",
                    "totalPackets",
                    "batchId",
                    "ingestedAt");

    private final RestApiClient restApiClient;

    public ConnectionWindowsTool(
            RestApiClient restApiClient) {

        this.restApiClient = restApiClient;
    }

    @Override
    public String name() {
        return "connection-windows";
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
            RestApiResponse<List<ConnectionRow>> response = executeRequest(requirements, timeRange);

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
                || requirements.granularity() != null || !requirements.filters().isEmpty()
                || !requirements.groupBy().isEmpty()) {

            return false;
        }

        if (!Set.copyOf(requirements.dimensions()).equals(DIMENSIONS)) {

            return false;
        }

        return supportsMetrics(requirements) && supportsIpRequirements(requirements)
                && supportsSort(requirements) && supportsLimit(requirements.limit())
                && supportsTrafficDirection(requirements);
    }

    static AnalyticsResult normalize(RestApiResponse<List<ConnectionRow>> response) {

        List<Map<String, Object>> rows =
                response.data().stream()
                        .map(ConnectionWindowsTool::toRow)
                        .toList();

        return AnalyticsResult.postgresTool(COLUMNS, rows, metadata(response));
    }

    private RestApiResponse<List<ConnectionRow>> executeRequest(
            QueryRequirements requirements,
            QueryRequirements.TimeRange timeRange) {

        RestApiPageRequest<ConnectionSortField> pageRequest = pageRequest(requirements);

        QueryRequirements.IpRequirement either =
                findIpRequirement(
                        requirements,
                        QueryRequirements.IpRole.EITHER);

        if (either != null) {
            return restApiClient
                    .getConnectionWindowsForIp(
                            either.values().get(0),
                            timeRange.start(),
                            timeRange.end(),
                            pageRequest);
        }

        QueryRequirements.IpRequirement source =
                findIpRequirement(
                        requirements,
                        QueryRequirements.IpRole.SOURCE);

        QueryRequirements.IpRequirement destination =
                findIpRequirement(
                        requirements,
                        QueryRequirements.IpRole.DESTINATION);

        return restApiClient.getConnectionWindows(
                source == null
                        ? null
                        : source.values().get(0),
                destination == null
                        ? null
                        : destination.values().get(0),
                timeRange.start(),
                timeRange.end(),
                pageRequest);
    }

    private static boolean supportsMetrics(
            QueryRequirements requirements) {

        return requirements.metrics().stream()
                .allMatch(metric ->
                        METRICS.contains(metric.field())
                                && metric.aggregation()
                                == QueryRequirements.Aggregation.NONE);
    }

    private static boolean supportsIpRequirements(
            QueryRequirements requirements) {

        if (requirements.ipRequirements().isEmpty()) {
            return true;
        }

        if (requirements.ipRequirements().stream()
                .anyMatch(requirement ->
                        requirement.matchType()
                                != QueryRequirements.IpMatchType.EXACT
                                || requirement.values().size() != 1)) {

            return false;
        }

        long eitherCount =
                requirements.ipRequirements().stream()
                        .filter(requirement ->
                                requirement.role()
                                        == QueryRequirements.IpRole.EITHER)
                        .count();

        if (eitherCount > 0) {
            return requirements.ipRequirements().size() == 1;
        }

        long sourceCount =
                requirements.ipRequirements().stream()
                        .filter(requirement ->
                                requirement.role()
                                        == QueryRequirements.IpRole.SOURCE)
                        .count();

        long destinationCount =
                requirements.ipRequirements().stream()
                        .filter(requirement ->
                                requirement.role()
                                        == QueryRequirements.IpRole.DESTINATION)
                        .count();

        return sourceCount <= 1
                && destinationCount <= 1
                && sourceCount + destinationCount
                == requirements.ipRequirements().size();
    }

    private static boolean supportsSort(
            QueryRequirements requirements) {

        if (requirements.sort().isEmpty()) {
            return true;
        }

        if (requirements.sort().size() != 1) {
            return false;
        }

        QueryRequirements.SortRequirement sort =
                requirements.sort().get(0);

        if (!SORTABLE_FIELDS.contains(sort.field())) {
            return false;
        }

        if (SemanticFieldNames.SOURCE_IP.equals(sort.field())
                || SemanticFieldNames.DESTINATION_IP.equals(sort.field())) {

            return sort.aggregation() == null;
        }

        return sort.aggregation()
                == QueryRequirements.Aggregation.NONE;
    }

    private static boolean supportsLimit(Integer limit) {
        return limit == null
                || limit >= 1 && limit <= 100;
    }

    private static boolean supportsTrafficDirection(
            QueryRequirements requirements) {

        return switch (requirements.trafficDirection()) {
            case UNSPECIFIED, BOTH -> true;

            case INBOUND -> requirements.metrics()
                    .stream()
                    .allMatch(metric ->
                            SemanticFieldNames.INBOUND_METRICS.contains(metric.field()));

            case OUTBOUND -> requirements.metrics()
                    .stream()
                    .allMatch(metric ->
                            SemanticFieldNames.OUTBOUND_METRICS.contains(metric.field()));
        };
    }

    private static QueryRequirements.IpRequirement
    findIpRequirement(
            QueryRequirements requirements,
            QueryRequirements.IpRole role) {

        return requirements.ipRequirements()
                .stream()
                .filter(requirement ->
                        requirement.role() == role)
                .findFirst()
                .orElse(null);
    }

    private RestApiPageRequest<ConnectionSortField>
    pageRequest(QueryRequirements requirements) {

        int size = requirements.limit() != null
                ? requirements.limit()
                : DEFAULT_PAGE_SIZE;

        if (requirements.sort().isEmpty()) {
            return new RestApiPageRequest<>(
                    0,
                    size,
                    ConnectionSortField.WINDOW_START,
                    RestApiPageRequest.SortDirection.DESC);
        }

        QueryRequirements.SortRequirement sort =
                requirements.sort().get(0);

        return new RestApiPageRequest<>(
                0,
                size,
                toConnectionSortField(sort.field()),
                RestApiPageRequest.SortDirection.valueOf(
                        sort.direction().name()));
    }

    private ConnectionSortField toConnectionSortField(
            String semanticField) {

        return switch (semanticField) {
            case SemanticFieldNames.SOURCE_IP ->
                    ConnectionSortField.SOURCE_IP;

            case SemanticFieldNames.DESTINATION_IP ->
                    ConnectionSortField.DESTINATION_IP;

            case SemanticFieldNames.IN_BYTES ->
                    ConnectionSortField.IN_BYTES;

            case SemanticFieldNames.OUT_BYTES ->
                    ConnectionSortField.OUT_BYTES;

            case SemanticFieldNames.IN_PACKETS ->
                    ConnectionSortField.IN_PACKETS;

            case SemanticFieldNames.OUT_PACKETS ->
                    ConnectionSortField.OUT_PACKETS;

            default -> throw new IllegalArgumentException(
                    "Unsupported connection sort field: "
                            + semanticField);
        };
    }

    private static Map<String, Object> toRow(
            ConnectionRow row) {

        return ToolResultRows.fromColumns(
                COLUMNS,
                row.id(),
                row.srcIp(),
                row.dstIp(),
                row.windowStart(),
                row.windowEnd(),
                row.inBytes(),
                row.outBytes(),
                row.totalBytes(),
                row.inPackets(),
                row.outPackets(),
                row.totalPackets(),
                row.batchId(),
                row.ingestedAt());
    }

    private static Map<String, Object> metadata(
            RestApiResponse<?> response) {

        RestApiPagination pagination =
                response.pagination();

        if (pagination == null) {
            return Map.of();
        }

        Map<String, Object> metadata =
                new LinkedHashMap<>();

        metadata.put("totalElements", pagination.totalElements());
        metadata.put("totalPages", pagination.totalPages());
        metadata.put("page", pagination.page());
        metadata.put("size", pagination.size());
        metadata.put("first", pagination.first());
        metadata.put("last", pagination.last());

        return metadata;
    }
}
