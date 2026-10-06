package com.nimbo.analyticsquery.routing;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticField;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.schema.SemanticSchemaCatalog;
import com.nimbo.analyticsquery.schema.SemanticTable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class DataSourceRouter {

  private static final List<AnalysisDataSource> ROUTING_ORDER =
      List.of(AnalysisDataSource.POSTGRESQL, AnalysisDataSource.STARROCKS);

  private final SemanticSchemaCatalog catalog;

  public DataSourceRouter(SemanticSchemaCatalog catalog) {
    this.catalog = catalog;
  }

  public Optional<DataSourceRoute> route(QueryRequirements requirements) {

    Objects.requireNonNull(requirements, "queryRequirements must not be null");

    for (AnalysisDataSource dataSource : ROUTING_ORDER) {
      Optional<SemanticTable> table = selectTable(catalog.tables(dataSource), requirements);

      if (table.isPresent()) {
        return Optional.of(new DataSourceRoute(dataSource, table.get()));
      }
    }

    return Optional.empty();
  }

  private Optional<SemanticTable> selectTable(
      Collection<SemanticTable> tables, QueryRequirements requirements) {

    Set<String> requestedDimensions = requestedDimensions(requirements);

    List<TableCandidate> candidates =
        tables.stream()
            .filter(table -> preservesSemantics(table, requirements))
            .map(
                table -> new TableCandidate(table, extraDimensionCount(table, requestedDimensions)))
            .sorted(
                Comparator.comparingInt(TableCandidate::extraDimensions)
                    .thenComparing(candidate -> candidate.table().name()))
            .toList();

    if (candidates.isEmpty()) {
      return Optional.empty();
    }

    TableCandidate best = candidates.get(0);

    if (candidates.size() > 1 && candidates.get(1).extraDimensions() == best.extraDimensions()) {
      return Optional.empty();
    }

    return Optional.of(best.table());
  }

  private boolean preservesSemantics(SemanticTable table, QueryRequirements requirements) {

    return supportsDimensions(table, requirements)
        && supportsMetrics(table, requirements)
        && supportsOtherFields(table, requirements)
        && supportsIpRequirements(table, requirements)
        && supportsTrafficDirection(requirements)
        && supportsTimeResolution(table, requirements);
  }

  private boolean supportsDimensions(SemanticTable table, QueryRequirements requirements) {

    Set<String> dimensions = new HashSet<>(requirements.dimensions());
    dimensions.addAll(requirements.groupBy());

    return dimensions.stream()
        .allMatch(field -> column(table, field).filter(SemanticColumn::isDimension).isPresent());
  }

  private boolean supportsMetrics(SemanticTable table, QueryRequirements requirements) {

    return requirements.metrics().stream()
        .allMatch(metric -> supportsAggregation(table, metric.field(), metric.aggregation()));
  }

  private boolean supportsOtherFields(SemanticTable table, QueryRequirements requirements) {

    boolean filtersSupported =
        requirements.filters().stream()
            .allMatch(filter -> table.supportsSemanticField(filter.field()));

    boolean sortingSupported =
        requirements.sort().stream()
            .allMatch(
                sort ->
                    sort.aggregation() == null
                        ? column(table, sort.field())
                            .filter(SemanticColumn::isDimension)
                            .isPresent()
                        : supportsAggregation(table, sort.field(), sort.aggregation()));

    return filtersSupported && sortingSupported;
  }

  private boolean supportsAggregation(SemanticTable table, String field, Aggregation aggregation) {

    return table.supportsSemanticField(field)
        && table.allowedAggregations(field).contains(aggregation);
  }

  private boolean supportsIpRequirements(SemanticTable table, QueryRequirements requirements) {

    Set<String> fields = table.semanticFields();

    return requirements.ipRequirements().stream()
        .allMatch(
            requirement -> switch (requirement.role()) {
              case SOURCE -> fields.contains(SemanticFieldNames.SOURCE_IP);
              case DESTINATION -> fields.contains(SemanticFieldNames.DESTINATION_IP);
              case EITHER ->
                  fields.contains(SemanticFieldNames.IP_ADDRESS)
                      || fields.contains(SemanticFieldNames.SOURCE_IP)
                          && fields.contains(SemanticFieldNames.DESTINATION_IP);
            });
  }

  private boolean supportsTrafficDirection(QueryRequirements requirements) {

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

  private boolean supportsTimeResolution(SemanticTable table, QueryRequirements requirements) {

    Duration resolution = table.timeResolution();

    if (resolution == null) {
      return true;
    }

    QueryRequirements.TimeRange timeRange = requirements.timeRange();

    if (timeRange == null
        || !isAligned(timeRange.start(), resolution)
        || !isAligned(timeRange.end(), resolution)) {
      return false;
    }

    Duration requested = requirements.granularity();

    return requested == null
        || requested.compareTo(resolution) >= 0
            && requested.toSeconds() % resolution.toSeconds() == 0;
  }

  private Optional<SemanticColumn> column(SemanticTable table, String semanticField) {

    return table.columns().stream().filter(column -> column.represents(semanticField)).findFirst();
  }

  private Set<String> requestedDimensions(QueryRequirements requirements) {

    Set<String> dimensions = new HashSet<>(requirements.dimensions());

    requirements.filters().stream()
        .map(QueryRequirements.FilterRequirement::field)
        .filter(
            field ->
                catalog
                    .find(field)
                    .filter(
                        semanticField ->
                            semanticField.usages().contains(SemanticField.Usage.DIMENSION))
                    .isPresent())
        .forEach(dimensions::add);

    requirements
        .ipRequirements()
        .forEach(
            requirement ->
                dimensions.add(
                    switch (requirement.role()) {
                      case SOURCE -> SemanticFieldNames.SOURCE_IP;
                      case DESTINATION -> SemanticFieldNames.DESTINATION_IP;
                      case EITHER -> SemanticFieldNames.IP_ADDRESS;
                    }));

    return dimensions;
  }

  private int extraDimensionCount(SemanticTable table, Set<String> requestedDimensions) {

    return (int)
        table.columns().stream()
            .filter(SemanticColumn::isDimension)
            .map(SemanticColumn::semanticField)
            .filter(Objects::nonNull)
            .filter(field -> !requestedDimensions.contains(field))
            .count();
  }

  private static boolean isAligned(Instant value, Duration resolution) {

    long resolutionSeconds = resolution.toSeconds();

    return resolutionSeconds > 0
        && value.getNano() == 0
        && Math.floorMod(value.getEpochSecond(), resolutionSeconds) == 0;
  }

  private record TableCandidate(SemanticTable table, int extraDimensions) {}
}
