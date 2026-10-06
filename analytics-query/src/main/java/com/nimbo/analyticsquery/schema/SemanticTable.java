package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record SemanticTable(
    String name,
    AnalysisDataSource dataSource,
    String meaning,
    String grain,
    String timestampColumn,
    Duration timeResolution,
    List<SemanticColumn> columns,
    List<String> relationships,
    List<String> supportedAnalyses,
    List<String> unsupportedAnalyses) {

  public SemanticTable {
    requireText(name, "name");
    Objects.requireNonNull(dataSource, "dataSource must not be null");
    requireText(meaning, "meaning");
    requireText(grain, "grain");
    requireText(timestampColumn, "timestampColumn");

    if (timeResolution != null && (timeResolution.isZero() || timeResolution.isNegative())) {
      throw new IllegalArgumentException("timeResolution must be positive");
    }

    columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));

    relationships =
        List.copyOf(Objects.requireNonNull(relationships, "relationships must not be null"));

    supportedAnalyses =
        List.copyOf(
            Objects.requireNonNull(supportedAnalyses, "supportedAnalyses must not be null"));

    unsupportedAnalyses =
        List.copyOf(
            Objects.requireNonNull(unsupportedAnalyses, "unsupportedAnalyses must not be null"));

    Set<String> names = new HashSet<>();

    for (SemanticColumn column : columns) {
      if (!names.add(column.name())) {
        throw new IllegalArgumentException("duplicate column: " + column.name());
      }
    }

    if (!names.contains(timestampColumn)) {
      throw new IllegalArgumentException("timestamp column is not present in table");
    }
  }

  public Optional<SemanticColumn> column(String name) {
    return columns.stream().filter(column -> column.name().equals(name)).findFirst();
  }

  public Set<String> dimensions() {
    return columns.stream()
        .filter(SemanticColumn::isDimension)
        .map(SemanticColumn::name)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  public Set<String> metrics() {
    return columns.stream()
        .filter(SemanticColumn::isMetric)
        .map(SemanticColumn::name)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  public Set<String> semanticFields() {
    Set<String> fields = new HashSet<>();

    columns.stream()
        .map(SemanticColumn::semanticField)
        .filter(Objects::nonNull)
        .forEach(fields::add);

    addDerivedSemanticFields(fields);

    return Set.copyOf(fields);
  }

  public boolean supportsSemanticField(String semanticField) {
    return semanticFields().contains(semanticField);
  }

  public Set<Aggregation> allowedAggregations(String semanticField) {

    Optional<SemanticColumn> direct =
        columns.stream().filter(column -> column.represents(semanticField)).findFirst();

    if (direct.isPresent()) {
      return direct.get().allowedAggregations();
    }

    if (SemanticFieldNames.TOTAL_BYTES.equals(semanticField)) {
      return sharedAggregations(SemanticFieldNames.IN_BYTES, SemanticFieldNames.OUT_BYTES);
    }

    if (SemanticFieldNames.TOTAL_PACKETS.equals(semanticField)) {
      return sharedAggregations(SemanticFieldNames.IN_PACKETS, SemanticFieldNames.OUT_PACKETS);
    }

    return Set.of();
  }

  private Set<Aggregation> sharedAggregations(String first, String second) {

    Set<Aggregation> shared = new HashSet<>(allowedAggregations(first));
    shared.retainAll(allowedAggregations(second));
    return Set.copyOf(shared);
  }

  private static void addDerivedSemanticFields(Set<String> fields) {

    if (fields.contains(SemanticFieldNames.IN_BYTES)
        && fields.contains(SemanticFieldNames.OUT_BYTES)) {

      fields.add(SemanticFieldNames.TOTAL_BYTES);
    }

    if (fields.contains(SemanticFieldNames.IN_PACKETS)
        && fields.contains(SemanticFieldNames.OUT_PACKETS)) {

      fields.add(SemanticFieldNames.TOTAL_PACKETS);
    }
  }

  private static void requireText(String value, String field) {

    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
