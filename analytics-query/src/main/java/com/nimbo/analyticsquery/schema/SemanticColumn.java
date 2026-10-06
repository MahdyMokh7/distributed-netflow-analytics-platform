package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import java.util.Objects;
import java.util.Set;

public record SemanticColumn(
    String name,
    String sqlType,
    String meaning,
    Set<Role> roles,
    boolean nullable,
    String semanticField,
    Set<Aggregation> allowedAggregations) {

  public SemanticColumn {
    requireText(name, "name");
    requireText(sqlType, "sqlType");
    requireText(meaning, "meaning");

    roles = Set.copyOf(Objects.requireNonNull(roles, "roles must not be null"));

    if (roles.isEmpty()) {
      throw new IllegalArgumentException("column roles must not be empty");
    }

    if (semanticField != null && semanticField.isBlank()) {
      throw new IllegalArgumentException("semanticField must not be blank");
    }

    allowedAggregations =
        Set.copyOf(
            Objects.requireNonNull(allowedAggregations, "allowedAggregations must not be null"));

    if (semanticField == null && !allowedAggregations.isEmpty()) {
      throw new IllegalArgumentException("non-semantic columns cannot allow aggregations");
    }

    if (semanticField != null && allowedAggregations.isEmpty()) {
      throw new IllegalArgumentException("semantic columns must allow an aggregation");
    }
  }

  public SemanticColumn(
      String name, String sqlType, String meaning, Set<Role> roles, boolean nullable) {

    this(name, sqlType, meaning, roles, nullable, null, Set.of());
  }

  public boolean isDimension() {
    return roles.contains(Role.DIMENSION);
  }

  public boolean isMetric() {
    return roles.contains(Role.METRIC);
  }

  public boolean represents(String field) {
    return Objects.equals(semanticField, field);
  }

  private static void requireText(String value, String field) {

    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }

  public enum Role {
    DIMENSION,
    METRIC,
    TIME,
    IDENTIFIER,
    METADATA
  }
}
