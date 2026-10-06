package com.nimbo.analyticsquery.sql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public record SqlGenerationContext(
    AnalysisDataSource dataSource, List<Table> tables, List<String> queryRestrictions) {

  public SqlGenerationContext {
    Objects.requireNonNull(dataSource, "dataSource must not be null");
    tables = List.copyOf(Objects.requireNonNull(tables, "tables must not be null"));
    queryRestrictions =
        List.copyOf(
            Objects.requireNonNull(queryRestrictions, "queryRestrictions must not be null"));

    if (tables.isEmpty()) {
      throw new IllegalArgumentException("at least one table must be allowlisted");
    }

    if (queryRestrictions.isEmpty()) {
      throw new IllegalArgumentException("query restrictions must not be empty");
    }
  }

  public record Table(
      String name,
      String meaning,
      String grain,
      TimeSemantics timeSemantics,
      List<SemanticColumn> columns,
      List<String> relationships) {

    public Table {
      requireText(name, "name");
      requireText(meaning, "meaning");
      requireText(grain, "grain");
      Objects.requireNonNull(timeSemantics, "timeSemantics must not be null");
      columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
      relationships =
          List.copyOf(Objects.requireNonNull(relationships, "relationships must not be null"));

      if (columns.isEmpty()) {
        throw new IllegalArgumentException("table columns must not be empty");
      }
    }

    @JsonProperty("semanticExpressions")
    public List<SemanticExpression> semanticExpressions() {
      Map<String, SemanticColumn> byField =
          columns.stream()
              .filter(column -> column.semanticField() != null)
              .collect(
                  Collectors.toMap(
                      SemanticColumn::semanticField,
                      Function.identity(),
                      (first, ignored) -> first));

      List<SemanticExpression> expressions =
          byField.values().stream()
              .map(column -> new SemanticExpression(column.semanticField(), column.name()))
              .sorted(java.util.Comparator.comparing(SemanticExpression::semanticField))
              .collect(Collectors.toCollection(java.util.ArrayList::new));

      addDerived(
          expressions,
          byField,
          SemanticFieldNames.TOTAL_BYTES,
          SemanticFieldNames.IN_BYTES,
          SemanticFieldNames.OUT_BYTES);
      addDerived(
          expressions,
          byField,
          SemanticFieldNames.TOTAL_PACKETS,
          SemanticFieldNames.IN_PACKETS,
          SemanticFieldNames.OUT_PACKETS);

      return List.copyOf(expressions);
    }

    private static void addDerived(
        List<SemanticExpression> expressions,
        Map<String, SemanticColumn> byField,
        String derivedField,
        String firstField,
        String secondField) {

      SemanticColumn first = byField.get(firstField);
      SemanticColumn second = byField.get(secondField);

      if (first != null && second != null) {
        expressions.add(
            new SemanticExpression(derivedField, "(" + first.name() + " + " + second.name() + ")"));
      }
    }
  }

  public record SemanticExpression(String semanticField, String sqlExpression) {

    public SemanticExpression {
      requireText(semanticField, "semanticField");
      requireText(sqlExpression, "sqlExpression");
    }
  }

  public record TimeSemantics(String column, ZoneId timeZone, Duration resolution) {

    public TimeSemantics {
      requireText(column, "column");
      Objects.requireNonNull(timeZone, "timeZone must not be null");
    }
  }

  private static void requireText(String value, String field) {

    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
