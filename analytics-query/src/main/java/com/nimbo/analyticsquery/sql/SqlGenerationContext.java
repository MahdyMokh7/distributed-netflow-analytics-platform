package com.nimbo.analyticsquery.sql;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;

public record SqlGenerationContext(
        AnalysisDataSource dataSource,
        List<Table> tables,
        List<String> queryRestrictions) {

    public SqlGenerationContext {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        tables = List.copyOf(Objects.requireNonNull(tables, "tables must not be null"));
        queryRestrictions =
                List.copyOf(
                        Objects.requireNonNull(
                                queryRestrictions, "queryRestrictions must not be null"));

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
                    List.copyOf(
                            Objects.requireNonNull(
                                    relationships, "relationships must not be null"));

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("table columns must not be empty");
            }
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
