package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;

import java.util.Objects;
import java.util.Set;

public record SemanticField(

        String name,
        String description,
        Set<Usage> usages,
        Set<Aggregation> allowedAggregations,
        boolean filterable,
        boolean groupable,
        boolean sortable) {

    public SemanticField {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(description, "description must not be null");
        usages = Set.copyOf(Objects.requireNonNull(usages, "usages must not be null"));
        allowedAggregations = Set.copyOf(Objects.requireNonNull(
                        allowedAggregations,
                        "allowedAggregations must not be null")
        );
    }

    public enum Usage {
        DIMENSION,
        METRIC
    }
}