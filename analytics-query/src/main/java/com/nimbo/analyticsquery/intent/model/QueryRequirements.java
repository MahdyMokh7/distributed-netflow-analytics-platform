package com.nimbo.analyticsquery.intent.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.UniqueElements;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public record QueryRequirements(

        @NotNull
        @Size(max = 20)
        @UniqueElements(message = "contains duplicate fields")
        List<@NotBlank @Size(max = 64) String> dimensions,

        @NotNull
        @Size(max = 20)
        @UniqueElements(message = "Duplicate metric requirement")
        List<@NotNull @Valid MetricRequirement> metrics,

        @NotNull
        @Size(max = 20)
        List<@NotNull @Valid FilterRequirement> filters,

        @NotNull
        @Size(max = 4)
        List<@NotNull @Valid IpRequirement> ipRequirements,

        @NotNull
        TrafficDirection trafficDirection,

        @Valid
        TimeRange timeRange,

        @NotNull
        @Size(max = 10)
        @UniqueElements(message = "contains duplicate fields")
        List<@NotBlank @Size(max = 64) String> groupBy,

        Duration granularity,

        @NotNull
        @Size(max = 10)
        List<@NotNull @Valid SortRequirement> sort,

        @Positive
        @Max(1000)
        Integer limit,

        boolean distinct) {

    public record MetricRequirement(

            @NotBlank
            @Size(max = 64)
            String field,

            @NotNull
            Aggregation aggregation) {
    }

    public record FilterRequirement(

            @NotBlank
            @Size(max = 64)
            String field,

            @NotNull
            FilterOperator operator,

            @NotNull
            @Size(min = 1, max = 50)
            List<@NotBlank @Size(max = 256) String> values) {
    }

    public record IpRequirement(

            @NotNull
            IpRole role,

            @NotNull
            IpMatchType matchType,

            @NotNull
            @Size(min = 1, max = 20)
            List<@NotBlank @Size(max = 64) String> values) {
    }

    public record TimeRange(

            @NotNull
            Instant start,

            @NotNull
            Instant end) {
    }

    public record SortRequirement(

            @NotBlank
            @Size(max = 64)
            String field,

            Aggregation aggregation,

            @NotNull
            SortDirection direction) {
    }

    public enum Aggregation {
        NONE,
        SUM,
        AVG,
        MIN,
        MAX,
        COUNT,
        COUNT_DISTINCT
    }

    public enum FilterOperator {
        EQ,
        NOT_EQ,
        GT,
        GTE,
        LT,
        LTE,
        IN,
        NOT_IN,
        BETWEEN
    }

    public enum IpRole {
        SOURCE,
        DESTINATION,
        EITHER
    }

    public enum IpMatchType {
        EXACT,
        CIDR
    }

    public enum TrafficDirection {
        INBOUND,
        OUTBOUND,
        BOTH,
        UNSPECIFIED
    }

    public enum SortDirection {
        ASC,
        DESC
    }

    public static QueryRequirements empty() {
        return new QueryRequirements(
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                TrafficDirection.UNSPECIFIED,
                null,
                List.of(),
                null,
                List.of(),
                null,
                false);
    }

    public boolean isEmpty() {
        return dimensions.isEmpty()
                && metrics.isEmpty()
                && filters.isEmpty()
                && ipRequirements.isEmpty()
                && trafficDirection == TrafficDirection.UNSPECIFIED
                && timeRange == null
                && groupBy.isEmpty()
                && granularity == null
                && sort.isEmpty()
                && limit == null
                && !distinct;
    }
}
