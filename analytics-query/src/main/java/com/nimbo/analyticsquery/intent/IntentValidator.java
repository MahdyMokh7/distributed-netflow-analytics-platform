package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.config.IntentProperties;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.intent.model.ClarificationRequirement;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.schema.SemanticField;
import com.nimbo.analyticsquery.schema.SemanticSchemaCatalog;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class IntentValidator {

    private static final Set<String> IP_FIELDS = Set.of(
            "SOURCE_IP",
            "DESTINATION_IP",
            "IP_ADDRESS");

    private static final Set<AnalysisStatus> ALLOWED_STATUSES = Set.of(
            AnalysisStatus.SUCCESS,
            AnalysisStatus.NEEDS_CLARIFICATION,
            AnalysisStatus.UNSUPPORTED_ANALYSIS,
            AnalysisStatus.OUT_OF_SCOPE,
            AnalysisStatus.REJECTED);

    private static final Set<AnalysisStatus> TERMINAL_STATUSES = Set.of(
            AnalysisStatus.UNSUPPORTED_ANALYSIS,
            AnalysisStatus.OUT_OF_SCOPE,
            AnalysisStatus.REJECTED);

    private final Validator beanValidator;
    private final SemanticSchemaCatalog catalog;
    private final Clock clock;
    private final Duration maxTimeRange;

    public IntentValidator(
            Validator beanValidator,
            SemanticSchemaCatalog catalog,
            Clock clock,
            IntentProperties properties) {

        this.beanValidator = beanValidator;
        this.catalog = catalog;
        this.clock = clock;
        this.maxTimeRange = properties.maxTimeRange();
    }

    public AnalysisIntent validate(AnalysisIntent intent) {
        if (intent == null) {
            throw new InvalidIntentException("LLM returned no intent");
        }

        validateBeanConstraints(intent);
        validateStatus(intent.status());

        if (TERMINAL_STATUSES.contains(intent.status())) {
            validateTerminalIntent(intent);
            return intent;
        }

        validateRequirements(intent.queryRequirements());

        if (intent.status() == AnalysisStatus.SUCCESS) {
            validateSuccessfulIntent(intent);
        } else {
            validateClarificationIntent(intent);
        }

        return intent;
    }

    private void validateBeanConstraints(AnalysisIntent intent) {
        Set<ConstraintViolation<AnalysisIntent>> violations =
                beanValidator.validate(intent);

        if (violations.isEmpty()) {
            return;
        }

        String details = violations.stream()
                .map(violation ->
                        violation.getPropertyPath()
                                + " "
                                + violation.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));

        throw new InvalidIntentException(
                "LLM intent violates the structured contract: "
                        + details);
    }

    private void validateStatus(AnalysisStatus status) {
        if (!ALLOWED_STATUSES.contains(status)) {
            throw new InvalidIntentException(
                    "Status is not valid during intent interpretation: "
                            + status);
        }
    }

    private void validateRequirements(QueryRequirements requirements) {

        validateDimensions(requirements);
        validateMetrics(requirements);
        validateFilters(requirements);
        validateIpRequirements(requirements);
        validateTimeRange(requirements.timeRange());
        validateGranularity(requirements.granularity());
        validateGrouping(requirements);
        validateSorting(requirements);
    }

    private void validateDimensions(
            QueryRequirements requirements) {

        for (String dimension : requirements.dimensions()) {
            SemanticField field = requireField(dimension);

            if (!field.usages()
                    .contains(SemanticField.Usage.DIMENSION)) {

                throw new InvalidIntentException(
                        dimension
                                + " is not an approved dimension");
            }
        }
    }

    private void validateMetrics(
            QueryRequirements requirements) {

        Set<String> seen = new HashSet<>();

        for (QueryRequirements.MetricRequirement metric
                : requirements.metrics()) {

            SemanticField field = requireField(metric.field());

            if (!field.usages()
                    .contains(SemanticField.Usage.METRIC)) {

                throw new InvalidIntentException(
                        metric.field()
                                + " is not an approved metric");
            }

            if (!field.allowedAggregations()
                    .contains(metric.aggregation())) {

                throw new InvalidIntentException(
                        "Aggregation "
                                + metric.aggregation()
                                + " is not allowed for "
                                + metric.field());
            }

            String key =
                    metric.field() + ":" + metric.aggregation();

            if (!seen.add(key)) {
                throw new InvalidIntentException(
                        "Duplicate metric requirement: " + key);
            }
        }

        validateAggregationConsistency(requirements);
    }

    private void validateAggregationConsistency(
            QueryRequirements requirements) {

        boolean hasAggregatedMetric = requirements.metrics()
                .stream()
                .anyMatch(metric ->
                        metric.aggregation()
                                != QueryRequirements.Aggregation.NONE);

        boolean hasRawMetric = requirements.metrics()
                .stream()
                .anyMatch(metric ->
                        metric.aggregation()
                                == QueryRequirements.Aggregation.NONE);

        if (hasAggregatedMetric && hasRawMetric) {
            throw new InvalidIntentException(
                    "Aggregated and non-aggregated metrics cannot be mixed");
        }

        if (!hasAggregatedMetric) {
            return;
        }

        Set<String> groupedFields =
                Set.copyOf(requirements.groupBy());

        for (String dimension : requirements.dimensions()) {
            if (!groupedFields.contains(dimension)) {
                throw new InvalidIntentException(
                        "Aggregated queries must group by every requested dimension: "
                                + dimension);
            }
        }
    }

    private void validateFilters(
            QueryRequirements requirements) {

        for (QueryRequirements.FilterRequirement filter
                : requirements.filters()) {

            if (IP_FIELDS.contains(filter.field())) {

                throw new InvalidIntentException(
                        "IP filters must use ipRequirements");
            }

            SemanticField field = requireField(filter.field());

            if (!field.filterable()) {
                throw new InvalidIntentException(
                        filter.field()
                                + " is not an approved filter");
            }

            validateFilterValues(filter);
        }
    }

    private void validateFilterValues(
            QueryRequirements.FilterRequirement filter) {

        int valueCount = filter.values().size();

        switch (filter.operator()) {
            case BETWEEN -> {
                if (valueCount != 2) {
                    throw new InvalidIntentException(
                            "BETWEEN requires exactly two values");
                }
            }

            case IN, NOT_IN -> {
                if (valueCount < 1) {
                    throw new InvalidIntentException(
                            filter.operator()
                                    + " requires at least one value");
                }
            }

            default -> {
                if (valueCount != 1) {
                    throw new InvalidIntentException(
                            filter.operator()
                                    + " requires exactly one value");
                }
            }
        }
    }

    private void validateIpRequirements(
            QueryRequirements requirements) {

        Set<String> seen = new HashSet<>();

        for (QueryRequirements.IpRequirement ipRequirement
                : requirements.ipRequirements()) {

            for (String value : ipRequirement.values()) {
                String key = ipRequirement.role()
                        + ":"
                        + ipRequirement.matchType()
                        + ":"
                        + value;

                if (!seen.add(key)) {
                    throw new InvalidIntentException(
                            "Duplicate IP requirement: " + value);
                }

                switch (ipRequirement.matchType()) {
                    case EXACT -> validateIpv4(value);
                    case CIDR -> validateIpv4Cidr(value);
                }
            }
        }
    }

    private void validateTimeRange(
            QueryRequirements.TimeRange timeRange) {

        if (timeRange == null) {
            return;
        }

        if (!timeRange.start().isBefore(timeRange.end())) {
            throw new InvalidIntentException(
                    "Time range start must be before end");
        }

        if (timeRange.end().isAfter(clock.instant())) {
            throw new InvalidIntentException(
                    "Time range end must not be in the future");
        }

        if (Duration.between(timeRange.start(), timeRange.end())
                .compareTo(maxTimeRange) > 0) {
            throw new InvalidIntentException(
                    "Time range must not exceed " + maxTimeRange);
        }
    }

    private void validateGranularity(Duration granularity) {
        if (granularity == null) {
            return;
        }

        if (granularity.isZero() || granularity.isNegative()) {
            throw new InvalidIntentException(
                    "Granularity must be positive");
        }
    }

    private void validateGrouping(
            QueryRequirements requirements) {

        Set<String> dimensions =
                Set.copyOf(requirements.dimensions());

        for (String groupField : requirements.groupBy()) {
            SemanticField field = requireField(groupField);

            if (!field.groupable()) {
                throw new InvalidIntentException(
                        groupField
                                + " cannot be used for grouping");
            }

            if (!dimensions.contains(groupField)) {
                throw new InvalidIntentException(
                        "Grouped field must also be requested as a dimension: "
                                + groupField);
            }
        }
    }

    private void validateSorting(
            QueryRequirements requirements) {

        Set<String> seen = new HashSet<>();

        for (QueryRequirements.SortRequirement sort
                : requirements.sort()) {

            String key =
                    sort.field() + ":" + sort.aggregation();

            if (!seen.add(key)) {
                throw new InvalidIntentException(
                        "Duplicate sort requirement: " + sort.field());
            }

            SemanticField field = requireField(sort.field());

            if (!field.sortable()) {
                throw new InvalidIntentException(
                        sort.field()
                                + " cannot be used for sorting");
            }

            if (sort.aggregation() == null) {
                validateDimensionSort(requirements, sort);
            } else {
                validateMetricSort(requirements, sort);
            }
        }
    }

    private void validateDimensionSort(
            QueryRequirements requirements,
            QueryRequirements.SortRequirement sort) {

        if (!requirements.dimensions()
                .contains(sort.field())) {

            throw new InvalidIntentException(
                    "Non-aggregated sort field must be a requested dimension: "
                            + sort.field());
        }
    }

    private void validateMetricSort(
            QueryRequirements requirements,
            QueryRequirements.SortRequirement sort) {

        boolean matchingMetric = requirements.metrics()
                .stream()
                .anyMatch(metric ->
                        metric.field().equals(sort.field())
                                && metric.aggregation()
                                == sort.aggregation());

        if (!matchingMetric) {
            throw new InvalidIntentException(
                    "Aggregated sort must reference a requested metric");
        }
    }

    private void validateSuccessfulIntent(
            AnalysisIntent intent) {

        QueryRequirements requirements =
                intent.queryRequirements();

        if (requirements.timeRange() == null) {
            throw new InvalidIntentException(
                    "Successful intent must contain a bounded time range");
        }

        if (requirements.dimensions().isEmpty()
                && requirements.metrics().isEmpty()) {

            throw new InvalidIntentException(
                    "Successful intent must request at least one dimension or metric");
        }

        if (!intent.clarification().isEmpty()) {
            throw new InvalidIntentException(
                    "Successful intent cannot require clarification");
        }
    }

    private void validateClarificationIntent(
            AnalysisIntent intent) {

        ClarificationRequirement clarification =
                intent.clarification();

        if (!clarification.required()) {
            throw new InvalidIntentException(
                    "NEEDS_CLARIFICATION must require clarification");
        }

        if (clarification.missingFields().isEmpty()) {
            throw new InvalidIntentException(
                    "Clarification must identify missing information");
        }

        if (clarification.question() == null
                || clarification.question().isBlank()) {

            throw new InvalidIntentException(
                    "Clarification must contain a question");
        }
    }

    private void validateTerminalIntent(
            AnalysisIntent intent) {

        if (!intent.queryRequirements().isEmpty()) {
            throw new InvalidIntentException(
                    intent.status()
                            + " intent must not contain query requirements");
        }

        if (!intent.clarification().isEmpty()) {
            throw new InvalidIntentException(
                    intent.status()
                            + " intent must not contain clarification requirements");
        }
    }

    private SemanticField requireField(String name) {
        return catalog.find(name)
                .orElseThrow(() ->
                        new InvalidIntentException(
                                "Unknown or unapproved semantic field: "
                                        + name));
    }

    private void validateUniqueFields(
            List<String> fields,
            String fieldName) {

        if (new HashSet<>(fields).size() != fields.size()) {
            throw new InvalidIntentException(
                    fieldName + " contains duplicate fields");
        }
    }

    private void validateIpv4(String value) {
        if (!isValidIpv4(value)) {
            throw new InvalidIntentException(
                    "Invalid IPv4 address");
        }
    }

    private void validateIpv4Cidr(String value) {
        String[] parts = value.split("/", -1);

        if (parts.length != 2 || !isValidIpv4(parts[0])) {
            throw new InvalidIntentException(
                    "Invalid IPv4 CIDR");
        }

        try {
            int prefix = Integer.parseInt(parts[1]);

            if (prefix < 0 || prefix > 32) {
                throw new InvalidIntentException(
                        "Invalid IPv4 CIDR prefix");
            }

        } catch (NumberFormatException exception) {
            throw new InvalidIntentException(
                    "Invalid IPv4 CIDR prefix");
        }
    }

    private boolean isValidIpv4(String value) {
        String[] parts = value.split("\\.", -1);

        if (parts.length != 4) {
            return false;
        }

        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3) {
                return false;
            }

            for (int index = 0;
                 index < part.length();
                 index++) {

                if (!Character.isDigit(part.charAt(index))) {
                    return false;
                }
            }

            try {
                if (Integer.parseInt(part) > 255) {
                    return false;
                }
            } catch (NumberFormatException exception) {
                return false;
            }
        }

        return true;
    }
}
