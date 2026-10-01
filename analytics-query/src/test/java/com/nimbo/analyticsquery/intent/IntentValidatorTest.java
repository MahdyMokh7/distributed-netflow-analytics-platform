package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.config.IntentProperties;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.intent.model.ClarificationRequirement;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation.COUNT;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation.NONE;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation.SUM;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.FilterOperator.BETWEEN;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.FilterOperator.EQ;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.FilterOperator.IN;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.IpMatchType.CIDR;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.IpMatchType.EXACT;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.IpRole.SOURCE;
import static com.nimbo.analyticsquery.intent.model.QueryRequirements.SortDirection.DESC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntentValidatorTest {

    private static final Instant START =
            Instant.parse("2026-10-01T08:00:00Z");

    private static final Instant END =
            Instant.parse("2026-10-01T10:00:00Z");

    private static final Instant CURRENT_TIME =
            Instant.parse("2026-10-10T00:00:00Z");

    private static ValidatorFactory validatorFactory;
    private static IntentValidator validator;

    @BeforeAll
    static void setUpValidator() {
        validatorFactory =
                Validation.buildDefaultValidatorFactory();

        validator = new IntentValidator(
                validatorFactory.getValidator(),
                new CuratedSemanticSchemaCatalog(),
                Clock.fixed(CURRENT_TIME, ZoneOffset.UTC),
                new IntentProperties(Duration.ofDays(7)));
    }

    @AfterAll
    static void closeValidatorFactory() {
        validatorFactory.close();
    }

    @Test
    void givenValidSuccessfulIntentWhenValidatedThenSameIntentIsReturned() {
        // Given: a valid aggregated traffic intent
        AnalysisIntent intent = success(new RequirementsBuilder().build());

        // When: the structured intent is validated
        AnalysisIntent result = validator.validate(intent);

        // Then: the valid intent passes unchanged
        assertThat(result).isSameAs(intent);
    }

    @Test
    void givenNullIntentWhenValidatedThenIntentIsRejected() {
        // Given: no parsed intent
        AnalysisIntent intent = null;

        // When: the missing intent is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: validation reports that no intent was returned
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("LLM returned no intent");
    }

    @Test
    void givenExecutionPhaseStatusWhenValidatedThenIntentIsRejected() {
        // Given: an execution-only status returned during interpretation
        AnalysisIntent intent = new AnalysisIntent(
                AnalysisStatus.QUERY_REJECTED,
                QueryRequirements.empty(),
                ClarificationRequirement.none());

        // When: the status is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: the status is rejected for intent interpretation
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "Status is not valid during intent interpretation");
    }

    @ParameterizedTest
    @EnumSource(
            value = AnalysisStatus.class,
            names = {
                    "UNSUPPORTED_ANALYSIS",
                    "OUT_OF_SCOPE",
                    "REJECTED"
            })
    void givenValidTerminalIntentWhenValidatedThenIntentIsAccepted(
            AnalysisStatus status) {

        // Given: a terminal intent without executable requirements
        AnalysisIntent intent = new AnalysisIntent(
                status,
                QueryRequirements.empty(),
                ClarificationRequirement.none());

        // When: the terminal intent is validated
        AnalysisIntent result = validator.validate(intent);

        // Then: the terminal intent passes unchanged
        assertThat(result).isSameAs(intent);
    }

    @Test
    void givenTerminalIntentWithRequirementsWhenValidatedThenIntentIsRejected() {
        // Given: a rejected intent that still contains query requirements
        AnalysisIntent intent = new AnalysisIntent(
                AnalysisStatus.REJECTED,
                new RequirementsBuilder().build(),
                ClarificationRequirement.none());

        // When: the contradictory terminal intent is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: terminal intents cannot carry query requirements
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "must not contain query requirements");
    }

    @Test
    void givenValidClarificationIntentWhenValidatedThenIntentIsAccepted() {
        // Given: an intent that explicitly requires a time range
        AnalysisIntent intent = new AnalysisIntent(
                AnalysisStatus.NEEDS_CLARIFICATION,
                QueryRequirements.empty(),
                new ClarificationRequirement(
                        true,
                        List.of(
                                ClarificationRequirement.MissingField.TIME_RANGE),
                        "What time range should I use?"));

        // When: the clarification intent is validated
        AnalysisIntent result = validator.validate(intent);

        // Then: the clarification intent passes unchanged
        assertThat(result).isSameAs(intent);
    }

    @Test
    void givenClarificationWithoutQuestionWhenValidatedThenIntentIsRejected() {
        // Given: clarification is required but no question is provided
        AnalysisIntent intent = new AnalysisIntent(
                AnalysisStatus.NEEDS_CLARIFICATION,
                QueryRequirements.empty(),
                new ClarificationRequirement(
                        true,
                        List.of(
                                ClarificationRequirement.MissingField.TIME_RANGE),
                        null));

        // When: the incomplete clarification is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: a clarification question is required
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Clarification must contain a question");
    }

    @Test
    void givenSuccessfulIntentWithoutTimeRangeWhenValidatedThenIntentIsRejected() {
        // Given: an otherwise valid intent without a bounded time range
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .timeRange(null)
                        .build());

        // When: the successful intent is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: successful analytics requires a bounded time range
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage(
                        "Successful intent must contain a bounded time range");
    }

    @Test
    void givenSuccessfulIntentWithoutDimensionsOrMetricsWhenValidatedThenIntentIsRejected() {
        // Given: a successful intent requesting no analytics data
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .dimensions()
                        .metrics()
                        .groupBy()
                        .sort()
                        .build());

        // When: the empty successful intent is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: at least one dimension or metric is required
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "at least one dimension or metric");
    }

    @Test
    void givenUnknownSemanticDimensionWhenValidatedThenIntentIsRejected() {
        // Given: an intent referencing a field outside the curated catalog
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .dimensions("UNKNOWN_FIELD")
                        .groupBy("UNKNOWN_FIELD")
                        .build());

        // When: the semantic requirement is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: unapproved semantic fields are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage(
                        "Unknown or unapproved semantic field: UNKNOWN_FIELD");
    }

    @Test
    void givenUnsupportedMetricAggregationWhenValidatedThenIntentIsRejected() {
        // Given: TOTAL_BYTES is incorrectly requested with COUNT aggregation
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .metrics(metric("TOTAL_BYTES", COUNT))
                        .build());

        // When: the metric capability is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: unsupported metric aggregation is rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "Aggregation COUNT is not allowed for TOTAL_BYTES");
    }

    @Test
    void givenDuplicateMetricWhenValidatedThenIntentIsRejected() {
        // Given: the same metric and aggregation are requested twice
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .metrics(
                                metric("TOTAL_BYTES", SUM),
                                metric("TOTAL_BYTES", SUM))
                        .build());

        // When: metric uniqueness is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: duplicate metric requirements are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "Duplicate metric requirement");
    }

    @Test
    void givenMixedRawAndAggregatedMetricsWhenValidatedThenIntentIsRejected() {
        // Given: raw and aggregated metrics are mixed in one request
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .metrics(
                                metric("TOTAL_BYTES", SUM),
                                metric("OUT_BYTES", NONE))
                        .build());

        // When: aggregation consistency is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: contradictory aggregation modes are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage(
                        "Aggregated and non-aggregated metrics cannot be mixed");
    }

    @Test
    void givenUngroupedDimensionWithAggregatedMetricWhenValidatedThenIntentIsRejected() {
        // Given: an aggregate query has a dimension missing from groupBy
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .groupBy()
                        .build());

        // When: aggregate grouping consistency is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: every aggregate dimension must be grouped
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "Aggregated queries must group by every requested dimension");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SOURCE_IP", "DESTINATION_IP", "IP_ADDRESS"})
    void givenIpFieldAsGenericFilterWhenValidatedThenIntentIsRejected(
            String ipField) {
        // Given: an IP semantic field is incorrectly expressed as a generic filter
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .filters(filter(ipField, EQ, "10.1.2.3"))
                        .build());

        // When: filter semantics are validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: IP filtering must use the dedicated IP requirement model
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("IP filters must use ipRequirements");
    }

    @Test
    void givenBetweenFilterWithOneValueWhenValidatedThenIntentIsRejected() {
        // Given: BETWEEN contains only one boundary value
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .filters(filter("L7_PROTOCOL", BETWEEN, "7"))
                        .build());

        // When: filter value cardinality is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: BETWEEN requires exactly two values
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("BETWEEN requires exactly two values");
    }

    @Test
    void givenMultiValueInFilterWhenValidatedThenIntentIsAccepted() {
        // Given: an IN filter contains two valid values
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .filters(filter(
                                "L7_PROTOCOL",
                                IN,
                                "7",
                                "9"))
                        .build());

        // When: the multi-value filter is validated
        AnalysisIntent result = validator.validate(intent);

        // Then: IN accepts multiple values
        assertThat(result).isSameAs(intent);
    }

    @Test
    void givenInvalidExactIpv4WhenValidatedThenIntentIsRejected() {
        // Given: an exact IP requirement contains an invalid IPv4 address
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .ipRequirements(ip(
                                SOURCE,
                                EXACT,
                                "10.1.2.999"))
                        .build());

        // When: the IP requirement is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: invalid IPv4 addresses are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Invalid IPv4 address");
    }

    @Test
    void givenInvalidCidrPrefixWhenValidatedThenIntentIsRejected() {
        // Given: a CIDR requirement contains a prefix above 32
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .ipRequirements(ip(
                                SOURCE,
                                CIDR,
                                "10.1.0.0/33"))
                        .build());

        // When: the CIDR requirement is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: invalid IPv4 CIDR prefixes are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Invalid IPv4 CIDR prefix");
    }

    @Test
    void givenDuplicateIpRequirementWhenValidatedThenIntentIsRejected() {
        // Given: the same IP requirement is repeated
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .ipRequirements(ip(
                                SOURCE,
                                EXACT,
                                "10.1.2.3",
                                "10.1.2.3"))
                        .build());

        // When: IP requirement uniqueness is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: duplicate IP requirements are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining("Duplicate IP requirement");
    }

    @Test
    void givenReversedTimeRangeWhenValidatedThenIntentIsRejected() {
        // Given: the requested start time is after the end time
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .timeRange(new QueryRequirements.TimeRange(
                                END,
                                START))
                        .build());

        // When: the time range is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: reversed time ranges are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Time range start must be before end");
    }

    @Test
    void givenTimeRangeEndingInFutureWhenValidatedThenIntentIsRejected() {
        // Given: a range ends after the validator's current UTC time
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .timeRange(new QueryRequirements.TimeRange(
                                CURRENT_TIME.minus(Duration.ofHours(1)),
                                CURRENT_TIME.plusSeconds(1)))
                        .build());

        // When: the future boundary is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: future query ranges are rejected before execution
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Time range end must not be in the future");
    }

    @Test
    void givenTimeRangeLongerThanSevenDaysWhenValidatedThenIntentIsRejected() {
        // Given: a range exceeds the maximum analytics scan window
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .timeRange(new QueryRequirements.TimeRange(
                                CURRENT_TIME.minus(Duration.ofDays(7).plusSeconds(1)),
                                CURRENT_TIME))
                        .build());

        // When: the maximum range is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: oversized query ranges are rejected before execution
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Time range must not exceed PT168H");
    }

    @Test
    void givenTimeRangeOfExactlySevenDaysWhenValidatedThenIntentIsAccepted() {
        // Given: a range exactly matches the maximum analytics window
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .timeRange(new QueryRequirements.TimeRange(
                                CURRENT_TIME.minus(Duration.ofDays(7)),
                                CURRENT_TIME))
                        .build());

        // When: the inclusive maximum boundary is validated
        AnalysisIntent result = validator.validate(intent);

        // Then: the seven-day boundary remains supported
        assertThat(result).isSameAs(intent);
    }

    @Test
    void givenZeroGranularityWhenValidatedThenIntentIsRejected() {
        // Given: a request contains zero-length time granularity
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .granularity(Duration.ZERO)
                        .build());

        // When: granularity is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: granularity must be strictly positive
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage("Granularity must be positive");
    }

    @Test
    void givenUnrequestedGroupedFieldWhenValidatedThenIntentIsRejected() {
        // Given: groupBy contains a dimension absent from the result dimensions
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .metrics(metric("TOTAL_BYTES", NONE))
                        .groupBy("DESTINATION_IP")
                        .sort()
                        .build());

        // When: grouping consistency is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: grouped fields must also be requested dimensions
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining(
                        "Grouped field must also be requested as a dimension");
    }

    @Test
    void givenDuplicateSortRequirementWhenValidatedThenIntentIsRejected() {
        // Given: the same aggregated sort appears twice
        QueryRequirements.SortRequirement sort =
                sort("TOTAL_BYTES", SUM);

        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .sort(sort, sort)
                        .build());

        // When: sort uniqueness is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: duplicate sort requirements are rejected
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining("Duplicate sort requirement");
    }

    @Test
    void givenAggregatedSortWithoutMetricWhenValidatedThenIntentIsRejected() {
        // Given: sorting references an aggregate metric not requested by the query
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .sort(sort("OUT_BYTES", SUM))
                        .build());

        // When: the aggregated sort reference is validated
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: sort must reference an actual requested metric
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessage(
                        "Aggregated sort must reference a requested metric");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1001})
    void givenIllegalLimitWhenValidatedThenIntentIsRejected(int limit) {
        // Given: the query limit is outside the allowed range
        AnalysisIntent intent = success(
                new RequirementsBuilder()
                        .limit(limit)
                        .build());

        // When: bean constraints validate the intent
        var assertion = assertThatThrownBy(() -> validator.validate(intent));

        // Then: the illegal limit violates the structured contract
        assertion
                .isInstanceOf(InvalidIntentException.class)
                .hasMessageContaining("queryRequirements.limit");
    }

    private static AnalysisIntent success(
            QueryRequirements requirements) {

        return new AnalysisIntent(
                AnalysisStatus.SUCCESS,
                requirements,
                ClarificationRequirement.none());
    }

    private static QueryRequirements.MetricRequirement metric(
            String field,
            QueryRequirements.Aggregation aggregation) {

        return new QueryRequirements.MetricRequirement(
                field,
                aggregation);
    }

    private static QueryRequirements.FilterRequirement filter(
            String field,
            QueryRequirements.FilterOperator operator,
            String... values) {

        return new QueryRequirements.FilterRequirement(
                field,
                operator,
                List.of(values));
    }

    private static QueryRequirements.IpRequirement ip(
            QueryRequirements.IpRole role,
            QueryRequirements.IpMatchType matchType,
            String... values) {

        return new QueryRequirements.IpRequirement(
                role,
                matchType,
                List.of(values));
    }

    private static QueryRequirements.SortRequirement sort(
            String field,
            QueryRequirements.Aggregation aggregation) {

        return new QueryRequirements.SortRequirement(
                field,
                aggregation,
                DESC);
    }

    private static final class RequirementsBuilder {

        private List<String> dimensions =
                List.of("SOURCE_IP");

        private List<QueryRequirements.MetricRequirement> metrics =
                List.of(metric("TOTAL_BYTES", SUM));

        private List<QueryRequirements.FilterRequirement> filters =
                List.of();

        private List<QueryRequirements.IpRequirement> ipRequirements =
                List.of();

        private QueryRequirements.TrafficDirection trafficDirection =
                QueryRequirements.TrafficDirection.UNSPECIFIED;

        private QueryRequirements.TimeRange timeRange =
                new QueryRequirements.TimeRange(START, END);

        private List<String> groupBy =
                List.of("SOURCE_IP");

        private Duration granularity;

        private List<QueryRequirements.SortRequirement> sort =
                List.of(IntentValidatorTest.sort("TOTAL_BYTES", SUM));

        private Integer limit = 10;

        private boolean distinct;

        RequirementsBuilder dimensions(String... dimensions) {
            this.dimensions = List.of(dimensions);
            return this;
        }

        RequirementsBuilder metrics(
                QueryRequirements.MetricRequirement... metrics) {

            this.metrics = List.of(metrics);
            return this;
        }

        RequirementsBuilder filters(
                QueryRequirements.FilterRequirement... filters) {

            this.filters = List.of(filters);
            return this;
        }

        RequirementsBuilder ipRequirements(
                QueryRequirements.IpRequirement... ipRequirements) {

            this.ipRequirements = List.of(ipRequirements);
            return this;
        }

        RequirementsBuilder timeRange(
                QueryRequirements.TimeRange timeRange) {

            this.timeRange = timeRange;
            return this;
        }

        RequirementsBuilder groupBy(String... groupBy) {
            this.groupBy = List.of(groupBy);
            return this;
        }

        RequirementsBuilder granularity(Duration granularity) {
            this.granularity = granularity;
            return this;
        }

        RequirementsBuilder sort(
                QueryRequirements.SortRequirement... sort) {

            this.sort = List.of(sort);
            return this;
        }

        RequirementsBuilder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        QueryRequirements build() {
            return new QueryRequirements(
                    dimensions,
                    metrics,
                    filters,
                    ipRequirements,
                    trafficDirection,
                    timeRange,
                    groupBy,
                    granularity,
                    sort,
                    limit,
                    distinct);
        }
    }
}
