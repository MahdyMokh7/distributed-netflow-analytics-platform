package com.nimbo.analyticsquery.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Endpoint;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.model.AnalysisStatus;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class AnalyticsQueryMetricsTest {

    @Test
    void givenSuccessfulRequestWhenObservedThenRequestAndDurationAreRecorded() {
        // Given: an isolated metrics registry
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AnalyticsQueryMetrics metrics = new AnalyticsQueryMetrics(registry);

        // When: a successful query request is observed
        AnalysisStatus result = metrics.observeRequest(
                Endpoint.QUERY,
                () -> AnalysisStatus.SUCCESS,
                status -> status);

        // Then: request, duration and active-request metrics are consistent
        assertThat(result).isEqualTo(AnalysisStatus.SUCCESS);
        assertThat(registry.counter(
                        "analytics.query.requests",
                        "endpoint", "query",
                        "status", "success")
                .count()).isEqualTo(1);
        assertThat(registry.timer(
                        "analytics.query.request.duration",
                        "endpoint", "query",
                        "status", "success")
                .count()).isEqualTo(1);
        assertThat(registry.get("analytics.query.active.requests").gauge().value())
                .isZero();
    }

    @Test
    void givenFailedStageWhenObservedThenTimingAndFailureRemainControlled() {
        // Given: metrics for an SQL validation stage
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AnalyticsQueryMetrics metrics = new AnalyticsQueryMetrics(registry);

        // When: the stage throws and its controlled failure is recorded
        assertThatThrownBy(() -> metrics.observeStage(
                        Stage.SQL_VALIDATION,
                        () -> {
                            throw new IllegalStateException("invalid SQL");
                        }))
                .isInstanceOf(IllegalStateException.class);
        metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_POLICY);

        // Then: only controlled stage and reason labels are emitted
        assertThat(registry.timer(
                        "analytics.query.stage.duration",
                        "stage", "sql_validation",
                        "outcome", "failure")
                .count()).isEqualTo(1);
        assertThat(registry.counter(
                        "analytics.query.failures",
                        "stage", "sql_validation",
                        "reason", "sql_policy")
                .count()).isEqualTo(1);
    }
}
