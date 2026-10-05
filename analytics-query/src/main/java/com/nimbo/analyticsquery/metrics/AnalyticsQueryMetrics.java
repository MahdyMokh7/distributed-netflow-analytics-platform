package com.nimbo.analyticsquery.metrics;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.ExecutionMode;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class AnalyticsQueryMetrics {

    private final MeterRegistry registry;
    private final AtomicInteger activeRequests = new AtomicInteger();

    public AnalyticsQueryMetrics(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("analytics.query.active.requests", activeRequests, AtomicInteger::get)
                .register(registry);
    }

    public <T> T observeRequest(
            Endpoint endpoint,
            Supplier<T> operation,
            Function<T, AnalysisStatus> statusExtractor) {

        Timer.Sample sample = Timer.start(registry);
        activeRequests.incrementAndGet();

        AnalysisStatus status = AnalysisStatus.EXECUTION_FAILED;

        try {
            T result = operation.get();
            status = statusExtractor.apply(result);
            return result;
        } finally {
            activeRequests.decrementAndGet();
            Counter.builder("analytics.query.requests")
                    .tag("endpoint", value(endpoint))
                    .tag("status", value(status))
                    .register(registry)
                    .increment();
            sample.stop(requestTimer(endpoint, status));
        }
    }

    public <T> T observeStage(Stage stage, Supplier<T> operation) {
        return observeStage(stage, operation, ignored -> true);
    }

    public <T> T observeStage(
            Stage stage,
            Supplier<T> operation,
            Predicate<T> successful) {

        return observe(
                "analytics.query.stage.duration",
                "stage",
                value(stage),
                operation,
                successful);
    }

    public <T> T observeDependency(
            Dependency dependency,
            Supplier<T> operation) {

        return observeDependency(dependency, operation, ignored -> true);
    }

    public <T> T observeDependency(
            Dependency dependency,
            Supplier<T> operation,
            Predicate<T> successful) {

        return observe(
                "analytics.query.dependency.duration",
                "dependency",
                value(dependency),
                operation,
                successful);
    }

    public void recordFailure(Stage stage, FailureReason reason) {
        Counter.builder("analytics.query.failures")
                .tag("stage", value(stage))
                .tag("reason", value(reason))
                .register(registry)
                .increment();
    }

    public void recordExecution(ExecutionMode mode, AnalysisDataSource dataSource) {
        Counter.builder("analytics.query.execution")
                .tag("mode", value(mode))
                .tag("datasource", value(dataSource))
                .register(registry)
                .increment();
    }

    public static Dependency dependency(AnalysisDataSource dataSource) {
        return switch (dataSource) {
            case POSTGRESQL -> Dependency.POSTGRESQL;
            case STARROCKS -> Dependency.STARROCKS;
        };
    }

    private Timer requestTimer(Endpoint endpoint, AnalysisStatus status) {
        return Timer.builder("analytics.query.request.duration")
                .tag("endpoint", value(endpoint))
                .tag("status", value(status))
                .publishPercentileHistogram()
                .register(registry);
    }

    private <T> T observe(
            String metric,
            String tagName,
            String tagValue,
            Supplier<T> operation,
            Predicate<T> successful) {

        Timer.Sample sample = Timer.start(registry);
        Outcome outcome = Outcome.FAILURE;

        try {
            T result = operation.get();
            outcome = successful.test(result) ? Outcome.SUCCESS : Outcome.FAILURE;
            return result;
        } finally {
            sample.stop(
                    Timer.builder(metric)
                            .tag(tagName, tagValue)
                            .tag("outcome", value(outcome))
                            .publishPercentileHistogram()
                            .register(registry));
        }
    }

    private static String value(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }

    public enum Endpoint {
        QUERY,
        SQL_GENERATE,
        SQL_EXECUTE
    }

    public enum Stage {
        INTENT,
        PLANNER,
        TOOL,
        SQL_ELIGIBILITY,
        ROUTING,
        SQL_GENERATION,
        SQL_VALIDATION,
        QUERY_PLAN,
        SQL_EXECUTION,
        EXPLANATION
    }

    public enum Dependency {
        LLM,
        REST_API,
        POSTGRESQL,
        STARROCKS
    }

    public enum Outcome {
        SUCCESS,
        FAILURE
    }

    public enum FailureReason {
        INVALID_INTENT,
        INVALID_LLM_RESPONSE,
        DEPENDENCY_UNAVAILABLE,
        TOOL_EXECUTION,
        SQL_PARSE,
        SQL_POLICY,
        QUERY_PLAN,
        SQL_EXECUTION,
        COMPONENT_UNAVAILABLE,
        EXPLANATION
    }
}
