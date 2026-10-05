package com.nimbo.analyticsquery.planner;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Dependency;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.routing.DataSourceRouter;
import com.nimbo.analyticsquery.tool.AnalyticsTool;
import com.nimbo.analyticsquery.tool.ToolExecutionException;
import com.nimbo.analyticsquery.tool.ToolMatcher;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AnalyticsPlanner {

    private final ToolMatcher toolMatcher;
    private final SqlFallbackEligibilityPolicy sqlFallbackEligibilityPolicy;
    private final DataSourceRouter dataSourceRouter;
    private final SqlFallbackExecutionService sqlFallbackExecutionService;
    private final AnalyticsQueryMetrics metrics;

    public AnalyticsPlanner(
            ToolMatcher toolMatcher,
            SqlFallbackEligibilityPolicy sqlFallbackEligibilityPolicy,
            DataSourceRouter dataSourceRouter,
            SqlFallbackExecutionService sqlFallbackExecutionService,
            AnalyticsQueryMetrics metrics) {
        this.toolMatcher = toolMatcher;
        this.sqlFallbackEligibilityPolicy = sqlFallbackEligibilityPolicy;
        this.dataSourceRouter = dataSourceRouter;
        this.sqlFallbackExecutionService = sqlFallbackExecutionService;
        this.metrics = metrics;
    }

    public PlannerResult plan(String question, AnalysisIntent intent) {

        if (intent.status() != AnalysisStatus.SUCCESS) {
            String message =
                    intent.status() == AnalysisStatus.NEEDS_CLARIFICATION
                            ? intent.clarification().question()
                            : "Analytics request cannot be executed";

            return PlannerResult.failure(intent.status(), message);
        }

        return toolMatcher
                .findTool(intent.queryRequirements())
                .map(tool -> execute(tool, intent))
                .orElseGet(() -> evaluateSqlFallback(question, intent));
    }

    private PlannerResult evaluateSqlFallback(String question, AnalysisIntent intent) {

        SqlFallbackEligibility eligibility = metrics.observeStage(
                Stage.SQL_ELIGIBILITY,
                () -> sqlFallbackEligibilityPolicy.evaluate(intent.queryRequirements()),
                SqlFallbackEligibility::eligible);

        if (!eligibility.eligible()) {
            return PlannerResult.failure(eligibility.status(), eligibility.message());
        }

        return metrics.observeStage(
                        Stage.ROUTING,
                        () -> dataSourceRouter.route(intent.queryRequirements()),
                        java.util.Optional::isPresent)
                .map(
                        route ->
                                sqlFallbackExecutionService.execute(
                                        question,
                                        intent.queryRequirements(),
                                        route))
                .orElseGet(
                        () ->
                                PlannerResult.failure(
                                        AnalysisStatus.UNSUPPORTED_ANALYSIS,
                                        "No single datasource preserves the requested semantics"));
    }

    private PlannerResult execute(AnalyticsTool tool, AnalysisIntent intent) {

        log.info("Matched analytics tool tool={}", tool.name());

        try {
            AnalyticsResult result = metrics.observeStage(
                    Stage.TOOL,
                    () -> metrics.observeDependency(
                            Dependency.REST_API,
                            () -> tool.execute(intent.queryRequirements())));

            return PlannerResult.success(result);

        } catch (ToolExecutionException exception) {
            FailureReason failureReason =
                    exception.status() == AnalysisStatus.DEPENDENCY_UNAVAILABLE
                            ? FailureReason.DEPENDENCY_UNAVAILABLE
                            : FailureReason.TOOL_EXECUTION;
            log.warn(
                    "Analytics tool failed tool={} status={} category={}",
                    tool.name(),
                    exception.status(),
                    failureReason);
            metrics.recordFailure(Stage.TOOL, failureReason);

            return PlannerResult.failure(exception.status(), exception.userMessage());
        }
    }
}
