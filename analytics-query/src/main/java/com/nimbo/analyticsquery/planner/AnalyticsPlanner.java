package com.nimbo.analyticsquery.planner;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.routing.DataSourceRouter;
import com.nimbo.analyticsquery.sql.SqlGenerationContext;
import com.nimbo.analyticsquery.sql.SqlGenerationContextFactory;
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
    private final SqlGenerationContextFactory sqlGenerationContextFactory;

    public AnalyticsPlanner(
            ToolMatcher toolMatcher,
            SqlFallbackEligibilityPolicy sqlFallbackEligibilityPolicy,
            DataSourceRouter dataSourceRouter,
            SqlGenerationContextFactory sqlGenerationContextFactory) {
        this.toolMatcher = toolMatcher;
        this.sqlFallbackEligibilityPolicy = sqlFallbackEligibilityPolicy;
        this.dataSourceRouter = dataSourceRouter;
        this.sqlGenerationContextFactory = sqlGenerationContextFactory;
    }

    public PlannerResult plan(AnalysisIntent intent) {

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
                .orElseGet(() -> evaluateSqlFallback(intent));
    }

    private PlannerResult evaluateSqlFallback(AnalysisIntent intent) {

        SqlFallbackEligibility eligibility =
                sqlFallbackEligibilityPolicy.evaluate(intent.queryRequirements());

        if (!eligibility.eligible()) {
            return PlannerResult.failure(eligibility.status(), eligibility.message());
        }

        return dataSourceRouter
                .route(intent.queryRequirements())
                .map(this::pendingSqlGeneration)
                .orElseGet(
                        () ->
                                PlannerResult.failure(
                                        AnalysisStatus.UNSUPPORTED_ANALYSIS,
                                        "No single datasource preserves the requested semantics"));
    }

    private PlannerResult pendingSqlGeneration(DataSourceRoute route) {

        SqlGenerationContext context = sqlGenerationContextFactory.create(route);

        log.debug(
                "Prepared SQL context for {} with {} allowlisted table",
                context.dataSource(),
                context.tables().size());

        return PlannerResult.failure(
                AnalysisStatus.UNSUPPORTED_ANALYSIS, "SQL generation is not implemented yet");
    }

    private PlannerResult execute(AnalyticsTool tool, AnalysisIntent intent) {

        try {
            AnalyticsResult result = tool.execute(intent.queryRequirements());

            return PlannerResult.success(result);

        } catch (ToolExecutionException exception) {
            log.warn(
                    "Analytics tool {} failed with status {}",
                    tool.name(),
                    exception.status(),
                    exception);

            return PlannerResult.failure(exception.status(), exception.userMessage());
        }
    }
}
