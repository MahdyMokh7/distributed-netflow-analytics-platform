package com.nimbo.analyticsquery.planner;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.tool.AnalyticsTool;
import com.nimbo.analyticsquery.tool.ToolExecutionException;
import com.nimbo.analyticsquery.tool.ToolMatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AnalyticsPlanner {

    private final ToolMatcher toolMatcher;


    public AnalyticsPlanner(ToolMatcher toolMatcher) {
        this.toolMatcher = toolMatcher;
    }


    public PlannerResult plan(AnalysisIntent intent) {

        if (intent.status() != AnalysisStatus.SUCCESS) {
            String message = intent.status() == AnalysisStatus.NEEDS_CLARIFICATION
                    ? intent.clarification().question()
                    : "Analytics request cannot be executed";

            return PlannerResult.failure(intent.status(), message);
        }

        return toolMatcher.findTool(
                intent.queryRequirements())
                .map(tool -> execute(tool, intent))
                .orElseGet(() -> PlannerResult.failure(
                                AnalysisStatus.UNSUPPORTED_ANALYSIS,
                                "No supported analytics capability found"));
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

            return PlannerResult.failure(
                    exception.status(),
                    exception.userMessage());
        }
    }
}
