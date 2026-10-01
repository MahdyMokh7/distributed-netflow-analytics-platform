package com.nimbo.analyticsquery.planner;

import com.nimbo.analyticsquery.intent.IntentInterpreter;
import com.nimbo.analyticsquery.intent.InvalidIntentException;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.PlannerResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AnalyticsQueryService {

    private final IntentInterpreter intentInterpreter;
    private final AnalyticsPlanner analyticsPlanner;

    public AnalyticsQueryService(
            IntentInterpreter intentInterpreter,
            AnalyticsPlanner analyticsPlanner) {

        this.intentInterpreter = intentInterpreter;
        this.analyticsPlanner = analyticsPlanner;
    }

    public PlannerResult query(String question) {
        try {
            AnalysisIntent intent = intentInterpreter.interpret(question);

            return analyticsPlanner.plan(intent);

        } catch (LlmClientException exception) {
            log.warn("LLM intent interpretation failed with kind {}", exception.kind(), exception);

            if (exception.kind() == LlmClientException.Kind.UNAVAILABLE) {
                return PlannerResult.failure(
                        AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                        "Analytics interpretation service is temporarily unavailable");
            }

            return interpretationFailure();

        } catch (InvalidIntentException exception) {
            log.warn("LLM returned an invalid analytics intent", exception);

            return interpretationFailure();
        }
    }

    private PlannerResult interpretationFailure() {
        return PlannerResult.failure(
                AnalysisStatus.EXECUTION_FAILED,
                "Analytics interpretation failed");
    }
}
