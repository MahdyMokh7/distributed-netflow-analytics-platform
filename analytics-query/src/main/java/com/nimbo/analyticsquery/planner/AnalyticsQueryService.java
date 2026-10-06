package com.nimbo.analyticsquery.planner;

import com.nimbo.analyticsquery.explanation.ExplanationService;
import com.nimbo.analyticsquery.intent.IntentInterpreter;
import com.nimbo.analyticsquery.intent.InvalidIntentException;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Dependency;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.PlannerResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AnalyticsQueryService {

  private final IntentInterpreter intentInterpreter;
  private final AnalyticsPlanner analyticsPlanner;
  private final ExplanationService explanationService;
  private final AnalyticsQueryMetrics metrics;

  public AnalyticsQueryService(
      IntentInterpreter intentInterpreter,
      AnalyticsPlanner analyticsPlanner,
      ExplanationService explanationService,
      AnalyticsQueryMetrics metrics) {

    this.intentInterpreter = intentInterpreter;
    this.analyticsPlanner = analyticsPlanner;
    this.explanationService = explanationService;
    this.metrics = metrics;
  }

  public PlannerResult query(String question) {
    try {
      AnalysisIntent intent =
          metrics.observeStage(
              Stage.INTENT,
              () ->
                  metrics.observeDependency(
                      Dependency.LLM, () -> intentInterpreter.interpret(question)));

      PlannerResult result =
          metrics.observeStage(
              Stage.PLANNER,
              () -> analyticsPlanner.plan(question, intent),
              planned -> planned.status() == AnalysisStatus.SUCCESS);

      if (result.status() != AnalysisStatus.SUCCESS) {
        return result;
      }

      String explanation =
          metrics.observeStage(
              Stage.EXPLANATION,
              () ->
                  metrics.observeDependency(
                      Dependency.LLM,
                      () ->
                          explanationService.explain(
                              question, intent.queryRequirements(), result.result()),
                      java.util.Objects::nonNull),
              java.util.Objects::nonNull);

      if (explanation == null) {
        metrics.recordFailure(Stage.EXPLANATION, FailureReason.EXPLANATION);
      }

      return result.withExplanation(explanation);

    } catch (LlmClientException exception) {
      log.warn("LLM intent interpretation failed category={}", exception.kind());
      metrics.recordFailure(
          Stage.INTENT,
          exception.kind() == LlmClientException.Kind.UNAVAILABLE
              ? FailureReason.DEPENDENCY_UNAVAILABLE
              : FailureReason.INVALID_INTENT);

      if (exception.kind() == LlmClientException.Kind.UNAVAILABLE) {
        return PlannerResult.failure(
            AnalysisStatus.DEPENDENCY_UNAVAILABLE,
            "Analytics interpretation service is temporarily unavailable");
      }

      return interpretationFailure();

    } catch (InvalidIntentException exception) {
      log.warn("LLM returned an invalid analytics intent category=INVALID_INTENT");
      metrics.recordFailure(Stage.INTENT, FailureReason.INVALID_INTENT);

      return interpretationFailure();
    }
  }

  private PlannerResult interpretationFailure() {
    return PlannerResult.failure(
        AnalysisStatus.EXECUTION_FAILED, "Analytics interpretation failed");
  }
}
