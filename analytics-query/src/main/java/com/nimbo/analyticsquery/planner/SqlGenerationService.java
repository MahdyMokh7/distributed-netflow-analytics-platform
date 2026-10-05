package com.nimbo.analyticsquery.planner;

import org.springframework.stereotype.Service;

import com.nimbo.analyticsquery.intent.IntentInterpreter;
import com.nimbo.analyticsquery.intent.InvalidIntentException;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Dependency;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.routing.DataSourceRouter;
import com.nimbo.analyticsquery.sql.GeneratedSqlCandidate;
import com.nimbo.analyticsquery.sql.ParsedSql;
import com.nimbo.analyticsquery.sql.SqlAstParser;
import com.nimbo.analyticsquery.sql.SqlGenerationContext;
import com.nimbo.analyticsquery.sql.SqlGenerationContextFactory;
import com.nimbo.analyticsquery.sql.SqlGenerationResult;
import com.nimbo.analyticsquery.sql.SqlGenerator;
import com.nimbo.analyticsquery.sql.SqlParsingException;
import com.nimbo.analyticsquery.sql.SqlRequirementValidator;
import com.nimbo.analyticsquery.sql.SqlValidationException;
import com.nimbo.analyticsquery.sql.SqlValidator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SqlGenerationService {

    private final IntentInterpreter intentInterpreter;
    private final SqlFallbackEligibilityPolicy eligibilityPolicy;
    private final DataSourceRouter dataSourceRouter;
    private final SqlGenerationContextFactory contextFactory;
    private final SqlGenerator sqlGenerator;
    private final SqlAstParser sqlAstParser;
    private final SqlValidator sqlValidator;
    private final SqlRequirementValidator sqlRequirementValidator;
    private final AnalyticsQueryMetrics metrics;

    public SqlGenerationService(
            IntentInterpreter intentInterpreter,
            SqlFallbackEligibilityPolicy eligibilityPolicy,
            DataSourceRouter dataSourceRouter,
            SqlGenerationContextFactory contextFactory,
            SqlGenerator sqlGenerator,
            SqlAstParser sqlAstParser,
            SqlValidator sqlValidator,
            SqlRequirementValidator sqlRequirementValidator,
            AnalyticsQueryMetrics metrics) {
        this.intentInterpreter = intentInterpreter;
        this.eligibilityPolicy = eligibilityPolicy;
        this.dataSourceRouter = dataSourceRouter;
        this.contextFactory = contextFactory;
        this.sqlGenerator = sqlGenerator;
        this.sqlAstParser = sqlAstParser;
        this.sqlValidator = sqlValidator;
        this.sqlRequirementValidator = sqlRequirementValidator;
        this.metrics = metrics;
    }

    public SqlGenerationResult generate(String question) {
        try {
            AnalysisIntent intent = metrics.observeStage(
                    Stage.INTENT,
                    () -> metrics.observeDependency(
                            Dependency.LLM,
                            () -> intentInterpreter.interpret(question)));

            if (intent.status() != AnalysisStatus.SUCCESS) {
                return terminalResult(intent);
            }

            SqlFallbackEligibility eligibility = metrics.observeStage(
                    Stage.SQL_ELIGIBILITY,
                    () -> eligibilityPolicy.evaluate(intent.queryRequirements()),
                    SqlFallbackEligibility::eligible);

            if (!eligibility.eligible()) {
                return SqlGenerationResult.failure(
                        eligibility.status(), eligibility.message());
            }

            DataSourceRoute route = metrics.observeStage(
                    Stage.ROUTING,
                    () -> dataSourceRouter.route(intent.queryRequirements()),
                    java.util.Optional::isPresent)
                    .orElse(null);

            if (route == null) {
                return SqlGenerationResult.failure(
                        AnalysisStatus.UNSUPPORTED_ANALYSIS,
                        "No single datasource preserves the requested semantics");
            }

            SqlGenerationContext context = contextFactory.create(route);
            GeneratedSqlCandidate candidate = metrics.observeStage(
                    Stage.SQL_GENERATION,
                    () -> metrics.observeDependency(
                            Dependency.LLM,
                            () -> sqlGenerator.generate(
                                    question,
                                    intent.queryRequirements(),
                                    context)));
            ParsedSql parsedSql = metrics.observeStage(
                    Stage.SQL_VALIDATION,
                    () -> sqlAstParser.parse(candidate));

            ParsedSql validatedSql = metrics.observeStage(
                    Stage.SQL_VALIDATION,
                    () -> sqlValidator.validate(parsedSql, context));
            sqlRequirementValidator.validate(
                    validatedSql, intent.queryRequirements(), context);
            return SqlGenerationResult.success(validatedSql);

        } catch (LlmClientException exception) {
            log.warn("LLM processing failed during SQL generation with kind {}", exception.kind());
            metrics.recordFailure(
                    Stage.SQL_GENERATION,
                    exception.kind() == LlmClientException.Kind.UNAVAILABLE
                            ? FailureReason.DEPENDENCY_UNAVAILABLE
                            : FailureReason.INVALID_LLM_RESPONSE);

            if (exception.kind() == LlmClientException.Kind.UNAVAILABLE) {
                return SqlGenerationResult.failure(
                        AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                        "Analytics generation service is temporarily unavailable");
            }

            return generationFailure();

        } catch (InvalidIntentException exception) {
            log.warn("LLM returned an invalid analytics intent during SQL generation");
            metrics.recordFailure(Stage.INTENT, FailureReason.INVALID_INTENT);
            return generationFailure();

        } catch (SqlParsingException exception) {
            log.warn("Generated SQL could not be parsed");
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_PARSE);
            return SqlGenerationResult.failure(
                    AnalysisStatus.QUERY_REJECTED,
                    "Generated SQL did not satisfy the query safety policy");

        } catch (SqlValidationException exception) {
            log.warn("Generated SQL was rejected by policy: {}", exception.violation());
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_POLICY);
            return SqlGenerationResult.failure(
                    AnalysisStatus.QUERY_REJECTED,
                    "Generated SQL did not satisfy the query safety policy");
        }
    }

    private SqlGenerationResult terminalResult(AnalysisIntent intent) {
        String message =
                intent.status() == AnalysisStatus.NEEDS_CLARIFICATION
                        ? intent.clarification().question()
                        : "Analytics request cannot be converted to SQL";

        return SqlGenerationResult.failure(intent.status(), message);
    }

    private SqlGenerationResult generationFailure() {
        return SqlGenerationResult.failure(
                AnalysisStatus.EXECUTION_FAILED,
                "Analytics SQL generation failed");
    }
}
