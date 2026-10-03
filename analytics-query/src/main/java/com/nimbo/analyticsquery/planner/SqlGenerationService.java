package com.nimbo.analyticsquery.planner;

import org.springframework.stereotype.Service;

import com.nimbo.analyticsquery.intent.IntentInterpreter;
import com.nimbo.analyticsquery.intent.InvalidIntentException;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;
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

    public SqlGenerationService(
            IntentInterpreter intentInterpreter,
            SqlFallbackEligibilityPolicy eligibilityPolicy,
            DataSourceRouter dataSourceRouter,
            SqlGenerationContextFactory contextFactory,
            SqlGenerator sqlGenerator,
            SqlAstParser sqlAstParser,
            SqlValidator sqlValidator) {
        this.intentInterpreter = intentInterpreter;
        this.eligibilityPolicy = eligibilityPolicy;
        this.dataSourceRouter = dataSourceRouter;
        this.contextFactory = contextFactory;
        this.sqlGenerator = sqlGenerator;
        this.sqlAstParser = sqlAstParser;
        this.sqlValidator = sqlValidator;
    }

    public SqlGenerationResult generate(String question) {
        try {
            AnalysisIntent intent = intentInterpreter.interpret(question);

            if (intent.status() != AnalysisStatus.SUCCESS) {
                return terminalResult(intent);
            }

            SqlFallbackEligibility eligibility =
                    eligibilityPolicy.evaluate(intent.queryRequirements());

            if (!eligibility.eligible()) {
                return SqlGenerationResult.failure(
                        eligibility.status(), eligibility.message());
            }

            DataSourceRoute route = dataSourceRouter
                    .route(intent.queryRequirements())
                    .orElse(null);

            if (route == null) {
                return SqlGenerationResult.failure(
                        AnalysisStatus.UNSUPPORTED_ANALYSIS,
                        "No single datasource preserves the requested semantics");
            }

            SqlGenerationContext context = contextFactory.create(route);
            GeneratedSqlCandidate candidate =
                    sqlGenerator.generate(question, intent.queryRequirements(), context);
            ParsedSql parsedSql = sqlAstParser.parse(candidate);

            return SqlGenerationResult.success(sqlValidator.validate(parsedSql, context));

        } catch (LlmClientException exception) {
            log.warn("LLM processing failed during SQL generation with kind {}", exception.kind());

            if (exception.kind() == LlmClientException.Kind.UNAVAILABLE) {
                return SqlGenerationResult.failure(
                        AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                        "Analytics generation service is temporarily unavailable");
            }

            return generationFailure();

        } catch (InvalidIntentException exception) {
            log.warn("LLM returned an invalid analytics intent during SQL generation");
            return generationFailure();

        } catch (SqlParsingException | SqlValidationException exception) {
            log.warn("Generated SQL was rejected: {}", exception.getMessage());
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
