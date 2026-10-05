package com.nimbo.analyticsquery.planner;

import org.springframework.stereotype.Service;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Dependency;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.sql.GeneratedSqlCandidate;
import com.nimbo.analyticsquery.sql.ParsedSql;
import com.nimbo.analyticsquery.sql.SqlAstParser;
import com.nimbo.analyticsquery.sql.SqlGenerationContext;
import com.nimbo.analyticsquery.sql.SqlGenerationContextFactory;
import com.nimbo.analyticsquery.sql.SqlGenerator;
import com.nimbo.analyticsquery.sql.SqlParsingException;
import com.nimbo.analyticsquery.sql.SqlRequirementValidator;
import com.nimbo.analyticsquery.sql.SqlValidationException;
import com.nimbo.analyticsquery.sql.execution.SqlExecutionService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SqlFallbackExecutionService {

    private final SqlGenerationContextFactory contextFactory;
    private final SqlGenerator sqlGenerator;
    private final SqlAstParser sqlAstParser;
    private final SqlExecutionService sqlExecutionService;
    private final SqlRequirementValidator sqlRequirementValidator;
    private final AnalyticsQueryMetrics metrics;

    public SqlFallbackExecutionService(
            SqlGenerationContextFactory contextFactory,
            SqlGenerator sqlGenerator,
            SqlAstParser sqlAstParser,
            SqlExecutionService sqlExecutionService,
            SqlRequirementValidator sqlRequirementValidator,
            AnalyticsQueryMetrics metrics) {
        this.contextFactory = contextFactory;
        this.sqlGenerator = sqlGenerator;
        this.sqlAstParser = sqlAstParser;
        this.sqlExecutionService = sqlExecutionService;
        this.sqlRequirementValidator = sqlRequirementValidator;
        this.metrics = metrics;
    }

    public PlannerResult execute(
            String question,
            QueryRequirements requirements,
            DataSourceRoute route) {
        try {
            SqlGenerationContext context = contextFactory.create(route);
            GeneratedSqlCandidate candidate = metrics.observeStage(
                    Stage.SQL_GENERATION,
                    () -> metrics.observeDependency(
                            Dependency.LLM,
                            () -> sqlGenerator.generate(question, requirements, context)));
            ParsedSql parsedSql = metrics.observeStage(
                    Stage.SQL_VALIDATION,
                    () -> sqlAstParser.parse(candidate));
            metrics.observeStage(
                    Stage.SQL_VALIDATION,
                    () -> {
                        sqlRequirementValidator.validate(parsedSql, requirements, context);
                        return parsedSql;
                    });

            return sqlExecutionService.execute(parsedSql, context);

        } catch (LlmClientException exception) {
            log.warn("LLM SQL generation failed with kind {}", exception.kind());
            metrics.recordFailure(
                    Stage.SQL_GENERATION,
                    exception.kind() == LlmClientException.Kind.UNAVAILABLE
                            ? FailureReason.DEPENDENCY_UNAVAILABLE
                            : FailureReason.INVALID_LLM_RESPONSE);

            AnalysisStatus status =
                    exception.kind() == LlmClientException.Kind.UNAVAILABLE
                            ? AnalysisStatus.DEPENDENCY_UNAVAILABLE
                            : AnalysisStatus.EXECUTION_FAILED;

            return PlannerResult.failure(
                    status,
                    status == AnalysisStatus.DEPENDENCY_UNAVAILABLE
                            ? "Analytics generation service is temporarily unavailable"
                            : "Analytics SQL generation failed");

        } catch (SqlParsingException exception) {
            log.warn("Generated SQL could not be parsed");
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_PARSE);
            return PlannerResult.failure(
                    AnalysisStatus.QUERY_REJECTED,
                    "Generated SQL did not satisfy the query safety policy");

        } catch (SqlValidationException exception) {
            log.warn("Generated SQL did not preserve validated requirements");
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_POLICY);
            return PlannerResult.failure(
                    AnalysisStatus.QUERY_REJECTED,
                    "Generated SQL did not satisfy the query safety policy");
        }
    }
}
