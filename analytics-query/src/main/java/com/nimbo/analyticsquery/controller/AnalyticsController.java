package com.nimbo.analyticsquery.controller;

import com.nimbo.analyticsquery.dto.request.AnalyticsQueryRequest;
import com.nimbo.analyticsquery.dto.request.SqlExecutionRequest;
import com.nimbo.analyticsquery.dto.request.SqlGenerationRequest;
import com.nimbo.analyticsquery.dto.response.AnalyticsQueryResponse;
import com.nimbo.analyticsquery.dto.response.SqlExecutionResponse;
import com.nimbo.analyticsquery.dto.response.SqlGenerationResponse;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Endpoint;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.ExecutionMode;
import com.nimbo.analyticsquery.planner.AnalyticsQueryService;
import com.nimbo.analyticsquery.planner.SqlGenerationService;
import com.nimbo.analyticsquery.sql.execution.SqlExecutionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/analytics")
@Tag(name = "Analytics", description = "Natural-language analytics and controlled SQL query API")
public class AnalyticsController {

    private final AnalyticsQueryService analyticsQueryService;
    private final SqlGenerationService sqlGenerationService;
    private final SqlExecutionService sqlExecutionService;
    private final AnalyticsQueryMetrics metrics;

    public AnalyticsController(
            AnalyticsQueryService analyticsQueryService,
            SqlGenerationService sqlGenerationService,
            SqlExecutionService sqlExecutionService,
            AnalyticsQueryMetrics metrics) {
        this.analyticsQueryService = analyticsQueryService;
        this.sqlGenerationService = sqlGenerationService;
        this.sqlExecutionService = sqlExecutionService;
        this.metrics = metrics;
    }

    @PostMapping("/query")
    @Operation(summary = "Execute an analytics question",
            description = "Interprets a natural-language analytics question and executes it "
                    + "through an approved PostgreSQL cube tool. Unsupported requests "
                    + "return a controlled status."
    )
    public AnalyticsQueryResponse query(@Valid @RequestBody AnalyticsQueryRequest request) {
        long startedAt = System.nanoTime();
        AnalyticsQueryResponse response = metrics.observeRequest(
                Endpoint.QUERY,
                () -> AnalyticsQueryResponse.from(
                        analyticsQueryService.query(request.question())),
                AnalyticsQueryResponse::status);

        if (response.status() == AnalysisStatus.SUCCESS) {
            metrics.recordExecution(response.executionMode(), response.dataSource());
        }

        log.info(
                "Analytics request completed endpoint={} status={} executionMode={} "
                        + "datasource={} rowCount={} durationMs={}",
                Endpoint.QUERY,
                response.status(),
                response.executionMode(),
                response.dataSource(),
                response.data() == null ? 0 : response.data().size(),
                elapsedMillis(startedAt));

        return response;
    }

    @PostMapping("/sql/generate")
    @Operation(summary = "Generate validated SQL",
            description = "Generates and validates SQL for a natural-language analytics question "
                    + "without executing the query."
    )
    public SqlGenerationResponse generateSql(@Valid @RequestBody SqlGenerationRequest request) {
        long startedAt = System.nanoTime();
        SqlGenerationResponse response = metrics.observeRequest(
                Endpoint.SQL_GENERATE,
                () -> SqlGenerationResponse.from(
                        sqlGenerationService.generate(request.question())),
                SqlGenerationResponse::status);

        log.info(
                "Analytics request completed endpoint={} status={} datasource={} durationMs={}",
                Endpoint.SQL_GENERATE,
                response.status(),
                response.dataSource(),
                elapsedMillis(startedAt));

        return response;
    }

    @PostMapping("/sql/execute")
    @Operation(summary = "Validate and execute SQL",
            description = "Validates the supplied SQL against the analytics query security policy "
                    + "before read-only execution."
    )
    public SqlExecutionResponse executeSql(@Valid @RequestBody SqlExecutionRequest request) {
        long startedAt = System.nanoTime();
        SqlExecutionResponse response = metrics.observeRequest(
                Endpoint.SQL_EXECUTE,
                () -> SqlExecutionResponse.from(
                        sqlExecutionService.execute(request.sql())),
                SqlExecutionResponse::status);

        if (response.status() == AnalysisStatus.SUCCESS) {
            metrics.recordExecution(
                    ExecutionMode.GENERATED_SQL,
                    response.dataSource());
        }

        log.info(
                "Analytics request completed endpoint={} status={} executionMode={} "
                        + "datasource={} rowCount={} durationMs={}",
                Endpoint.SQL_EXECUTE,
                response.status(),
                ExecutionMode.GENERATED_SQL,
                response.dataSource(),
                response.rowCount(),
                elapsedMillis(startedAt));

        return response;
    }

    private static long elapsedMillis(long startedAt) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - startedAt);
    }
}
