package com.nimbo.analyticsquery.controller;

import com.nimbo.analyticsquery.dto.request.AnalyticsQueryRequest;
import com.nimbo.analyticsquery.dto.request.SqlExecutionRequest;
import com.nimbo.analyticsquery.dto.request.SqlGenerationRequest;
import com.nimbo.analyticsquery.dto.response.AnalyticsQueryResponse;
import com.nimbo.analyticsquery.dto.response.SqlExecutionResponse;
import com.nimbo.analyticsquery.dto.response.SqlGenerationResponse;
import com.nimbo.analyticsquery.exception.FeatureNotImplementedException;
import com.nimbo.analyticsquery.planner.AnalyticsQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/analytics")
@Tag(name = "Analytics", description = "Natural-language analytics and controlled SQL query API")
public class AnalyticsController {

    private final AnalyticsQueryService analyticsQueryService;

    public AnalyticsController(AnalyticsQueryService analyticsQueryService) {
        this.analyticsQueryService = analyticsQueryService;
    }

    @PostMapping("/query")
    @Operation(summary = "Execute an analytics question",
            description = "Interprets a natural-language analytics question and executes it "
                    + "through an approved PostgreSQL cube tool. Unsupported requests "
                    + "return a controlled status."
    )
    public AnalyticsQueryResponse query(@Valid @RequestBody AnalyticsQueryRequest request) {
        return AnalyticsQueryResponse.from(analyticsQueryService.query(request.question()));
    }

    @PostMapping("/sql/generate")
    @Operation(summary = "Generate validated SQL",
            description = "Generates and validates SQL for a natural-language analytics question "
                    + "without executing the query."
    )
    public SqlGenerationResponse generateSql(@Valid @RequestBody SqlGenerationRequest request) {

        throw new FeatureNotImplementedException("SQL generation is not implemented yet");
    }

    @PostMapping("/sql/execute")
    @Operation(summary = "Validate and execute SQL",
            description = "Validates the supplied SQL against the analytics query security policy "
                    + "before read-only execution."
    )
    public SqlExecutionResponse executeSql(@Valid @RequestBody SqlExecutionRequest request) {

        throw new FeatureNotImplementedException("SQL execution is not implemented yet");
    }
}
