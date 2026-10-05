package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.model.ExecutionMode;
import com.nimbo.analyticsquery.model.PlannerResult;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AnalyticsQueryResponse(
        AnalysisStatus status,
        ExecutionMode executionMode,
        AnalysisDataSource dataSource,
        List<Map<String, Object>> data,
        Map<String, Object> metadata,
        String explanation,
        String message) {

    public static AnalyticsQueryResponse from(
            PlannerResult plannerResult) {

        AnalyticsResult result = plannerResult.result();

        if (result == null) {
            return new AnalyticsQueryResponse(
                    plannerResult.status(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    plannerResult.message());
        }

        return new AnalyticsQueryResponse(
                plannerResult.status(),
                result.executionMode(),
                result.dataSource(),
                result.rows(),
                result.metadata().isEmpty()
                        ? null
                        : result.metadata(),
                plannerResult.explanation(),
                null);
    }
}
