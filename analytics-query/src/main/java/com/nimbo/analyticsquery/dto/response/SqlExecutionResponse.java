package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.model.PlannerResult;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SqlExecutionResponse(
        AnalysisStatus status,
        AnalysisDataSource dataSource,
        List<Map<String, Object>> data,
        int rowCount,
        String message) {

    public static SqlExecutionResponse from(
            PlannerResult plannerResult) {

        AnalyticsResult result = plannerResult.result();

        if (result == null) {
            return new SqlExecutionResponse(
                    plannerResult.status(),
                    null,
                    null,
                    0,
                    plannerResult.message());
        }

        return new SqlExecutionResponse(
                plannerResult.status(),
                result.dataSource(),
                result.rows(),
                Math.toIntExact(result.rowCount()),
                null);
    }
}
