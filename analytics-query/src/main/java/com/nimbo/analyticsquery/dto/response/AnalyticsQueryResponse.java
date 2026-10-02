package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.ExecutionMode;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AnalyticsQueryResponse(
        AnalysisStatus status,
        ExecutionMode executionMode,
        AnalysisDataSource dataSource,
        List<Map<String, Object>> data,
        String explanation,
        String message) {}