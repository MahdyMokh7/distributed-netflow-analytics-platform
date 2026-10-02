package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SqlExecutionResponse(
        AnalysisStatus status,
        AnalysisDataSource dataSource,
        List<Map<String, Object>> data,
        int rowCount,
        String message) {}