package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SqlGenerationResponse(
        AnalysisStatus status,
        AnalysisDataSource dataSource,
        String sql,
        String message) {}