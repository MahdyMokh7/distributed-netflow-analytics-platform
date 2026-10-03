package com.nimbo.analyticsquery.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.sql.SqlGenerationResult;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SqlGenerationResponse(
        AnalysisStatus status,
        AnalysisDataSource dataSource,
        String sql,
        String message) {

    public static SqlGenerationResponse from(SqlGenerationResult result) {
        return new SqlGenerationResponse(
                result.status(), result.dataSource(), result.sql(), result.message());
    }
}
