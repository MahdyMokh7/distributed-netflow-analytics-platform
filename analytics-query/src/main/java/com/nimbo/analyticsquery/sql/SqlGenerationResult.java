package com.nimbo.analyticsquery.sql;

import java.util.Objects;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;

public record SqlGenerationResult(
        AnalysisStatus status,
        AnalysisDataSource dataSource,
        String sql,
        String message) {

    public SqlGenerationResult {
        Objects.requireNonNull(status, "status must not be null");

        if (status == AnalysisStatus.SUCCESS) {
            Objects.requireNonNull(dataSource, "successful result must contain a datasource");
            if (sql == null || sql.isBlank()) {
                throw new IllegalArgumentException("successful result must contain SQL");
            }
            if (message != null) {
                throw new IllegalArgumentException("successful result must not contain a message");
            }
        } else if (dataSource != null || sql != null || message == null || message.isBlank()) {
            throw new IllegalArgumentException(
                    "failed result must contain only a non-blank message");
        }
    }

    public static SqlGenerationResult success(ParsedSql parsedSql) {
        return new SqlGenerationResult(
                AnalysisStatus.SUCCESS,
                parsedSql.dataSource(),
                parsedSql.sql(),
                null);
    }

    public static SqlGenerationResult failure(AnalysisStatus status, String message) {
        return new SqlGenerationResult(status, null, null, message);
    }
}
