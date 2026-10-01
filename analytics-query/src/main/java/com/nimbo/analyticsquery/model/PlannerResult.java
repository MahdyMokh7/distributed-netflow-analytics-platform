package com.nimbo.analyticsquery.model;

import java.util.Objects;

public record PlannerResult(
        AnalysisStatus status,
        AnalyticsResult result,
        String message
) {

    public PlannerResult {
        Objects.requireNonNull(status, "status must not be null");

        if (status == AnalysisStatus.SUCCESS) {
            Objects.requireNonNull(result, "successful result must not be null");
            if (message != null) {
                throw new IllegalArgumentException("successful result must not contain a failure message");
            }
        } else {
            if (result != null) {
                throw new IllegalArgumentException("failed result must not contain analytics data");
            }
            if (message == null || message.isBlank()) {
                throw new IllegalArgumentException("failed result must contain a message");
            }
        }
    }

    public static PlannerResult success(AnalyticsResult result) {

        return new PlannerResult(AnalysisStatus.SUCCESS, result, null);
    }


    public static PlannerResult failure(AnalysisStatus status, String message) {

        return new PlannerResult(status, null, message);
    }
}
