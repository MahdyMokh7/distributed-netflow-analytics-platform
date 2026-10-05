package com.nimbo.analyticsquery.model;

import java.util.Objects;

public record PlannerResult(
        AnalysisStatus status,
        AnalyticsResult result,
        String explanation,
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
            if (explanation != null) {
                throw new IllegalArgumentException("failed result must not contain an explanation");
            }
        }
    }

    public static PlannerResult success(AnalyticsResult result) {

        return new PlannerResult(AnalysisStatus.SUCCESS, result, null, null);
    }

    public PlannerResult withExplanation(String explanation) {
        if (status != AnalysisStatus.SUCCESS) {
            return this;
        }

        return new PlannerResult(status, result, explanation, null);
    }

    public static PlannerResult failure(AnalysisStatus status, String message) {

        return new PlannerResult(status, null, null, message);
    }
}
