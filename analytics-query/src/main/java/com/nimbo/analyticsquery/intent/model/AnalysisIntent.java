package com.nimbo.analyticsquery.intent.model;

import com.nimbo.analyticsquery.model.AnalysisStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record AnalysisIntent(

        @NotNull
        AnalysisStatus status,

        @NotNull
        @Valid
        QueryRequirements queryRequirements,

        @NotNull
        @Valid
        ClarificationRequirement clarification) {
}