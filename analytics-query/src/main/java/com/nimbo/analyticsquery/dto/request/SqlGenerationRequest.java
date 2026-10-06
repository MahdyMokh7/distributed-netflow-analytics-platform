package com.nimbo.analyticsquery.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SqlGenerationRequest(
    @Schema(
            description = "Natural-language analytics question to convert into validated SQL",
            example = "Which 10 source IPs contacted the most destination ports in the last hour?")
        @NotBlank(message = "Question must not be blank")
        @Size(max = 2000, message = "Question must not exceed 2000 characters")
        String question) {}
