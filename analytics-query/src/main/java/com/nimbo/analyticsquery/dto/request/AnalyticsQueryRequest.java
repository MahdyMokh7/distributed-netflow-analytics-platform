package com.nimbo.analyticsquery.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record AnalyticsQueryRequest(

        @Schema(
                description = "Natural-language analytics question",
                example = "Show the top 10 IPs by bandwidth during the last two hours")
        @NotBlank(message = "Question must not be blank")
        @Size(max = 2000, message = "Question must not exceed 2000 characters")
        String question) {}