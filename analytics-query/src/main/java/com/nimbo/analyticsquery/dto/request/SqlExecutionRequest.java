package com.nimbo.analyticsquery.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SqlExecutionRequest(
    @Schema(
            description =
                "SQL query to validate and execute through the protected analytics query path",
            example =
                "SELECT ip, SUM(inbound_bytes + outbound_bytes) AS total_bytes "
                    + "FROM ip_bandwidth_aggregates "
                    + "WHERE window_start >= '2026-09-30 08:00:00' "
                    + "GROUP BY ip ORDER BY total_bytes DESC LIMIT 10")
        @NotBlank(message = "SQL must not be blank")
        @Size(max = 20_000, message = "SQL must not exceed 20000 characters")
        String sql) {}
