package com.nimbo.analyticsquery.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "analytics-query.intent")
public record IntentProperties(
        @NotNull Duration maxTimeRange) {

    public IntentProperties {
        if (maxTimeRange != null
                && (maxTimeRange.isZero() || maxTimeRange.isNegative())) {
            throw new IllegalArgumentException(
                    "maxTimeRange must be positive");
        }
    }
}
