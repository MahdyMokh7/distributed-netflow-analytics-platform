package com.nimbo.analyticsquery.config;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "analytics-query.intent")
public record IntentProperties(@NotNull Duration maxTimeRange) {

  public IntentProperties {
    if (maxTimeRange != null && (maxTimeRange.isZero() || maxTimeRange.isNegative())) {
      throw new IllegalArgumentException("maxTimeRange must be positive");
    }
  }
}
