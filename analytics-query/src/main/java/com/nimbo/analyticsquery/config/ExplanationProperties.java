package com.nimbo.analyticsquery.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "analytics-query.explanation")
public record ExplanationProperties(
    @NotNull Duration timeout, @Min(1) @Max(100) int maxRows, @Positive int maxPayloadCharacters) {

  public ExplanationProperties {
    if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
      throw new IllegalArgumentException("Explanation timeout must be positive");
    }
  }
}
