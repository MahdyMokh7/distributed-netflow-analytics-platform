package com.nimbo.analyticsquery.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "analytics-query.query-plan-safety")
public record QueryPlanSafetyProperties(
    Duration explainTimeout,
    DataSize maxExplainSize,
    long maxEstimatedRows,
    double postgresMaxTotalCost) {

  public QueryPlanSafetyProperties {
    if (explainTimeout == null || explainTimeout.isZero() || explainTimeout.isNegative()) {

      throw new IllegalArgumentException("explainTimeout must be positive");
    }

    if (maxExplainSize == null || maxExplainSize.toBytes() <= 0) {

      throw new IllegalArgumentException("maxExplainSize must be positive");
    }

    if (maxEstimatedRows < 1) {
      throw new IllegalArgumentException("maxEstimatedRows must be positive");
    }

    if (!Double.isFinite(postgresMaxTotalCost) || postgresMaxTotalCost <= 0) {

      throw new IllegalArgumentException("postgresMaxTotalCost must be positive and finite");
    }
  }
}
