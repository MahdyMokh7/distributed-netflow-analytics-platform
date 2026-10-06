package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalyticsResult;

public interface AnalyticsTool {

  String name();

  boolean supports(QueryRequirements requirements);

  AnalyticsResult execute(QueryRequirements requirements);

  default void requireSupported(QueryRequirements requirements) {

    if (!supports(requirements)) {
      throw new IllegalArgumentException(
          "Tool " + name() + " cannot satisfy the supplied query requirements");
    }
  }
}
