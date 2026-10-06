package com.nimbo.analyticsquery.planner;

import com.nimbo.analyticsquery.model.AnalysisStatus;
import java.util.Objects;

public record SqlFallbackEligibility(boolean eligible, AnalysisStatus status, String message) {

  public SqlFallbackEligibility {
    Objects.requireNonNull(status, "status must not be null");

    if (eligible) {
      if (status != AnalysisStatus.SUCCESS) {
        throw new IllegalArgumentException("Eligible SQL fallback must have SUCCESS status");
      }

      if (message != null) {
        throw new IllegalArgumentException("Eligible SQL fallback must not contain a message");
      }
    } else {
      if (status == AnalysisStatus.SUCCESS) {
        throw new IllegalArgumentException("Ineligible SQL fallback cannot have SUCCESS status");
      }

      if (message == null || message.isBlank()) {
        throw new IllegalArgumentException("Ineligible SQL fallback must contain a message");
      }
    }
  }

  public static SqlFallbackEligibility allow() {
    return new SqlFallbackEligibility(true, AnalysisStatus.SUCCESS, null);
  }

  public static SqlFallbackEligibility deny(AnalysisStatus status, String message) {

    return new SqlFallbackEligibility(false, status, message);
  }
}
