package com.nimbo.analyticsquery.intent.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

public record ClarificationRequirement(
    boolean required,
    @NotNull @Size(max = 10) List<MissingField> missingFields,
    @Size(max = 500) String question) {

  public enum MissingField {
    TIME_RANGE,
    DIMENSION,
    METRIC,
    FILTER_VALUE,
    IP_ADDRESS,
    IP_ROLE,
    TRAFFIC_DIRECTION,
    GROUPING,
    SORTING,
    LIMIT,
    QUERY_MEANING
  }

  public static ClarificationRequirement none() {
    return new ClarificationRequirement(false, List.of(), null);
  }

  public boolean isEmpty() {
    return !required && missingFields.isEmpty() && (question == null || question.isBlank());
  }
}
