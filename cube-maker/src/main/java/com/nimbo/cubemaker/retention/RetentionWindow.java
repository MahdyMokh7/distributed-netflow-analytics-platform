package com.nimbo.cubemaker.retention;

import java.time.Instant;
import java.time.LocalDate;

public record RetentionWindow(LocalDate cutoffDate, Instant cutoffInstant) {

  public static RetentionWindow unbounded() {
    return new RetentionWindow(LocalDate.MIN, Instant.MIN);
  }

  public boolean retains(Instant timestamp) {
    return !timestamp.isBefore(cutoffInstant);
  }

  public boolean isBounded() {
    return !Instant.MIN.equals(cutoffInstant);
  }
}
