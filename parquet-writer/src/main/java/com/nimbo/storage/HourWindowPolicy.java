package com.nimbo.storage;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public final class HourWindowPolicy {

  private final int allowedLatenessHours;
  private final boolean lateRoutingEnabled;
  private final Clock clock;

  public HourWindowPolicy(int allowedLatenessHours, boolean lateRoutingEnabled, Clock clock) {

    if (allowedLatenessHours < 0 || allowedLatenessHours > 24) {
      throw new IllegalArgumentException("allowedLatenessHours must be between 0 and 24");
    }
    this.allowedLatenessHours = allowedLatenessHours;
    this.lateRoutingEnabled = lateRoutingEnabled;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public boolean includes(long kafkaTimestampMillis) {
    if (!lateRoutingEnabled) {
      return true;
    }

    Instant recordHour = hourOf(kafkaTimestampMillis);
    Instant currentHour = currentHour();
    return !recordHour.isBefore(currentHour.minus(allowedLatenessHours, ChronoUnit.HOURS))
        && !recordHour.isAfter(currentHour);
  }

  public int maxOpenWriters() {
    return allowedLatenessHours + 1;
  }

  private Instant currentHour() {
    return clock.instant().truncatedTo(ChronoUnit.HOURS);
  }

  static Instant hourOf(long timestampMillis) {
    return Instant.ofEpochMilli(timestampMillis).truncatedTo(ChronoUnit.HOURS);
  }
}
