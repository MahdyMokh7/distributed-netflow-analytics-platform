package com.nimbo.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class HourWindowPolicyTest {

  @Test
  void shouldIncludeCurrentAndPreviousHourOnly() {
    Clock clock = Clock.fixed(Instant.parse("2026-09-18T11:30:00Z"), ZoneOffset.UTC);
    HourWindowPolicy policy = new HourWindowPolicy(1, true, clock);

    assertTrue(policy.includes(Instant.parse("2026-09-18T10:00:00Z").toEpochMilli()));
    assertTrue(policy.includes(Instant.parse("2026-09-18T11:59:59Z").toEpochMilli()));
    assertFalse(policy.includes(Instant.parse("2026-09-18T09:59:59Z").toEpochMilli()));
    assertFalse(policy.includes(Instant.parse("2026-09-18T12:00:00Z").toEpochMilli()));
    assertEquals(2, policy.maxOpenWriters());
  }

  @Test
  void shouldIncludeHistoricalRecordsWhenLateRoutingIsDisabled() {
    Clock clock = Clock.fixed(Instant.parse("2026-10-07T11:30:00Z"), ZoneOffset.UTC);
    HourWindowPolicy policy = new HourWindowPolicy(1, false, clock);

    assertTrue(policy.includes(Instant.parse("2026-10-03T11:30:00Z").toEpochMilli()));
    assertEquals(2, policy.maxOpenWriters());
  }
}
