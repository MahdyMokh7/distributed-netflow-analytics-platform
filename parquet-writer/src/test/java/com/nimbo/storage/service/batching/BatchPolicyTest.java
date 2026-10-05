package com.nimbo.storage.service.batching;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchPolicyTest {

    private static final Instant BASE_TIME = Instant.parse("2026-08-11T10:00:00Z");

    private final BatchPolicy policy = new BatchPolicy(100, 1_000, Duration.ofMinutes(2));

    @Test
    void shouldNotFlushEmptyBatch() {
        boolean result = policy.shouldFlush(0, 0, null, BASE_TIME);
        assertFalse(result);
    }

    @Test
    void shouldFlushWhenRecordCountReachesLimit() {
        boolean result = policy.shouldFlush(100, 500, BASE_TIME, BASE_TIME);
        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenRecordCountIsBelowLimit() {
        boolean result = policy.shouldFlush(99, 500, BASE_TIME, BASE_TIME);
        assertFalse(result);
    }

    @Test
    void shouldFlushWhenSizeReachesLimit() {
        boolean result = policy.shouldFlush(50, 1_000, BASE_TIME, BASE_TIME);
        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenSizeIsBelowLimit() {
        boolean result = policy.shouldFlush(50, 999, BASE_TIME, BASE_TIME);
        assertFalse(result);
    }

    @Test
    void shouldFlushWhenBatchAgeReachesLimit() {
        Instant now = BASE_TIME.plus(Duration.ofMinutes(2));
        boolean result = policy.shouldFlush(50, 500, BASE_TIME, now);

        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenBatchAgeIsBelowLimit() {
        Instant now = BASE_TIME.plus(Duration.ofMinutes(2)).minusMillis(1);
        boolean result = policy.shouldFlush(50, 500, BASE_TIME, now);

        assertFalse(result);
    }

    @Test
    void shouldNotFlushWhenFirstRecordTimeIsNull() {
        boolean result = policy.shouldFlush(50, 500, null, BASE_TIME);

        assertFalse(result);
    }

    @Test
    void shouldRejectNullNow() {
        assertThrows(NullPointerException.class, () -> policy.shouldFlush(50, 500, BASE_TIME, null));
    }

    @Test
    void shouldRejectZeroMaxRecordCount() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(0, 1_000, Duration.ofMinutes(2)));
    }

    @Test
    void shouldRejectNegativeMaxRecordCount() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(-1, 1_000, Duration.ofMinutes(2)));
    }

    @Test
    void shouldRejectZeroMaxSize() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(100, 0, Duration.ofMinutes(2)));
    }

    @Test
    void shouldRejectNegativeMaxSize() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(100, -1, Duration.ofMinutes(2)));
    }

    @Test
    void shouldRejectNullBatchAge() {
        assertThrows(NullPointerException.class, () -> new BatchPolicy(100, 1_000, null));
    }

    @Test
    void shouldRejectZeroBatchAge() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(100, 1_000, Duration.ZERO));
    }

    @Test
    void shouldRejectNegativeBatchAge() {
        assertThrows(IllegalArgumentException.class, () -> new BatchPolicy(100, 1_000, Duration.ofSeconds(-1)));
    }
}
