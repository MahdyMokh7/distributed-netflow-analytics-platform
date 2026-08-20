package com.nimbo.storage.service.batching;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class BatchPolicyTest {

    private final BatchPolicy policy =
            new BatchPolicy(
                    100,
                    1_000,
                    Duration.ofMinutes(2)
            );

    @Test
    void shouldNotFlushEmptyBatch() {
        boolean result = policy.shouldFlush(
                0,
                0,
                null,
                Instant.now()
        );

        assertFalse(result);
    }

    @Test
    void shouldFlushWhenRecordCountReachesLimit() {
        boolean result = policy.shouldFlush(
                100,
                500,
                Instant.now(),
                Instant.now()
        );

        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenRecordCountIsBelowLimit() {
        boolean result = policy.shouldFlush(
                99,
                500,
                Instant.now(),
                Instant.now()
        );

        assertFalse(result);
    }

    @Test
    void shouldFlushWhenSizeReachesLimit() {
        boolean result = policy.shouldFlush(
                50,
                1_000,
                Instant.now(),
                Instant.now()
        );

        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenSizeIsBelowLimit() {
        boolean result = policy.shouldFlush(
                50,
                999,
                Instant.now(),
                Instant.now()
        );

        assertFalse(result);
    }

    @Test
    void shouldFlushWhenBatchAgeReachesLimit() {
        Instant firstRecordAt = Instant.parse("2026-08-11T10:00:00Z");
        Instant now = Instant.parse("2026-08-11T10:02:00Z");

        boolean result = policy.shouldFlush(
                50,
                500,
                firstRecordAt,
                now
        );

        assertTrue(result);
    }

    @Test
    void shouldNotFlushWhenBatchAgeIsBelowLimit() {
        Instant firstRecordAt = Instant.parse("2026-08-11T10:00:00Z");
        Instant now = Instant.parse("2026-08-11T10:01:59Z");

        boolean result = policy.shouldFlush(
                50,
                500,
                firstRecordAt,
                now
        );

        assertFalse(result);
    }

    @Test
    void shouldNotFlushWhenFirstRecordTimeIsNull() {
        boolean result = policy.shouldFlush(
                50,
                500,
                null,
                Instant.now()
        );

        assertFalse(result);
    }

    @Test
    void shouldRejectNonPositiveRecordCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BatchPolicy(
                        0,
                        1_000,
                        Duration.ofMinutes(2)
                )
        );
    }

    @Test
    void shouldRejectNonPositiveSize() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BatchPolicy(
                        100,
                        0,
                        Duration.ofMinutes(2)
                )
        );
    }

    @Test
    void shouldRejectNullBatchAge() {
        assertThrows(
                NullPointerException.class,
                () -> new BatchPolicy(
                        100,
                        1_000,
                        null
                )
        );
    }

    @Test
    void shouldRejectNonPositiveBatchAge() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BatchPolicy(
                        100,
                        1_000,
                        Duration.ZERO
                )
        );
    }
}

