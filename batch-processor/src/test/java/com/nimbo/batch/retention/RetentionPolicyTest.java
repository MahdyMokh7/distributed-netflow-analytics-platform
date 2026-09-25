package com.nimbo.batch.retention;

import com.nimbo.batch.config.BatchConfig;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class RetentionPolicyTest {

    @Test
    void givenMiddayUtcAndThreePreviousDays_whenWindowIsCalculated_thenCutoffIsUtcMidnightThreeDaysAgo() {
        // Given: retention enabled at midday UTC with three previous calendar days
        BatchConfig config = new BatchConfig();
        config.getRetention().setEnabled(true);
        config.getRetention().setPreviousDays(3);
        Clock clock = Clock.fixed(Instant.parse("2026-09-25T15:45:00Z"), ZoneOffset.UTC);

        // When: the retention window is calculated
        RetentionWindow window = new RetentionPolicy(config, clock).currentWindow();

        // Then: today and the previous three UTC dates are retained
        assertThat(window.cutoffDate()).hasToString("2026-09-22");
        assertThat(window.cutoffInstant()).isEqualTo(Instant.parse("2026-09-22T00:00:00Z"));
        assertThat(window.retains(Instant.parse("2026-09-22T00:00:00Z"))).isTrue();
        assertThat(window.retains(Instant.parse("2026-09-21T23:59:59.999Z"))).isFalse();
    }

    @Test
    void givenRetentionDisabled_whenWindowIsCalculated_thenNoTimestampExpires() {
        // Given: retention disabled for a test environment
        BatchConfig config = new BatchConfig();
        config.getRetention().setEnabled(false);

        // When: the retention window is calculated
        RetentionWindow window = new RetentionPolicy(
                config, Clock.fixed(Instant.parse("2026-09-25T15:45:00Z"), ZoneOffset.UTC))
                .currentWindow();

        // Then: even the oldest representable timestamp remains eligible
        assertThat(window.isBounded()).isFalse();
        assertThat(window.retains(Instant.MIN)).isTrue();
    }
}
