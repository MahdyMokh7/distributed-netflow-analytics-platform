package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BatchConfigTest {

    @Test
    void shouldStoreAndReturnConfiguredValues() {
        BatchConfig config = new BatchConfig();
        config.setSchedule("0 0 * * * ?");
        config.setWindowDuration("1 hour");
        config.setEnabled(true);

        assertThat(config.getSchedule()).isEqualTo("0 0 * * * ?");
        assertThat(config.getWindowDuration()).isEqualTo("1 hour");
        assertThat(config.isEnabled()).isTrue();
    }

    @Test
    void shouldDefaultEnabledToFalseWhenNotSet() {
        BatchConfig config = new BatchConfig();

        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getSchedule()).isNull();
        assertThat(config.getWindowDuration()).isNull();
    }
}