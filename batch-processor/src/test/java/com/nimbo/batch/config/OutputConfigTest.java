package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutputConfigTest {

    @Test
    void shouldDefaultToConsoleMode() {
        OutputConfig config = new OutputConfig();

        assertThat(config.getMode()).isEqualTo(OutputConfig.Mode.CONSOLE);
        assertThat(config.isConsoleMode()).isTrue();
        assertThat(config.isDatabaseMode()).isFalse();
    }

    @Test
    void consoleModeShouldEnableConsoleOnly() {
        assertThat(OutputConfig.Mode.CONSOLE.isConsoleEnabled()).isTrue();
        assertThat(OutputConfig.Mode.CONSOLE.isDatabaseEnabled()).isFalse();
    }

    @Test
    void databaseModeShouldEnableDatabaseOnly() {
        OutputConfig config = new OutputConfig();
        config.setMode(OutputConfig.Mode.DATABASE);

        assertThat(config.isConsoleMode()).isFalse();
        assertThat(config.isDatabaseMode()).isTrue();
    }

    @Test
    void bothModeShouldEnableConsoleAndDatabase() {
        OutputConfig config = new OutputConfig();
        config.setMode(OutputConfig.Mode.BOTH);

        assertThat(config.isConsoleMode()).isTrue();
        assertThat(config.isDatabaseMode()).isTrue();
    }

    @Test
    void noneModeShouldDisableBothOutputs() {
        OutputConfig config = new OutputConfig();
        config.setMode(OutputConfig.Mode.NONE);

        assertThat(config.isConsoleMode()).isFalse();
        assertThat(config.isDatabaseMode()).isFalse();
    }
}