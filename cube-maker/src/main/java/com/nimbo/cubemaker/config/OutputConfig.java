package com.nimbo.cubemaker.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "output")
@Getter
@Setter
public class OutputConfig {

    private Mode mode = Mode.CONSOLE;

    public enum Mode {
        CONSOLE(true, false),
        DATABASE(false, true),
        BOTH(true, true),
        NONE(false, false);

        private final boolean consoleEnabled;
        private final boolean databaseEnabled;

        Mode(boolean consoleEnabled, boolean databaseEnabled) {
            this.consoleEnabled = consoleEnabled;
            this.databaseEnabled = databaseEnabled;
        }

        public boolean isConsoleEnabled() {
            return consoleEnabled;
        }

        public boolean isDatabaseEnabled() {
            return databaseEnabled;
        }
    }

    public boolean isConsoleMode() {
        return mode.isConsoleEnabled();
    }

    public boolean isDatabaseMode() {
        return mode.isDatabaseEnabled();
    }
}