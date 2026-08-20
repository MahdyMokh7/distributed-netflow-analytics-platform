package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "output")
@Getter
@Setter
public class OutputConfig {

    private String mode; // console or database

    public boolean isConsoleMode() {
        return "console".equalsIgnoreCase(mode);
    }

    public boolean isDatabaseMode() {
        return "database".equalsIgnoreCase(mode);
    }
}