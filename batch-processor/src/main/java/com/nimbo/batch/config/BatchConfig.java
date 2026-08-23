package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "batch")
@Getter
@Setter
public class BatchConfig {

    private String schedule;
    private String windowDuration;
    private boolean enabled;
}