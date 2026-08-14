package com.nimbo.storage.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Setter
@Getter
@ConfigurationProperties(prefix = "rolling")
public class RollingProperties {

    private int maxRecordCount;
    private long maxSizeBytes;
    private Duration maxAge;

}