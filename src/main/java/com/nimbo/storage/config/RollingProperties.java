package com.nimbo.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "rolling")
public class RollingProperties {

    private int maxRecordCount;
    private long maxSizeBytes;
    private Duration maxAge;

    public int getMaxRecordCount() {
        return maxRecordCount;
    }
    public void setMaxRecordCount(int maxRecordCount) {
        this.maxRecordCount = maxRecordCount;
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }
    public void setMaxSizeBytes(long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    public Duration getMaxAge() {
        return maxAge;
    }
    public void setMaxAge(Duration maxAge) {
        this.maxAge = maxAge;
    }
}