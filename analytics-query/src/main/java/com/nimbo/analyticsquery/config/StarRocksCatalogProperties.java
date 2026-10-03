package com.nimbo.analyticsquery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "analytics-query.schema.starrocks")
public record StarRocksCatalogProperties(
        boolean enabled,
        String jdbcUrl,
        String username,
        String password,
        String database,
        String table,
        Duration refreshInterval,
        Duration connectionTimeout,
        int poolSize,
        int queryTimeoutSeconds) {

    public StarRocksCatalogProperties {
        if (enabled) {
            requireText(jdbcUrl, "jdbcUrl");
            requireText(username, "username");
            requireText(database, "database");
            requireText(table, "table");

            if (password == null) {
                throw new IllegalArgumentException("password must not be null");
            }

            if (refreshInterval == null || refreshInterval.isZero() || refreshInterval.isNegative()) {
                throw new IllegalArgumentException("refreshInterval must be positive");
            }

            if (connectionTimeout == null || connectionTimeout.isZero() || connectionTimeout.isNegative()) {
                throw new IllegalArgumentException("connectionTimeout must be positive");
            }

            if (poolSize < 1 || queryTimeoutSeconds < 1) {
                throw new IllegalArgumentException("poolSize and queryTimeoutSeconds must be positive");
            }
        }
    }

    private static void requireText(String value, String field) {

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}