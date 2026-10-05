package com.nimbo.analyticsquery.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "analytics-query.sql-execution")
public record SqlExecutionProperties(
        Duration statementTimeout,
        DataSize maxResultSize,
        JdbcTarget postgresql,
        JdbcTarget starrocks) {

    public SqlExecutionProperties {
        requirePositive(statementTimeout, "statementTimeout");

        if (maxResultSize == null || maxResultSize.toBytes() <= 0) {
            throw new IllegalArgumentException("maxResultSize must be positive");
        }

        if (postgresql == null) {
            throw new IllegalArgumentException("postgresql configuration must not be null");
        }

        if (starrocks == null) {
            throw new IllegalArgumentException("starrocks configuration must not be null");
        }
    }

    public record JdbcTarget(
            boolean enabled,
            String jdbcUrl,
            String username,
            String password,
            int poolSize,
            Duration connectionTimeout) {

        public JdbcTarget {
            if (poolSize < 1) {
                throw new IllegalArgumentException("poolSize must be positive");
            }

            requirePositive(connectionTimeout, "connectionTimeout");

            if (enabled) {
                requireText(jdbcUrl, "jdbcUrl");
                requireText(username, "username");

                if (password == null) {
                    throw new IllegalArgumentException("password must not be null");
                }
            }
        }
    }

    private static void requirePositive(
            Duration value,
            String field) {

        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireText(
            String value,
            String field) {

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}