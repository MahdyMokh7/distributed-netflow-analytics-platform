package com.nimbo.analyticsquery.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "analytics-query.sql-generation")
public record SqlGenerationProperties(@Min(256) @Max(100_000) int maxSqlLength) {}
