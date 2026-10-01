package com.nimbo.analyticsquery.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "llm")
public record LlmProperties(
        @NotNull URI baseUrl,
        @NotBlank String apiKey,
        @NotBlank String model,
        @NotBlank String chatCompletionsPath,
        @Valid @NotNull Timeout timeout) {

    public record Timeout(
            @NotNull Duration connect,
            @NotNull Duration read
    ) {}
}