package com.nimbo.analyticsquery.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.LlmProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiCompatibleLlmClientTest {

    private OpenAiCompatibleLlmClient client;

    @BeforeEach
    void setUp() {
        LlmProperties properties = new LlmProperties(
                URI.create("http://127.0.0.1:65535"),
                "sk-test-key",
                "test-model",
                "/v1/chat/completions",
                new LlmProperties.Timeout(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1)));

        client = new OpenAiCompatibleLlmClient(
                RestClient.builder()
                        .baseUrl(properties.baseUrl().toString())
                        .build(),
                properties,
                new ObjectMapper());
    }

    @Test
    void givenBlankSystemPromptWhenGeneratingThenRequestIsRejectedBeforeIo() {
        // Given: a blank system prompt
        String systemPrompt = " ";

        // When: structured generation is requested
        var assertion = assertThatThrownBy(() ->
                client.generateStructured(
                        systemPrompt,
                        "Show traffic.",
                        TestResult.class));

        // Then: invalid input is rejected without an HTTP request
        assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("systemPrompt must not be blank");
    }

    @Test
    void givenBlankUserPromptWhenGeneratingThenRequestIsRejectedBeforeIo() {
        // Given: a blank user prompt
        String userPrompt = "";

        // When: structured generation is requested
        var assertion = assertThatThrownBy(() ->
                client.generateStructured(
                        "Interpret the request.",
                        userPrompt,
                        TestResult.class));

        // Then: invalid input is rejected without an HTTP request
        assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("userPrompt must not be blank");
    }

    @Test
    void givenNullResponseTypeWhenGeneratingThenRequestIsRejectedBeforeIo() {
        // Given: no target structured response type
        Class<TestResult> responseType = null;

        // When: structured generation is requested
        var assertion = assertThatThrownBy(() ->
                client.generateStructured(
                        "Interpret the request.",
                        "Show traffic.",
                        responseType));

        // Then: missing response type is rejected without an HTTP request
        assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("responseType must not be null");
    }

    private record TestResult(String status) {
    }
}