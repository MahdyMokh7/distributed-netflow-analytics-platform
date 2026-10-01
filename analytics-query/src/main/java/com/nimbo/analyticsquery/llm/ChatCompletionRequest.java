package com.nimbo.analyticsquery.llm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ChatCompletionRequest(
        String model,
        List<ChatMessage> messages,
        double temperature,
        @JsonProperty("response_format") ResponseFormat responseFormat
) {

    public record ResponseFormat(String type) {}
}