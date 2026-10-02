package com.nimbo.analyticsquery.llm;

public interface LlmClient {

    <T> T generateStructured(
            String systemPrompt,
            String userPrompt,
            Class<T> responseType);
}