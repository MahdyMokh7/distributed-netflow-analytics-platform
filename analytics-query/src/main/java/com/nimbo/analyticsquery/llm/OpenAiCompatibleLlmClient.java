package com.nimbo.analyticsquery.llm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.LlmProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;

@Component
public class OpenAiCompatibleLlmClient implements LlmClient {

    private static final String SYSTEM_ROLE = "system";
    private static final String USER_ROLE = "user";
    private static final String JSON_OBJECT = "json_object";

    private final RestClient restClient;
    private final LlmProperties properties;
    private final ObjectMapper objectMapper;

    public OpenAiCompatibleLlmClient(
            @Qualifier("llmRestClient") RestClient restClient,
            LlmProperties properties,
            ObjectMapper objectMapper) {

        this.restClient = restClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> T generateStructured(String systemPrompt, String userPrompt, Class<T> responseType) {

        validatePrompt(systemPrompt, "systemPrompt");
        validatePrompt(userPrompt, "userPrompt");

        if (responseType == null) {
            throw new IllegalArgumentException("responseType must not be null");
        }

        ChatCompletionRequest request = new ChatCompletionRequest(
                properties.model(),
                List.of(
                        new ChatMessage(SYSTEM_ROLE, systemPrompt),
                        new ChatMessage(USER_ROLE, userPrompt)
                ),
                0.0,
                new ChatCompletionRequest.ResponseFormat(JSON_OBJECT)
        );

        ChatCompletionResponse response = execute(request);

        String content = extractContent(response);

        try {
            return objectMapper
                    .readerFor(responseType)
                    .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readValue(content);
        } catch (JsonProcessingException exception) {
            throw new LlmClientException(
                    LlmClientException.Kind.INVALID_RESPONSE,
                    "LLM returned invalid structured JSON",
                    exception);
        }
    }

    private ChatCompletionResponse execute(ChatCompletionRequest request) {
        try {
            ChatCompletionResponse response = restClient
                    .post()
                    .uri(properties.chatCompletionsPath())
                    .body(request)
                    .retrieve()
                    .onStatus(
                            HttpStatusCode::isError,
                            (httpRequest, httpResponse) -> {
                                throw new LlmClientException(
                                        LlmClientException.Kind.UNAVAILABLE,
                                        "LLM request failed with HTTP status "
                                                + httpResponse.getStatusCode().value());
                            })
                    .body(ChatCompletionResponse.class);

            if (response == null) {
                throw invalidResponse("LLM returned an empty HTTP response");
            }

            return response;

        } catch (LlmClientException exception) {
            throw exception;

        } catch (ResourceAccessException exception) {
            throw new LlmClientException(
                    LlmClientException.Kind.UNAVAILABLE,
                    "LLM request timed out or the service is unreachable", exception
            );

        } catch (RestClientException exception) {
            throw new LlmClientException(
                    LlmClientException.Kind.INVALID_RESPONSE,
                    "LLM response could not be processed",
                    exception);
        }
    }

    private String extractContent(ChatCompletionResponse response) {
        if (response.choices() == null || response.choices().isEmpty()) {
            throw invalidResponse("LLM response contains no choices");
        }

        ChatCompletionResponse.Choice choice = response.choices().get(0);
        validateFinishReason(choice.finishReason());

        ChatCompletionResponse.Message message = choice.message();

        if (message == null || message.content() == null || message.content().isBlank()) {
            throw invalidResponse("LLM response contains no message content");
        }

        return message.content();
    }

    private void validateFinishReason(String finishReason) {
        if (finishReason != null
                && !finishReason.isBlank()
                && !"stop".equals(finishReason)) {

            throw invalidResponse(
                    "LLM response did not complete normally: " + finishReason);
        }
    }

    private LlmClientException invalidResponse(String message) {
        return new LlmClientException(
                LlmClientException.Kind.INVALID_RESPONSE,
                message);
    }

    private void validatePrompt(String prompt, String name) {
        if (prompt == null || prompt.isBlank()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
    }
}
