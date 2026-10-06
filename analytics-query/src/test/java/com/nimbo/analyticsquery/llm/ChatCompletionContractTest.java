package com.nimbo.analyticsquery.llm;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChatCompletionContractTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void givenStructuredChatRequestWhenSerializedThenOpenAiCompatibleShapeIsProduced()
      throws Exception {

    // Given: a structured chat-completion request
    ChatCompletionRequest request =
        new ChatCompletionRequest(
            "AliBaba/Qwen3.8-27B",
            List.of(
                new ChatMessage("system", "Interpret the request."),
                new ChatMessage("user", "Show traffic.")),
            0.0,
            new ChatCompletionRequest.ResponseFormat("json_object"));

    // When: the request is serialized to JSON
    JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(request));

    // Then: required OpenAI-compatible fields are serialized correctly
    assertThat(json.path("model").asText()).isEqualTo("AliBaba/Qwen3.8-27B");
    assertThat(json.path("messages").size()).isEqualTo(2);
    assertThat(json.path("temperature").asDouble()).isZero();
    assertThat(json.path("response_format").path("type").asText()).isEqualTo("json_object");
  }

  @Test
  void givenRealisticProviderResponseWhenDeserializedThenStructuredContentIsAvailable()
      throws Exception {

    // Given: an OpenAI-compatible response containing provider-specific fields
    String json =
        """
        {
          "id": "chatcmpl-test",
          "model": "AliBaba/Qwen3.8-27B",
          "choices": [
            {
              "index": 0,
              "finish_reason": "stop",
              "message": {
                "role": "assistant",
                "content": "{\\"status\\":\\"SUCCESS\\"}",
                "reasoning_content": "provider-specific reasoning",
                "provider_specific_fields": {
                  "reasoning": "ignored"
                }
              }
            }
          ],
          "usage": {
            "prompt_tokens": 20,
            "completion_tokens": 10
          }
        }
        """;

    // When: the provider response is deserialized
    ChatCompletionResponse response = objectMapper.readValue(json, ChatCompletionResponse.class);

    // Then: required content is preserved while extra fields are ignored
    assertThat(response.choices()).hasSize(1);
    assertThat(response.choices().get(0).finishReason()).isEqualTo("stop");
    assertThat(response.choices().get(0).message().role()).isEqualTo("assistant");
    assertThat(response.choices().get(0).message().content()).isEqualTo("{\"status\":\"SUCCESS\"}");
  }
}
