package com.nimbo.analyticsquery.llm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChatCompletionResponse(List<Choice> choices) {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Choice(
      int index, Message message, @JsonProperty("finish_reason") String finishReason) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Message(String role, String content) {}
}
