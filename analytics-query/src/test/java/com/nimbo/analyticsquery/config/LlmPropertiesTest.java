package com.nimbo.analyticsquery.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = LlmPropertiesTest.TestConfiguration.class)
@ActiveProfiles("test")
class LlmPropertiesTest {

  @Autowired private LlmProperties properties;

  @Test
  void givenTestProfileWhenPropertiesAreBoundThenLlmConfigurationMatches() {
    // Given: the test profile contains deterministic LLM configuration

    // When: Spring binds the LLM configuration properties
    LlmProperties actual = properties;

    // Then: all configured LLM values are bound correctly
    assertThat(actual.baseUrl()).isEqualTo(URI.create("http://127.0.0.1:65535"));
    assertThat(actual.apiKey()).isEqualTo("sk-test-key");
    assertThat(actual.model()).isEqualTo("test-model");
    assertThat(actual.chatCompletionsPath()).isEqualTo("/v1/chat/completions");
    assertThat(actual.timeout().connect()).isEqualTo(Duration.ofSeconds(1));
    assertThat(actual.timeout().read()).isEqualTo(Duration.ofSeconds(5));
  }

  @EnableConfigurationProperties(LlmProperties.class)
  static class TestConfiguration {}
}
