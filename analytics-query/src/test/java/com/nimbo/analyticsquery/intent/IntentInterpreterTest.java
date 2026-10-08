package com.nimbo.analyticsquery.intent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.IntentProperties;
import com.nimbo.analyticsquery.config.LlmProperties;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.intent.model.ClarificationRequirement;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.llm.LlmClient;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.llm.OpenAiCompatibleLlmClient;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

class IntentInterpreterTest {

  private static ValidatorFactory validatorFactory;
  private static IntentInterpreter interpreter;

  @BeforeAll
  static void setUpInterpreter() {
    LlmProperties properties =
        new LlmProperties(
            URI.create("http://127.0.0.1:65535"),
            "sk-test-key",
            "test-model",
            "/v1/chat/completions",
            new LlmProperties.Timeout(Duration.ofSeconds(1), Duration.ofSeconds(1)));

    CuratedSemanticSchemaCatalog catalog = new CuratedSemanticSchemaCatalog(java.util.List.of());

    validatorFactory = Validation.buildDefaultValidatorFactory();

    interpreter =
        new IntentInterpreter(
            new OpenAiCompatibleLlmClient(
                RestClient.builder().baseUrl(properties.baseUrl().toString()).build(),
                properties,
                new ObjectMapper()),
            new IntentPromptFactory(Clock.systemUTC(), catalog),
            new IntentValidator(
                validatorFactory.getValidator(),
                catalog,
                Clock.systemUTC(),
                new IntentProperties(Duration.ofDays(7))));
  }

  @AfterAll
  static void closeValidatorFactory() {
    validatorFactory.close();
  }

  @Test
  void givenBlankQuestionWhenInterpretedThenQuestionIsRejectedBeforeIo() {
    // Given: a blank natural-language question
    String question = " ";

    // When: the question is interpreted
    var assertion = assertThatThrownBy(() -> interpreter.interpret(question));

    // Then: input validation rejects it before contacting the LLM
    assertion.isInstanceOf(IllegalArgumentException.class).hasMessage("question must not be blank");
  }

  @Test
  void givenOversizedQuestionWhenInterpretedThenQuestionIsRejectedBeforeIo() {
    // Given: a question larger than the accepted input boundary
    String question = "a".repeat(2_001);

    // When: the question is interpreted
    var assertion = assertThatThrownBy(() -> interpreter.interpret(question));

    // Then: input validation rejects it before contacting the LLM
    assertion
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("question must not exceed 2000 characters");
  }

  @Test
  void givenMalformedFirstResponseWhenInterpretedThenGenerationIsRetriedOnce() {
    // Given: one malformed response followed by a valid controlled intent
    SequenceLlmClient llmClient =
        new SequenceLlmClient(invalidResponse(), outOfScopeIntent());
    IntentInterpreter retryingInterpreter = interpreter(llmClient);

    // When: the question is interpreted
    AnalysisIntent result = retryingInterpreter.interpret("Who won the football match?");

    // Then: the second structured response is returned after exactly one retry
    assertThat(result.status()).isEqualTo(AnalysisStatus.OUT_OF_SCOPE);
    assertThat(llmClient.calls()).isEqualTo(2);
  }

  @Test
  void givenTwoMalformedResponsesWhenInterpretedThenOnlyOneRetryIsAttempted() {
    // Given: both the initial generation and the single retry are malformed
    SequenceLlmClient llmClient = new SequenceLlmClient(invalidResponse(), invalidResponse());
    IntentInterpreter retryingInterpreter = interpreter(llmClient);

    // When: the question is interpreted
    var assertion =
        assertThatThrownBy(() -> retryingInterpreter.interpret("Show average inbound bytes."));

    // Then: the second failure is propagated without an unbounded retry loop
    assertion.isInstanceOf(LlmClientException.class);
    assertThat(llmClient.calls()).isEqualTo(2);
  }

  @Test
  void givenUnavailableLlmWhenInterpretedThenRequestIsNotRetried() {
    // Given: the LLM dependency is unavailable
    SequenceLlmClient llmClient =
        new SequenceLlmClient(
            new LlmClientException(LlmClientException.Kind.UNAVAILABLE, "unavailable"),
            outOfScopeIntent());
    IntentInterpreter retryingInterpreter = interpreter(llmClient);

    // When: the question is interpreted
    var assertion =
        assertThatThrownBy(() -> retryingInterpreter.interpret("Show average inbound bytes."));

    // Then: dependency failures are propagated without retrying
    assertion.isInstanceOf(LlmClientException.class);
    assertThat(llmClient.calls()).isEqualTo(1);
  }

  private static IntentInterpreter interpreter(LlmClient llmClient) {
    Clock clock = Clock.fixed(Instant.parse("2026-10-08T12:00:00Z"), ZoneOffset.UTC);
    CuratedSemanticSchemaCatalog catalog = new CuratedSemanticSchemaCatalog(List.of());

    return new IntentInterpreter(
        llmClient,
        new IntentPromptFactory(clock, catalog),
        new IntentValidator(
            validatorFactory.getValidator(),
            catalog,
            clock,
            new IntentProperties(Duration.ofDays(7))));
  }

  private static AnalysisIntent outOfScopeIntent() {
    return new AnalysisIntent(
        AnalysisStatus.OUT_OF_SCOPE,
        QueryRequirements.empty(),
        ClarificationRequirement.none());
  }

  private static LlmClientException invalidResponse() {
    return new LlmClientException(
        LlmClientException.Kind.INVALID_RESPONSE, "reason=INVALID_STRUCTURE path=root");
  }

  private static final class SequenceLlmClient implements LlmClient {

    private final List<Object> responses;
    private int calls;

    private SequenceLlmClient(Object... responses) {
      this.responses = List.of(responses);
    }

    @Override
    public <T> T generateStructured(
        String systemPrompt, String userPrompt, Class<T> responseType) {
      Object response = responses.get(calls++);
      if (response instanceof RuntimeException exception) {
        throw exception;
      }
      return responseType.cast(response);
    }

    private int calls() {
      return calls;
    }
  }
}
