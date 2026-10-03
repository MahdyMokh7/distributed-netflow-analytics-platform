package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.config.IntentProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.LlmProperties;
import com.nimbo.analyticsquery.llm.OpenAiCompatibleLlmClient;
import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntentInterpreterTest {

    private static ValidatorFactory validatorFactory;
    private static IntentInterpreter interpreter;

    @BeforeAll
    static void setUpInterpreter() {
        LlmProperties properties = new LlmProperties(
                URI.create("http://127.0.0.1:65535"),
                "sk-test-key",
                "test-model",
                "/v1/chat/completions",
                new LlmProperties.Timeout(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1)));

        CuratedSemanticSchemaCatalog catalog =
                new CuratedSemanticSchemaCatalog(java.util.List.of());

        validatorFactory = Validation.buildDefaultValidatorFactory();

        interpreter = new IntentInterpreter(
                new OpenAiCompatibleLlmClient(
                        RestClient.builder()
                                .baseUrl(properties.baseUrl().toString())
                                .build(),
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
        assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("question must not be blank");
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
}
