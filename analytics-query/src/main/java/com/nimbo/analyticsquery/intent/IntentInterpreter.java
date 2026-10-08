package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClient;
import com.nimbo.analyticsquery.llm.LlmClientException;
import org.springframework.stereotype.Service;

@Service
public class IntentInterpreter {

  private static final int MAX_QUESTION_LENGTH = 2_000;
  private static final String INVALID_RESPONSE_RETRY_INSTRUCTION =
      """

      Your previous response could not be parsed as the required JSON contract.
      Return exactly one JSON object with every required property and the exact
      nested array item structures defined above. Return JSON only.
      """;

  private final LlmClient llmClient;
  private final IntentPromptFactory promptFactory;
  private final IntentValidator validator;

  public IntentInterpreter(
      LlmClient llmClient, IntentPromptFactory promptFactory, IntentValidator validator) {

    this.llmClient = llmClient;
    this.promptFactory = promptFactory;
    this.validator = validator;
  }

  public AnalysisIntent interpret(String question) {
    validateQuestion(question);

    String systemPrompt = promptFactory.createSystemPrompt();
    AnalysisIntent interpretedIntent;

    try {
      interpretedIntent =
          llmClient.generateStructured(systemPrompt, question, AnalysisIntent.class);
    } catch (LlmClientException exception) {
      if (exception.kind() != LlmClientException.Kind.INVALID_RESPONSE) {
        throw exception;
      }

      interpretedIntent =
          llmClient.generateStructured(
              systemPrompt + INVALID_RESPONSE_RETRY_INSTRUCTION,
              question,
              AnalysisIntent.class);
    }

    return validator.validate(interpretedIntent);
  }

  private void validateQuestion(String question) {
    if (question == null || question.isBlank()) {
      throw new IllegalArgumentException("question must not be blank");
    }

    if (question.length() > MAX_QUESTION_LENGTH) {
      throw new IllegalArgumentException(
          "question must not exceed " + MAX_QUESTION_LENGTH + " characters");
    }
  }
}
