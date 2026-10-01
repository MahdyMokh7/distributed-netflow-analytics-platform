package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.llm.LlmClient;
import org.springframework.stereotype.Service;

@Service
public class IntentInterpreter {

    private static final int MAX_QUESTION_LENGTH = 2_000;

    private final LlmClient llmClient;
    private final IntentPromptFactory promptFactory;
    private final IntentValidator validator;

    public IntentInterpreter(
            LlmClient llmClient,
            IntentPromptFactory promptFactory,
            IntentValidator validator) {

        this.llmClient = llmClient;
        this.promptFactory = promptFactory;
        this.validator = validator;
    }

    public AnalysisIntent interpret(String question) {
        validateQuestion(question);

        AnalysisIntent interpretedIntent =
                llmClient.generateStructured(promptFactory.createSystemPrompt(), question, AnalysisIntent.class);

        return validator.validate(interpretedIntent);
    }

    private void validateQuestion(String question) {
        if (question == null || question.isBlank()) {
            throw new IllegalArgumentException("question must not be blank");
        }

        if (question.length() > MAX_QUESTION_LENGTH) {
            throw new IllegalArgumentException("question must not exceed " + MAX_QUESTION_LENGTH + " characters");
        }
    }
}
