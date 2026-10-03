package com.nimbo.analyticsquery.sql;

import java.util.Objects;

import org.springframework.stereotype.Service;

import com.nimbo.analyticsquery.config.SqlGenerationProperties;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.llm.LlmClient;
import com.nimbo.analyticsquery.llm.LlmClientException;

@Service
public class SqlGenerator {

    private final LlmClient llmClient;
    private final SqlGenerationPromptFactory promptFactory;
    private final SqlGenerationProperties properties;

    public SqlGenerator(
            LlmClient llmClient,
            SqlGenerationPromptFactory promptFactory,
            SqlGenerationProperties properties) {

        this.llmClient = llmClient;
        this.promptFactory = promptFactory;
        this.properties = properties;
    }

    public GeneratedSqlCandidate generate(
            String question, QueryRequirements requirements, SqlGenerationContext context) {

        Objects.requireNonNull(requirements, "queryRequirements must not be null");

        Objects.requireNonNull(context, "sqlGenerationContext must not be null");

        String systemPrompt = promptFactory.createSystemPrompt(requirements, context);

        String userPrompt = promptFactory.createUserPrompt(question);

        GeneratedSqlCandidate candidate =
                llmClient.generateStructured(systemPrompt, userPrompt, GeneratedSqlCandidate.class);

        validateCandidate(candidate, context);

        return candidate;
    }

    private void validateCandidate(GeneratedSqlCandidate candidate, SqlGenerationContext context) {

        if (candidate == null) {
            throw invalidResponse("LLM returned no generated SQL candidate");
        }

        if (candidate.dataSource() != context.dataSource()) {

            throw invalidResponse("LLM changed the selected datasource");
        }

        if (candidate.sql().length() > properties.maxSqlLength()) {

            throw invalidResponse("Generated SQL exceeds the maximum allowed size");
        }
    }

    private LlmClientException invalidResponse(String message) {

        return new LlmClientException(LlmClientException.Kind.INVALID_RESPONSE, message);
    }
}
