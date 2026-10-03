package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.nimbo.analyticsquery.config.LlmProperties;
import com.nimbo.analyticsquery.config.SqlGenerationProperties;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.llm.LlmClientException;
import com.nimbo.analyticsquery.llm.OpenAiCompatibleLlmClient;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.PostgresSemanticCatalogSource;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.schema.SemanticTable;
import com.nimbo.analyticsquery.support.RestApiTestServerExtension;

class SqlGeneratorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

    @RegisterExtension
    static final RestApiTestServerExtension SERVER = new RestApiTestServerExtension();

    private SqlGenerator generator;
    private QueryRequirements requirements;
    private SqlGenerationContext context;

    @BeforeEach
    void setUp() {
        LlmProperties llmProperties =
                new LlmProperties(
                        SERVER.originUrl(),
                        "test-key",
                        "test-model",
                        "/api/chat/completions",
                        new LlmProperties.Timeout(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        SqlGenerationProperties generationProperties = new SqlGenerationProperties(256);

        generator =
                new SqlGenerator(
                        new OpenAiCompatibleLlmClient(
                                RestClient.builder().baseUrl(SERVER.originUrl().toString()).build(),
                                llmProperties,
                                OBJECT_MAPPER),
                        new SqlGenerationPromptFactory(OBJECT_MAPPER, generationProperties),
                        generationProperties);

        requirements = requirements();
        context = context();
    }

    @Test
    void givenStructuredCandidateWhenSqlIsGeneratedThenDatasourceAndSqlArePreserved()
            throws JsonProcessingException {
        // Given: the LLM returns SQL for the selected PostgreSQL datasource
        SERVER.respondWith(
                200,
                response(
                        new GeneratedSqlCandidate(
                                AnalysisDataSource.POSTGRESQL,
                                "SELECT src_ip FROM traffic_aggregates LIMIT 10")));

        // When: SQL generation runs with the approved context
        GeneratedSqlCandidate candidate =
                generator.generate("Show source IPs from the last hour.", requirements, context);

        // Then: the structured candidate retains the selected datasource and SQL
        assertThat(candidate.dataSource()).isEqualTo(AnalysisDataSource.POSTGRESQL);
        assertThat(candidate.sql()).isEqualTo("SELECT src_ip FROM traffic_aggregates LIMIT 10");
    }

    @Test
    void givenChangedDatasourceWhenSqlIsGeneratedThenCandidateIsRejected()
            throws JsonProcessingException {
        // Given: the LLM changes the trusted PostgreSQL route to StarRocks
        SERVER.respondWith(
                200,
                response(
                        new GeneratedSqlCandidate(
                                AnalysisDataSource.STARROCKS,
                                "SELECT ipv4_src_addr FROM nimbo.netflow LIMIT 10")));

        // When: SQL generation validates the structured candidate
        var assertion =
                assertThatThrownBy(
                        () ->
                                generator.generate(
                                        "Show source IPs from the last hour.",
                                        requirements,
                                        context));

        // Then: changing the selected datasource is rejected
        assertion
                .isInstanceOf(LlmClientException.class)
                .hasMessage("LLM changed the selected datasource");
    }

    @Test
    void givenOversizedSqlWhenSqlIsGeneratedThenCandidateIsRejected()
            throws JsonProcessingException {
        // Given: the LLM returns SQL larger than the configured boundary
        SERVER.respondWith(
                200,
                response(
                        new GeneratedSqlCandidate(
                                AnalysisDataSource.POSTGRESQL, "SELECT " + "x".repeat(256))));

        // When: SQL generation validates the structured candidate
        var assertion =
                assertThatThrownBy(
                        () ->
                                generator.generate(
                                        "Show source IPs from the last hour.",
                                        requirements,
                                        context));

        // Then: oversized generated SQL is rejected
        assertion
                .isInstanceOf(LlmClientException.class)
                .hasMessage("Generated SQL exceeds the maximum allowed size");
    }

    private static QueryRequirements requirements() {
        return new QueryRequirements(
                List.of(SemanticFieldNames.SOURCE_IP),
                List.of(),
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.UNSPECIFIED,
                new QueryRequirements.TimeRange(
                        Instant.parse("2026-10-03T08:00:00Z"),
                        Instant.parse("2026-10-03T09:00:00Z")),
                List.of(),
                null,
                List.of(),
                10,
                false);
    }

    private static SqlGenerationContext context() {
        SemanticTable traffic =
                new PostgresSemanticCatalogSource()
                        .tables().stream()
                                .filter(table -> table.name().equals("traffic_aggregates"))
                                .findFirst()
                                .orElseThrow();

        return new SqlGenerationContextFactory()
                .create(new DataSourceRoute(AnalysisDataSource.POSTGRESQL, traffic));
    }

    private static String response(GeneratedSqlCandidate candidate) throws JsonProcessingException {
        String content = OBJECT_MAPPER.writeValueAsString(candidate);

        return OBJECT_MAPPER.writeValueAsString(
                new CompletionResponse(
                        List.of(new Choice(0, new Message("assistant", content), "stop"))));
    }

    private record CompletionResponse(List<Choice> choices) {}

    private record Choice(int index, Message message, String finish_reason) {}

    private record Message(String role, String content) {}
}
