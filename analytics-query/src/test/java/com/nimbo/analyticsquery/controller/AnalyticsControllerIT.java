package com.nimbo.analyticsquery.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.nimbo.analyticsquery.support.RestApiTestServerExtension;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class AnalyticsControllerIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @RegisterExtension
    static final RestApiTestServerExtension LLM = new RestApiTestServerExtension();

    @RegisterExtension
    static final RestApiTestServerExtension REST_API = new RestApiTestServerExtension();

    @DynamicPropertySource
    static void dependencies(DynamicPropertyRegistry registry) {
        registry.add("llm.base-url", () -> LLM.originUrl().toString());
        registry.add("llm.chat-completions-path", () -> "/api/chat/completions");
        registry.add("rest-api.base-url", () -> REST_API.baseUrl().toString());
    }

    @Autowired private TestRestTemplate restTemplate;

    @Test
    void givenSupportedAnalyticsQuestionWhenQueryEndpointIsCalledThenToolDataIsReturned() {
        // Given: a supported question with valid LLM intent and REST data
        LLM.respondWith(200, llmResponse(successfulTrafficIntent()));
        REST_API.respondWith(200, trafficResponse());
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Show traffic windows from the last hour"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: the complete tool-first path returns normalized PostgreSQL data
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
        assertThat(body.path("executionMode").asText()).isEqualTo("TOOL");
        assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
        assertThat(body.path("data").isArray()).isTrue();
        assertThat(body.path("data").get(0).path("srcIp").asText()).isEqualTo("10.0.0.1");
        assertThat(body.path("data").get(0).path("recordCount").asLong()).isEqualTo(4);
        assertThat(LLM.requestCount()).isEqualTo(1);
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenTerminalIntentWhenQueryEndpointIsCalledThenExecutionStopsBeforeRestApi() {
        // Given: an out-of-scope intent returned by the LLM
        LLM.respondWith(200, llmResponse(outOfScopeIntent()));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Who won the football match?"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: the controlled status is returned without executing a tool
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(requireBody(response).path("status").asText()).isEqualTo("OUT_OF_SCOPE");
        assertThat(LLM.requestCount()).isEqualTo(1);
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenIncompleteQuestionWhenQueryEndpointIsCalledThenClarificationQuestionIsReturned() {
        // Given: an incomplete request and the LLM clarification intent
        LLM.respondWith(200, llmResponse(clarificationIntent()));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Show traffic for 10.0.0.1"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: the user receives the specific question without tool execution
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("NEEDS_CLARIFICATION");
        assertThat(body.path("message").asText()).isEqualTo("What time range should I use?");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenIntentWithoutMatchingToolWhenQueryEndpointIsCalledThenUnsupportedIsReturned() {
        // Given: a valid NetFlow intent unsupported by the current tools
        LLM.respondWith(200, llmResponse(unsupportedIntent()));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Sum inbound bytes by source IP during the last hour"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: SQL fallback routes and stops before generation without invoking REST
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("UNSUPPORTED_ANALYSIS");
        assertThat(body.path("message").asText())
                .isEqualTo("SQL generation is not implemented yet");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenSupportedQuestionWithoutDataWhenQueryEndpointIsCalledThenEmptySuccessIsReturned() {
        // Given: a supported intent and the REST API no-data response
        LLM.respondWith(200, llmResponse(successfulTrafficIntent()));
        REST_API.respondWith(
                404,
                """
                {
                  "success": false,
                  "message": "No traffic data found"
                }
                """);
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Show traffic windows from the last hour"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: no data remains a successful empty tool result
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
        assertThat(body.path("data").isArray()).isTrue();
        assertThat(body.path("data")).isEmpty();
        assertThat(REST_API.requestCount()).isEqualTo(1);
    }

    @Test
    void givenUnavailableLlmWhenQueryEndpointIsCalledThenDependencyFailureIsReturned() {
        // Given: the LLM dependency is unavailable
        LLM.respondWith(503, "{\"message\":\"unavailable\"}");
        HttpEntity<String> request = analyticsQuestion();

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: a controlled dependency status is returned without tool execution
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("DEPENDENCY_UNAVAILABLE");
        assertThat(body.path("message").asText())
                .isEqualTo("Analytics interpretation service is temporarily unavailable");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenMalformedLlmContentWhenQueryEndpointIsCalledThenExecutionFailureIsReturned() {
        // Given: the LLM completes with malformed structured content
        LLM.respondWith(200, llmResponse("not-json"));
        HttpEntity<String> request = analyticsQuestion();

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: invalid model output is controlled without tool execution
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("EXECUTION_FAILED");
        assertThat(body.path("message").asText()).isEqualTo("Analytics interpretation failed");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenTruncatedLlmCompletionWhenQueryEndpointIsCalledThenExecutionFailureIsReturned() {
        // Given: the LLM reports a truncated completion
        LLM.respondWith(200, llmResponse(successfulTrafficIntent(), "length"));
        HttpEntity<String> request = analyticsQuestion();

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: incomplete model output is controlled without tool execution
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("EXECUTION_FAILED");
        assertThat(body.path("message").asText()).isEqualTo("Analytics interpretation failed");
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenBlankAnalyticsQuestionWhenQueryEndpointIsCalledThenBadRequestIsReturned() {
        // Given: an analytics request with a blank question
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": ""
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: request validation rejects the blank question
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        JsonNode body = requireBody(response);
        assertThat(body.path("status").asInt()).isEqualTo(400);
        assertThat(body.path("violations").path("question").asText())
                .isEqualTo("Question must not be blank");
    }

    @Test
    void givenMalformedJsonWhenQueryEndpointIsCalledThenBadRequestIsReturned() {
        // Given: malformed JSON for an analytics query
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question":
                }
                """);

        // When: the analytics query endpoint receives the malformed request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/query", request, JsonNode.class);

        // Then: malformed JSON is rejected with the expected API error
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        JsonNode body = requireBody(response);
        assertThat(body.path("status").asInt()).isEqualTo(400);
        assertThat(body.path("message").asText()).isEqualTo("Malformed JSON request");
    }

    @Test
    void givenEligibleQuestionWhenGenerateEndpointIsCalledThenValidatedSqlIsReturned() {
        // Given: an eligible intent followed by SQL for its routed PostgreSQL table
        LLM.respondWithSequence(
                llmResponse(unsupportedIntent()),
                llmResponse(generatedPostgresqlSql()));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Show the top source IPs in the last hour"
                }
                """);

        // When: the SQL generation endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/generate", request, JsonNode.class);

        // Then: validated SQL is returned without calling the REST API
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
        assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
        assertThat(body.path("sql").asText())
                .isEqualTo(
                        "SELECT src_ip, SUM(in_bytes) FROM traffic_aggregates "
                                + "GROUP BY src_ip LIMIT 10");
        assertThat(LLM.requestCount()).isEqualTo(2);
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenGeneratedSqlForUnknownTableWhenGenerateEndpointIsCalledThenQueryIsRejected() {
        // Given: an eligible intent followed by SQL referencing an unapproved table
        LLM.respondWithSequence(
                llmResponse(unsupportedIntent()),
                llmResponse(
                        """
                        {
                          "dataSource": "POSTGRESQL",
                          "sql": "SELECT src_ip FROM secret_table"
                        }
                        """));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Show the top source IPs in the last hour"
                }
                """);

        // When: the SQL generation endpoint validates the generated statement
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/generate", request, JsonNode.class);

        // Then: validation fails closed and no query is executed
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("QUERY_REJECTED");
        assertThat(body.path("message").asText())
                .isEqualTo("Generated SQL did not satisfy the query safety policy");
        assertThat(body.has("sql")).isFalse();
        assertThat(LLM.requestCount()).isEqualTo(2);
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenOutOfScopeIntentWhenGenerateEndpointIsCalledThenGenerationStops() {
        // Given: an out-of-scope intent returned by the interpretation call
        LLM.respondWith(200, llmResponse(outOfScopeIntent()));
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": "Who won the football match?"
                }
                """);

        // When: the SQL generation endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/generate", request, JsonNode.class);

        // Then: the controlled status returns before routing or SQL generation
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = requireBody(response);
        assertThat(body.path("status").asText()).isEqualTo("OUT_OF_SCOPE");
        assertThat(body.path("message").asText())
                .isEqualTo("Analytics request cannot be converted to SQL");
        assertThat(LLM.requestCount()).isEqualTo(1);
        assertThat(REST_API.requestCount()).isZero();
    }

    @Test
    void givenBlankSqlGenerationQuestionWhenGenerateEndpointIsCalledThenBadRequestIsReturned() {
        // Given: a SQL generation request with a blank question
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "question": " "
                }
                """);

        // When: the SQL generation endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/generate", request, JsonNode.class);

        // Then: request validation rejects the blank question
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(requireBody(response).path("violations").path("question").asText())
                .isEqualTo("Question must not be blank");
    }

    @Test
    void givenValidSqlWhenExecuteEndpointIsCalledThenNotImplementedIsReturned() {
        // Given: a valid SQL execution request
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "sql": "SELECT * FROM traffic_aggregates LIMIT 10"
                }
                """);

        // When: the SQL execution endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/execute", request, JsonNode.class);

        // Then: the endpoint reports that SQL execution is not implemented yet
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(requireBody(response).path("status").asInt()).isEqualTo(501);
    }

    @Test
    void givenBlankSqlWhenExecuteEndpointIsCalledThenBadRequestIsReturned() {
        // Given: a SQL execution request with blank SQL
        HttpEntity<String> request =
                jsonRequest(
                        """
                {
                  "sql": ""
                }
                """);

        // When: the SQL execution endpoint receives the request
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/analytics/sql/execute", request, JsonNode.class);

        // Then: request validation rejects the blank SQL
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(requireBody(response).path("violations").path("sql").asText())
                .isEqualTo("SQL must not be blank");
    }

    private HttpEntity<String> jsonRequest(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        return new HttpEntity<>(body, headers);
    }

    private HttpEntity<String> analyticsQuestion() {
        return jsonRequest(
                """
                {
                  "question": "Show traffic windows from the last hour"
                }
                """);
    }

    private JsonNode requireBody(ResponseEntity<JsonNode> response) {
        return Objects.requireNonNull(response.getBody(), "Expected HTTP response body");
    }

    private static String llmResponse(String intent) {
        return llmResponse(intent, "stop");
    }

    private static String llmResponse(String intent, String finishReason) {
        try {
            return """
                    {
                      "choices": [
                        {
                          "index": 0,
                          "message": {
                            "role": "assistant",
                            "content": %s
                          },
                          "finish_reason": "%s"
                        }
                      ]
                    }
                    """
                    .formatted(OBJECT_MAPPER.writeValueAsString(intent), finishReason);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Failed to build the deterministic LLM response", exception);
        }
    }

    private static String successfulTrafficIntent() {
        return """
                {
                  "status": "SUCCESS",
                  "queryRequirements": {
                    "dimensions": ["SOURCE_IP"],
                    "metrics": [
                      {
                        "field": "RECORD_COUNT",
                        "aggregation": "NONE"
                      }
                    ],
                    "filters": [],
                    "ipRequirements": [],
                    "trafficDirection": "BOTH",
                    "timeRange": {
                      "start": "2026-10-01T08:00:00Z",
                      "end": "2026-10-01T09:00:00Z"
                    },
                    "groupBy": [],
                    "granularity": null,
                    "sort": [],
                    "limit": 10,
                    "distinct": false
                  },
                  "clarification": {
                    "required": false,
                    "missingFields": [],
                    "question": null
                  }
                }
                """;
    }

    private static String outOfScopeIntent() {
        return """
                {
                  "status": "OUT_OF_SCOPE",
                  "queryRequirements": {
                    "dimensions": [],
                    "metrics": [],
                    "filters": [],
                    "ipRequirements": [],
                    "trafficDirection": "UNSPECIFIED",
                    "timeRange": null,
                    "groupBy": [],
                    "granularity": null,
                    "sort": [],
                    "limit": null,
                    "distinct": false
                  },
                  "clarification": {
                    "required": false,
                    "missingFields": [],
                    "question": null
                  }
                }
                """;
    }

    private static String clarificationIntent() {
        return """
                {
                  "status": "NEEDS_CLARIFICATION",
                  "queryRequirements": {
                    "dimensions": ["SOURCE_IP"],
                    "metrics": [
                      {
                        "field": "RECORD_COUNT",
                        "aggregation": "NONE"
                      }
                    ],
                    "filters": [],
                    "ipRequirements": [
                      {
                        "role": "SOURCE",
                        "matchType": "EXACT",
                        "values": ["10.0.0.1"]
                      }
                    ],
                    "trafficDirection": "UNSPECIFIED",
                    "timeRange": null,
                    "groupBy": [],
                    "granularity": null,
                    "sort": [],
                    "limit": null,
                    "distinct": false
                  },
                  "clarification": {
                    "required": true,
                    "missingFields": ["TIME_RANGE"],
                    "question": "What time range should I use?"
                  }
                }
                """;
    }

    private static String unsupportedIntent() {
        return """
                {
                  "status": "SUCCESS",
                  "queryRequirements": {
                    "dimensions": ["SOURCE_IP"],
                    "metrics": [
                      {
                        "field": "IN_BYTES",
                        "aggregation": "SUM"
                      }
                    ],
                    "filters": [],
                    "ipRequirements": [],
                    "trafficDirection": "BOTH",
                    "timeRange": {
                      "start": "2026-10-01T08:00:00Z",
                      "end": "2026-10-01T09:00:00Z"
                    },
                    "groupBy": ["SOURCE_IP"],
                    "granularity": null,
                    "sort": [],
                    "limit": 10,
                    "distinct": false
                  },
                  "clarification": {
                    "required": false,
                    "missingFields": [],
                    "question": null
                  }
                }
                """;
    }

    private static String trafficResponse() {
        return """
                {
                  "success": true,
                  "data": [
                    {
                      "srcIp": "10.0.0.1",
                      "windowStart": "2026-10-01T08:00:00Z",
                      "windowEnd": "2026-10-01T09:00:00Z",
                      "inBytes": 10,
                      "outBytes": 20,
                      "totalBytes": 30,
                      "inPackets": 1,
                      "outPackets": 2,
                      "totalPackets": 3,
                      "recordCount": 4,
                      "batchId": "11111111-1111-1111-1111-111111111111",
                      "ingestedAt": "2026-10-01T09:00:00Z"
                    }
                  ],
                  "timestamp": "2026-10-01T09:00:00Z"
                }
                """;
    }

    private static String generatedPostgresqlSql() {
        return """
                {
                  "dataSource": "POSTGRESQL",
                  "sql": "SELECT src_ip, SUM(in_bytes) FROM traffic_aggregates GROUP BY src_ip LIMIT 10"
                }
                """;
    }
}
