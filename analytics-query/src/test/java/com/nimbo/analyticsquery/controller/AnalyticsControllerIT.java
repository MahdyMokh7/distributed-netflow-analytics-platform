package com.nimbo.analyticsquery.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.support.PostgresSqlExecutionTestExtension;
import com.nimbo.analyticsquery.support.RestApiTestServerExtension;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@ExtendWith(OutputCaptureExtension.class)
class AnalyticsControllerIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @RegisterExtension static final RestApiTestServerExtension LLM = new RestApiTestServerExtension();

  @RegisterExtension
  static final RestApiTestServerExtension REST_API = new RestApiTestServerExtension();

  @RegisterExtension
  static final PostgresSqlExecutionTestExtension POSTGRES = new PostgresSqlExecutionTestExtension();

  @DynamicPropertySource
  static void dependencies(DynamicPropertyRegistry registry) {
    registry.add("llm.base-url", () -> LLM.originUrl().toString());
    registry.add("llm.chat-completions-path", () -> "/api/chat/completions");
    registry.add("rest-api.base-url", () -> REST_API.baseUrl().toString());
    registry.add("analytics-query.sql-execution.postgresql.enabled", () -> true);
    registry.add("analytics-query.sql-execution.postgresql.jdbc-url", POSTGRES::jdbcUrl);
    registry.add("analytics-query.sql-execution.postgresql.username", POSTGRES::username);
    registry.add("analytics-query.sql-execution.postgresql.password", POSTGRES::password);
  }

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private MeterRegistry meterRegistry;

  @Test
  void givenTheAnalyticsApplicationWhenBrowserRootIsOpenedThenStakeholderUiIsServed() {
    // Given: the analytics application is running

    // When: a stakeholder opens the browser root
    ResponseEntity<String> response = restTemplate.getForEntity("/", String.class);

    // Then: the question interface is returned without changing API paths
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody())
        .contains("Nimbo Insights", "Ask your network data a question", "/app.js", "/style.css");
  }

  @Test
  void givenSupportedAnalyticsQuestionWhenQueryEndpointIsCalledThenToolDataIsReturned(
      CapturedOutput output) {
    // Given: a supported question with valid LLM intent and REST data
    LLM.respondWithSequence(
        llmResponse(successfulTrafficIntent()),
        llmResponse(explanation("Traffic was observed for one source IP.")));
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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

    // Then: the complete tool-first path returns normalized PostgreSQL data
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("executionMode").asText()).isEqualTo("TOOL");
    assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
    assertThat(body.path("data").isArray()).isTrue();
    assertThat(body.path("data").get(0).path("srcIp").asText()).isEqualTo("10.0.0.1");
    assertThat(body.path("data").get(0).path("recordCount").asLong()).isEqualTo(4);
    assertThat(body.path("explanation").asText())
        .isEqualTo("Traffic was observed for one source IP.");
    assertThat(LLM.requestCount()).isEqualTo(2);
    assertThat(REST_API.requestCount()).isEqualTo(1);
    assertThat(output)
        .contains(
            "Matched analytics tool tool=traffic-windows",
            "endpoint=QUERY",
            "status=SUCCESS",
            "executionMode=TOOL",
            "datasource=POSTGRESQL",
            "rowCount=1",
            "durationMs=")
        .doesNotContain("Show traffic windows from the last hour", "10.0.0.1");
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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

    // Then: the user receives the specific question without tool execution
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("NEEDS_CLARIFICATION");
    assertThat(body.path("message").asText()).isEqualTo("What time range should I use?");
    assertThat(REST_API.requestCount()).isZero();
  }

  @Test
  void givenIntentWithoutMatchingToolWhenQueryEndpointIsCalledThenSqlFallbackExecutes() {
    // Given: an eligible intent followed by safe SQL for its routed datasource
    LLM.respondWithSequence(
        llmResponse(unsupportedIntent()),
        llmResponse(generatedPostgresqlSql()),
        llmResponse(explanation("10.0.0.1 has the highest inbound byte total.")));
    HttpEntity<String> request =
        jsonRequest(
            """
            {
              "question": "Sum inbound bytes by source IP during the last hour"
            }
            """);

    // When: the analytics query endpoint receives the request
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

    // Then: SQL fallback validates, plans and executes without invoking REST
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("executionMode").asText()).isEqualTo("GENERATED_SQL");
    assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
    assertThat(body.path("data").isArray()).isTrue();
    assertThat(body.path("data").get(0).path("src_ip").asText()).isEqualTo("10.0.0.1");
    assertThat(body.path("explanation").asText())
        .isEqualTo("10.0.0.1 has the highest inbound byte total.");
    assertThat(LLM.requestCount()).isEqualTo(3);
    assertThat(REST_API.requestCount()).isZero();
  }

  @Test
  void givenSupportedQuestionWithoutDataWhenQueryEndpointIsCalledThenEmptySuccessIsReturned() {
    // Given: a supported intent and the REST API no-data response
    LLM.respondWithSequence(
        llmResponse(successfulTrafficIntent()),
        llmResponse(explanation("No matching traffic rows were found.")));
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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

    // Then: no data remains a successful empty tool result
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("data").isArray()).isTrue();
    assertThat(body.path("data")).isEmpty();
    assertThat(body.path("explanation").asText()).isEqualTo("No matching traffic rows were found.");
    assertThat(REST_API.requestCount()).isEqualTo(1);
  }

  @Test
  void givenExplanationFailureWhenQuerySucceedsThenDataRemainsSuccessful() {
    // Given: a successful tool result followed by malformed explanation output
    LLM.respondWithSequence(llmResponse(successfulTrafficIntent()), llmResponse("not-json"));
    REST_API.respondWith(200, trafficResponse());
    HttpEntity<String> request = analyticsQuestion();

    // When: explanation generation fails after successful execution
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

    // Then: authoritative data remains successful and explanation is omitted
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("data")).hasSize(1);
    assertThat(body.has("explanation")).isFalse();
  }

  @Test
  void givenUnavailableLlmWhenQueryEndpointIsCalledThenDependencyFailureIsReturned() {
    // Given: the LLM dependency is unavailable
    LLM.respondWith(503, "{\"message\":\"unavailable\"}");
    HttpEntity<String> request = analyticsQuestion();

    // When: the analytics query endpoint receives the request
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/query", request, JsonNode.class);

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
        llmResponse(unsupportedIntent()), llmResponse(generatedPostgresqlSql()));
    HttpEntity<String> request =
        jsonRequest(
            """
            {
              "question": "Show the top source IPs in the last hour"
            }
            """);

    // When: the SQL generation endpoint receives the request
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/generate", request, JsonNode.class);

    // Then: validated SQL is returned without calling the REST API
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
    assertThat(body.path("sql").asText())
        .isEqualTo(
            "SELECT src_ip, SUM(in_bytes) FROM traffic_aggregates "
                + "WHERE window_start >= TIMESTAMP '2026-10-01 08:00:00' "
                + "AND window_start < TIMESTAMP '2026-10-01 09:00:00' "
                + "GROUP BY src_ip LIMIT 10");
    assertThat(body.has("data")).isFalse();
    assertThat(body.has("rowCount")).isFalse();
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
        restTemplate.postForEntity("/api/analytics/sql/generate", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/sql/generate", request, JsonNode.class);

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
        restTemplate.postForEntity("/api/analytics/sql/generate", request, JsonNode.class);

    // Then: request validation rejects the blank question
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(requireBody(response).path("violations").path("question").asText())
        .isEqualTo("Question must not be blank");
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
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: request validation rejects the blank SQL
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(requireBody(response).path("violations").path("sql").asText())
        .isEqualTo("SQL must not be blank");
  }

  @Test
  void givenApprovedSqlWhenExecuteEndpointIsCalledThenValidatedRowsAreReturned() {
    // Given: a bounded read-only query over an approved PostgreSQL cube
    HttpEntity<String> request = approvedSqlExecutionRequest();

    // When: the protected SQL execution endpoint receives the query
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: validation, EXPLAIN and read-only execution return normalized data
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = requireBody(response);
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
    assertThat(body.path("rowCount").asInt()).isEqualTo(2);
    assertThat(body.path("data").get(0).path("src_ip").asText()).isEqualTo("10.0.0.1");
  }

  @Test
  void givenWriteSqlWhenExecuteEndpointIsCalledThenExecutorIsNotReached() {
    // Given: a write statement and two existing aggregate rows
    HttpEntity<String> request =
        jsonRequest(
            """
            {
              "sql": "DELETE FROM traffic_aggregates"
            }
            """);
    assertThat(POSTGRES.trafficRowCount()).isEqualTo(2);

    // When: the protected SQL execution endpoint receives the write
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: policy rejects the statement before the executor changes data
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(requireBody(response).path("status").asText()).isEqualTo("QUERY_REJECTED");
    assertThat(POSTGRES.trafficRowCount()).isEqualTo(2);
  }

  @Test
  void givenSuccessfulRequestAndControlledFailureWhenPrometheusIsReadThenMetricsAreWired() {
    // Given: baseline counters and a successful query with failed explanation
    double requestCount =
        meterRegistry
            .counter("analytics.query.requests", "endpoint", "query", "status", "success")
            .count();
    double failureCount =
        meterRegistry
            .counter("analytics.query.failures", "stage", "explanation", "reason", "explanation")
            .count();
    LLM.respondWithSequence(llmResponse(successfulTrafficIntent()), llmResponse("not-json"));
    REST_API.respondWith(200, trafficResponse());

    // When: the request completes and Prometheus metrics are read
    ResponseEntity<JsonNode> queryResponse =
        restTemplate.postForEntity("/api/analytics/query", analyticsQuestion(), JsonNode.class);
    ResponseEntity<String> metricsResponse =
        restTemplate.getForEntity("/api/actuator/prometheus", String.class);

    // Then: request and failure counters increment and all custom metrics are exposed
    assertThat(queryResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(
            meterRegistry
                .counter("analytics.query.requests", "endpoint", "query", "status", "success")
                .count())
        .isEqualTo(requestCount + 1);
    assertThat(
            meterRegistry
                .counter(
                    "analytics.query.failures", "stage", "explanation", "reason", "explanation")
                .count())
        .isEqualTo(failureCount + 1);
    assertThat(metricsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(metricsResponse.getBody())
        .contains(
            "analytics_query_requests_total",
            "analytics_query_request_duration_seconds",
            "analytics_query_dependency_duration_seconds",
            "analytics_query_failures_total",
            "analytics_query_active_requests",
            "analytics_query_stage_duration_seconds",
            "analytics_query_execution_total");
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

  private HttpEntity<String> approvedSqlExecutionRequest() {
    return jsonRequest(
        """
        {
          "sql": "SELECT src_ip, in_bytes FROM traffic_aggregates \
        WHERE window_start >= TIMESTAMP '2026-10-01 08:00:00' \
        AND window_start < TIMESTAMP '2026-10-01 09:00:00' \
        ORDER BY src_ip LIMIT 10"
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
      throw new IllegalStateException("Failed to build the deterministic LLM response", exception);
    }
  }

  private static String explanation(String value) {
    return """
           {
             "explanation": "%s"
           }
           """
        .formatted(value);
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
        "sql": "SELECT src_ip, SUM(in_bytes) FROM traffic_aggregates \
      WHERE window_start >= TIMESTAMP '2026-10-01 08:00:00' \
      AND window_start < TIMESTAMP '2026-10-01 09:00:00' \
      GROUP BY src_ip LIMIT 10"
      }
      """;
  }
}
