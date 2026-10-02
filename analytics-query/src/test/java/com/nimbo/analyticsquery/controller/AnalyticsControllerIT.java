package com.nimbo.analyticsquery.controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class AnalyticsControllerIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void givenValidAnalyticsQuestionWhenQueryEndpointIsCalledThenNotImplementedIsReturned() {
        // Given: a valid analytics query request
        HttpEntity<String> request = jsonRequest("""
                {
                  "question": "Show the top 10 IPs by bandwidth during the last two hours"
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/query",
                request,
                JsonNode.class);

        // Then: the endpoint reports that query execution is not implemented yet
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);

        JsonNode body = requireBody(response);
        assertThat(body.path("status").asInt()).isEqualTo(501);
        assertThat(body.path("error").asText()).isEqualTo("Not Implemented");
    }

    @Test
    void givenBlankAnalyticsQuestionWhenQueryEndpointIsCalledThenBadRequestIsReturned() {
        // Given: an analytics request with a blank question
        HttpEntity<String> request = jsonRequest("""
                {
                  "question": ""
                }
                """);

        // When: the analytics query endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/query",
                request,
                JsonNode.class);

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
        HttpEntity<String> request = jsonRequest("""
                {
                  "question":
                }
                """);

        // When: the analytics query endpoint receives the malformed request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/query",
                request,
                JsonNode.class);

        // Then: malformed JSON is rejected with the expected API error
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        JsonNode body = requireBody(response);
        assertThat(body.path("status").asInt()).isEqualTo(400);
        assertThat(body.path("message").asText())
                .isEqualTo("Malformed JSON request");
    }

    @Test
    void givenValidSqlGenerationQuestionWhenGenerateEndpointIsCalledThenNotImplementedIsReturned() {
        // Given: a valid SQL generation request
        HttpEntity<String> request = jsonRequest("""
                {
                  "question": "Show the top source IPs in the last hour"
                }
                """);

        // When: the SQL generation endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/sql/generate",
                request,
                JsonNode.class);

        // Then: the endpoint reports that SQL generation is not implemented yet
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(requireBody(response).path("status").asInt()).isEqualTo(501);
    }

    @Test
    void givenBlankSqlGenerationQuestionWhenGenerateEndpointIsCalledThenBadRequestIsReturned() {
        // Given: a SQL generation request with a blank question
        HttpEntity<String> request = jsonRequest("""
                {
                  "question": " "
                }
                """);

        // When: the SQL generation endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/sql/generate",
                request,
                JsonNode.class);

        // Then: request validation rejects the blank question
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(requireBody(response)
                .path("violations")
                .path("question")
                .asText())
                .isEqualTo("Question must not be blank");
    }

    @Test
    void givenValidSqlWhenExecuteEndpointIsCalledThenNotImplementedIsReturned() {
        // Given: a valid SQL execution request
        HttpEntity<String> request = jsonRequest("""
                {
                  "sql": "SELECT * FROM traffic_aggregates LIMIT 10"
                }
                """);

        // When: the SQL execution endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/sql/execute",
                request,
                JsonNode.class);

        // Then: the endpoint reports that SQL execution is not implemented yet
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(requireBody(response).path("status").asInt()).isEqualTo(501);
    }

    @Test
    void givenBlankSqlWhenExecuteEndpointIsCalledThenBadRequestIsReturned() {
        // Given: a SQL execution request with blank SQL
        HttpEntity<String> request = jsonRequest("""
                {
                  "sql": ""
                }
                """);

        // When: the SQL execution endpoint receives the request
        ResponseEntity<JsonNode> response = restTemplate.postForEntity(
                "/analytics/sql/execute",
                request,
                JsonNode.class);

        // Then: request validation rejects the blank SQL
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(requireBody(response)
                .path("violations")
                .path("sql")
                .asText())
                .isEqualTo("SQL must not be blank");
    }

    private HttpEntity<String> jsonRequest(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        return new HttpEntity<>(body, headers);
    }

    private JsonNode requireBody(ResponseEntity<JsonNode> response) {
        return Objects.requireNonNull(
                response.getBody(),
                "Expected HTTP response body");
    }
}
