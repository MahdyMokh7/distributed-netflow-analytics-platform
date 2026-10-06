package com.nimbo.analyticsquery.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.nimbo.analyticsquery.support.PostgresSqlExecutionTestExtension;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class SqlExecutionControllerIT {

  @RegisterExtension
  static final PostgresSqlExecutionTestExtension POSTGRES = new PostgresSqlExecutionTestExtension();

  @DynamicPropertySource
  static void executionDatabase(DynamicPropertyRegistry registry) {
    registry.add("analytics-query.sql-execution.postgresql.enabled", () -> true);
    registry.add("analytics-query.sql-execution.postgresql.jdbc-url", POSTGRES::jdbcUrl);
    registry.add("analytics-query.sql-execution.postgresql.username", POSTGRES::username);
    registry.add("analytics-query.sql-execution.postgresql.password", POSTGRES::password);
  }

  @Autowired private TestRestTemplate restTemplate;

  @Test
  void givenApprovedSqlWhenExecuteEndpointIsCalledThenNormalizedRowsAreReturned() {
    // Given: a bounded read-only query over an approved PostgreSQL cube
    HttpEntity<String> request =
        jsonRequest(
            """
        {
          "sql": "SELECT src_ip, in_bytes FROM traffic_aggregates \
        WHERE window_start >= TIMESTAMP '2026-10-01 08:00:00' \
        AND window_start < TIMESTAMP '2026-10-01 09:00:00' \
        ORDER BY src_ip LIMIT 10"
        }
        """);

    // When: the protected SQL execution endpoint receives the query
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: validation, EXPLAIN and read-only execution return normalized data
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.path("status").asText()).isEqualTo("SUCCESS");
    assertThat(body.path("dataSource").asText()).isEqualTo("POSTGRESQL");
    assertThat(body.path("rowCount").asInt()).isEqualTo(2);
    assertThat(body.path("data").get(0).path("src_ip").asText()).isEqualTo("10.0.0.1");
  }

  @Test
  void givenWriteSqlWhenExecuteEndpointIsCalledThenPolicyRejectsIt() {
    // Given: a submitted SQL write against an approved table
    HttpEntity<String> request =
        jsonRequest(
            """
            {
              "sql": "DELETE FROM traffic_aggregates"
            }
            """);

    // When: the protected SQL execution endpoint receives the write
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: the controlled policy response is returned without execution
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.path("status").asText()).isEqualTo("QUERY_REJECTED");
    assertThat(body.path("data").isMissingNode()).isTrue();
  }

  @Test
  void givenUnknownTableWhenExecuteEndpointIsCalledThenDatasourceIdentificationRejectsIt() {
    // Given: a read-only query referencing an unapproved table
    HttpEntity<String> request =
        jsonRequest(
            """
        {
          "sql": "SELECT value FROM unknown_table \
        WHERE created_at >= '2026-10-01 08:00:00' \
        AND created_at < '2026-10-01 09:00:00' LIMIT 10"
        }
        """);

    // When: datasource identification evaluates the submitted query
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: no datasource or executor is selected
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.path("status").asText()).isEqualTo("QUERY_REJECTED");
    assertThat(body.path("dataSource").isMissingNode()).isTrue();
  }

  @Test
  void givenMultipleStatementsWhenExecuteEndpointIsCalledThenPolicyRejectsThem() {
    // Given: two submitted read-only statements
    HttpEntity<String> request =
        jsonRequest(
            """
        {
          "sql": "SELECT src_ip FROM traffic_aggregates LIMIT 1; \
        SELECT src_ip FROM traffic_aggregates LIMIT 1"
        }
        """);

    // When: the protected SQL execution endpoint receives both statements
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: the single-statement policy rejects the submission
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().path("status").asText()).isEqualTo("QUERY_REJECTED");
  }

  @Test
  void givenSystemTableWhenExecuteEndpointIsCalledThenPolicyRejectsIt() {
    // Given: SQL attempting to inspect the PostgreSQL system catalog
    HttpEntity<String> request =
        jsonRequest(
            """
            {
              "sql": "SELECT tablename FROM pg_catalog.pg_tables LIMIT 10"
            }
            """);

    // When: datasource identification evaluates the system-table query
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: system metadata remains outside the approved catalog
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().path("status").asText()).isEqualTo("QUERY_REJECTED");
  }

  @Test
  void givenCrossDatasourceSqlWhenExecuteEndpointIsCalledThenPolicyRejectsIt() {
    // Given: SQL combining PostgreSQL and StarRocks table names
    HttpEntity<String> request =
        jsonRequest(
            """
        {
          "sql": "SELECT t.src_ip FROM traffic_aggregates t \
        JOIN nimbo.netflow n ON n.ipv4_src_addr = t.src_ip LIMIT 10"
        }
        """);

    // When: datasource identification evaluates the mixed query
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

    // Then: no single datasource is authorized to execute it
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().path("status").asText()).isEqualTo("QUERY_REJECTED");
  }

  private static HttpEntity<String> jsonRequest(String body) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    return new HttpEntity<>(body, headers);
  }
}
