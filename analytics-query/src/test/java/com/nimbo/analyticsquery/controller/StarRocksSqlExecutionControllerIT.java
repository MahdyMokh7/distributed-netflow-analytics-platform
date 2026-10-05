package com.nimbo.analyticsquery.controller;

import static org.assertj.core.api.Assertions.assertThat;

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

import com.fasterxml.jackson.databind.JsonNode;

import com.nimbo.analyticsquery.support.StarRocksCatalogTestExtension;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class StarRocksSqlExecutionControllerIT {

    @RegisterExtension
    static final StarRocksCatalogTestExtension STARROCKS =
            new StarRocksCatalogTestExtension();

    @DynamicPropertySource
    static void starRocks(DynamicPropertyRegistry registry) {
        registry.add("analytics-query.schema.starrocks.enabled", () -> true);
        registry.add("analytics-query.schema.starrocks.jdbc-url", STARROCKS::jdbcUrl);
        registry.add("analytics-query.schema.starrocks.username", () -> "root");
        registry.add("analytics-query.schema.starrocks.password", () -> "");
        registry.add("analytics-query.sql-execution.starrocks.enabled", () -> true);
        registry.add("analytics-query.sql-execution.starrocks.jdbc-url", STARROCKS::jdbcUrl);
        registry.add("analytics-query.sql-execution.starrocks.username", () -> "root");
        registry.add("analytics-query.sql-execution.starrocks.password", () -> "");
    }

    @Autowired private TestRestTemplate restTemplate;

    @Test
    void givenApprovedStarRocksSqlWhenExecuteEndpointIsCalledThenResultIsNormalized() {
        // Given: a bounded read-only query over the approved StarRocks table
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>("""
                {
                  "sql": "SELECT ipv4_src_addr, in_bytes FROM nimbo.netflow WHERE PROCESS_TIME >= 1791014400000 AND PROCESS_TIME < 1791018000000 LIMIT 10"
                }
                """, headers);

        // When: the protected SQL execution endpoint receives the query
        ResponseEntity<JsonNode> response =
                restTemplate.postForEntity("/api/analytics/sql/execute", request, JsonNode.class);

        // Then: StarRocks validation, EXPLAIN and execution return a normalized result
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().path("status").asText()).isEqualTo("SUCCESS");
        assertThat(response.getBody().path("dataSource").asText()).isEqualTo("STARROCKS");
        assertThat(response.getBody().path("rowCount").asInt()).isZero();
        assertThat(response.getBody().path("data").isArray()).isTrue();
    }
}
