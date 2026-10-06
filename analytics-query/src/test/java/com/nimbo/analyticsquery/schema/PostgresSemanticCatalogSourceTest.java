package com.nimbo.analyticsquery.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PostgresSemanticCatalogSourceTest {

  private final PostgresSemanticCatalogSource source = new PostgresSemanticCatalogSource();

  @Test
  void givenFinalCubeDefinitionsWhenTablesAreReadThenOnlyFourAnalyticsTablesExist() {
    // Given: the curated PostgreSQL catalog source

    // When: its tables are read
    var tables = source.tables();

    // Then: exactly the four final aggregate cubes are exposed
    assertThat(tables)
        .extracting(SemanticTable::name)
        .containsExactly(
            "traffic_aggregates",
            "connection_aggregates",
            "ip_bandwidth_aggregates",
            "protocol_usage_aggregates");
  }

  @Test
  void givenTrafficCubeWhenColumnsAreReadThenFinalTypesAndNullabilityArePreserved() {
    // Given: the curated traffic aggregate table
    SemanticTable traffic =
        source.tables().stream()
            .filter(table -> table.name().equals("traffic_aggregates"))
            .findFirst()
            .orElseThrow();

    // When: important physical columns are selected
    SemanticColumn sourceIp = traffic.column("src_ip").orElseThrow();
    SemanticColumn ingestedAt = traffic.column("ingested_at").orElseThrow();

    // Then: migrated SQL types and nullable metadata match the table
    assertThat(sourceIp.sqlType()).isEqualTo("INET");
    assertThat(sourceIp.nullable()).isFalse();
    assertThat(sourceIp.semanticField()).isEqualTo(SemanticFieldNames.SOURCE_IP);
    assertThat(sourceIp.allowedAggregations()).containsExactly(Aggregation.COUNT_DISTINCT);
    assertThat(traffic.column("in_bytes").orElseThrow().allowedAggregations())
        .containsExactly(Aggregation.SUM);
    assertThat(traffic.timeResolution()).isEqualTo(Duration.ofHours(1));
    assertThat(ingestedAt.nullable()).isTrue();
    assertThat(ingestedAt.semanticField()).isNull();
  }
}
