package com.nimbo.analyticsquery.sql.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.SqlExecutionProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.support.StarRocksCatalogTestExtension;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.util.unit.DataSize;

@ExtendWith(StarRocksCatalogTestExtension.class)
class StarRocksSqlExecutorIT {

  @Test
  void givenLiveStarRocksWhenSelectIsExecutedThenRowsAreNormalized(
      StarRocksCatalogTestExtension starRocks) {
    // Given: a StarRocks executor connected to the approved analytics table
    try (HikariDataSource dataSource = dataSource(starRocks)) {
      StarRocksSqlExecutor executor =
          new StarRocksSqlExecutor(
              dataSource, new ObjectMapper().findAndRegisterModules(), properties());

      // When: a bounded read-only query is executed
      AnalyticsResult result =
          executor.execute(
              """
              SELECT ipv4_src_addr, in_bytes
              FROM nimbo.netflow
              LIMIT 1
              """);

      // Then: the empty result keeps its datasource and column contract
      assertThat(result.dataSource()).isEqualTo(AnalysisDataSource.STARROCKS);
      assertThat(result.columns()).containsExactly("ipv4_src_addr", "in_bytes");
      assertThat(result.rows()).isEmpty();
    }
  }

  private static SqlExecutionProperties properties() {
    SqlExecutionProperties.JdbcTarget disabled =
        new SqlExecutionProperties.JdbcTarget(false, "", "", "", 1, Duration.ofSeconds(1));

    return new SqlExecutionProperties(
        Duration.ofSeconds(5), DataSize.ofMegabytes(2), disabled, disabled);
  }

  private static HikariDataSource dataSource(StarRocksCatalogTestExtension starRocks) {

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(starRocks.jdbcUrl());
    config.setUsername("root");
    config.setPassword("");
    config.setMaximumPoolSize(1);
    config.setReadOnly(true);

    return new HikariDataSource(config);
  }
}
