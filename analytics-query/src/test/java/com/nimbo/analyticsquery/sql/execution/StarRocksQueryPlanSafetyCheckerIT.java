package com.nimbo.analyticsquery.sql.execution;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.util.unit.DataSize;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.nimbo.analyticsquery.config.QueryPlanSafetyProperties;
import com.nimbo.analyticsquery.support.StarRocksCatalogTestExtension;

@ExtendWith(StarRocksCatalogTestExtension.class)
class StarRocksQueryPlanSafetyCheckerIT {

    @Test
    void givenBoundedStarRocksPlanWhenCheckedThenItIsAccepted(
            StarRocksCatalogTestExtension starRocks) {
        // Given: StarRocks limits above the expected plan output
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            StarRocksQueryPlanSafetyChecker checker =
                    checker(dataSource, DataSize.ofKilobytes(64));

            // When: the approved StarRocks plan is checked
            var invocation = assertThatCode(
                    () -> checker.check("SELECT ipv4_src_addr FROM nimbo.netflow LIMIT 10"));

            // Then: the datasource-specific plan is accepted
            invocation.doesNotThrowAnyException();
        }
    }

    @Test
    void givenOversizedStarRocksPlanWhenCheckedThenItIsRejected(
            StarRocksCatalogTestExtension starRocks) {
        // Given: a StarRocks explain-output limit smaller than any useful plan
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            StarRocksQueryPlanSafetyChecker checker =
                    checker(dataSource, DataSize.ofBytes(1));

            // When: the explain output exceeds its independent limit
            var invocation = assertThatThrownBy(
                    () -> checker.check("SELECT ipv4_src_addr FROM nimbo.netflow LIMIT 10"));

            // Then: the controlled plan-output reason is returned
            invocation
                    .isInstanceOf(QueryPlanSafetyException.class)
                    .extracting(exception -> ((QueryPlanSafetyException) exception).reason())
                    .isEqualTo(QueryPlanSafetyException.Reason.PLAN_OUTPUT_TOO_LARGE);
        }
    }

    private static StarRocksQueryPlanSafetyChecker checker(
            HikariDataSource dataSource,
            DataSize maxExplainSize) {

        QueryPlanSafetyProperties properties =
                new QueryPlanSafetyProperties(
                        Duration.ofSeconds(5),
                        maxExplainSize,
                        10_000_000,
                        1_000_000);

        return new StarRocksQueryPlanSafetyChecker(
                dataSource,
                properties);
    }

    private static HikariDataSource dataSource(
            StarRocksCatalogTestExtension starRocks) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(starRocks.jdbcUrl());
        config.setUsername("root");
        config.setPassword("");
        config.setMaximumPoolSize(1);
        config.setReadOnly(true);

        return new HikariDataSource(config);
    }
}
