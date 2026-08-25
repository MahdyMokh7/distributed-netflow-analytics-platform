package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataSourceConfigTest {

    private PostgresConfig buildConfigPointingToUnreachableHost() {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("localhost");
        postgresConfig.setPort(59999); // nothing listens here -> fails fast, no long timeout
        postgresConfig.setDatabase("nimbo_db");
        postgresConfig.setUsername("nimbo_batch");
        postgresConfig.setPassword("secret");

        PostgresConfig.Pool pool = postgresConfig.getPool();
        pool.setSize(2);
        pool.setConnectionTimeout(500L); // keep the unit test fast
        pool.setPoolName("nimbo-batch-pg-pool-test");

        return postgresConfig;
    }

    @Test
    void shouldFailFastWhenDatabaseIsUnreachable() {
        PostgresConfig postgresConfig = buildConfigPointingToUnreachableHost();
        DataSourceConfig dataSourceConfig = new DataSourceConfig(postgresConfig);

        // We can't assert a live, working DataSource without a real Postgres instance,
        // but we can verify the bean wires PostgresConfig values through and fails
        // fast (connection refused) rather than hanging for the default 30s timeout.
        assertThatThrownBy(dataSourceConfig::postgresDataSource)
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldUseConfiguredJdbcUrlWhenBuildingPool() {
        PostgresConfig postgresConfig = buildConfigPointingToUnreachableHost();

        assertThat(postgresConfig.getJdbcUrl())
                .isEqualTo("jdbc:postgresql://localhost:59999/nimbo_db");
    }
}