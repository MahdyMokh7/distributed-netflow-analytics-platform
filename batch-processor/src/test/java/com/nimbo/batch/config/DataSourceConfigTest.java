package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataSourceConfigTest {

    private PostgresConfig buildConfigPointingToUnreachableHost() {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("localhost");
        postgresConfig.setPort(59999);
        postgresConfig.setDatabase("nimbo_db");
        postgresConfig.setUsername("nimbo_batch");
        postgresConfig.setPassword("secret");

        // ✅ CHANGE: Pool → DriverPool, getPool() → getDriverPool()
        PostgresConfig.DriverPool driverPool = postgresConfig.getDriverPool();
        driverPool.setSize(2);
        driverPool.setConnectionTimeout(500L);
        driverPool.setPoolName("nimbo-batch-pg-pool-test");

        return postgresConfig;
    }

    @Test
    void shouldFailFastWhenDatabaseIsUnreachable() {
        PostgresConfig postgresConfig = buildConfigPointingToUnreachableHost();
        DataSourceConfig dataSourceConfig = new DataSourceConfig(postgresConfig);

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