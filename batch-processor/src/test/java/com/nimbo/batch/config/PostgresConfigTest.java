package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresConfigTest {

    @Test
    void shouldBuildJdbcUrlFromHostPortAndDatabase() {
        PostgresConfig config = new PostgresConfig();
        config.setHost("10.10.61.70");
        config.setPort(5432);
        config.setDatabase("nimbo_db");

        assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://10.10.61.70:5432/nimbo_db");
    }

    @Test
    void shouldOverrideDefaultBatchSizeWhenSet() {
        PostgresConfig config = new PostgresConfig();
        config.setBatchSize(500);

        assertThat(config.getBatchSize()).isEqualTo(500);
    }

    @Test
    void shouldInitializePoolAndRetryWithNonNullDefaults() {
        PostgresConfig config = new PostgresConfig();

        // ✅ CHANGE: getPool() → getDriverPool()
        assertThat(config.getDriverPool()).isNotNull();
        assertThat(config.getRetry()).isNotNull();
    }

    @Test
    void poolNestedPropertiesShouldBeSettable() {
        // ✅ CHANGE: Pool → DriverPool
        PostgresConfig.DriverPool driverPool = new PostgresConfig.DriverPool();
        driverPool.setSize(20);
        driverPool.setConnectionTimeout(10_000L);
        driverPool.setPoolName("nimbo-pool");

        assertThat(driverPool.getSize()).isEqualTo(20);
        assertThat(driverPool.getConnectionTimeout()).isEqualTo(10_000L);
        assertThat(driverPool.getPoolName()).isEqualTo("nimbo-pool");
    }

    @Test
    void retryNestedPropertiesShouldBeSettable() {
        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(3);
        retry.setInitialDelayMs(1000L);
        retry.setMaxDelayMs(8000L);

        assertThat(retry.getMaxAttempts()).isEqualTo(3);
        assertThat(retry.getInitialDelayMs()).isEqualTo(1000L);
        assertThat(retry.getMaxDelayMs()).isEqualTo(8000L);
    }
}