package com.nimbo.cubemaker.writer;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.cubemaker.config.ExecutorPoolConfig;
import com.nimbo.cubemaker.config.PostgresConfig;
import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

class JdbiDaoFactoryTest {

  @Test
  void createsABoundedLazyPoolWithPostgresBatchRewritingEnabled() {
    PostgresConfig postgres = new PostgresConfig();
    postgres.setHost("postgresql");
    postgres.setPort(5432);
    postgres.setDatabase("nimbo_db");
    postgres.setUsername("nimbo_user");
    postgres.setPassword("secret");

    ExecutorPoolConfig executorPool = new ExecutorPoolConfig();
    executorPool.setMaxPoolSize(1);
    executorPool.setMinimumIdle(0);
    executorPool.setConnectionTimeoutMs(5_000);
    executorPool.setIdleTimeoutMs(60_000);
    executorPool.setMaxLifetimeMs(1_800_000);
    executorPool.setPoolName("executor-pool-test");

    HikariConfig hikari = new JdbiDaoFactory(postgres, executorPool).createHikariConfig();
    PGSimpleDataSource dataSource = (PGSimpleDataSource) hikari.getDataSource();

    assertThat(hikari.getMaximumPoolSize()).isEqualTo(1);
    assertThat(hikari.getMinimumIdle()).isZero();
    assertThat(hikari.getIdleTimeout()).isEqualTo(60_000);
    assertThat(hikari.getMaxLifetime()).isEqualTo(1_800_000);
    assertThat(dataSource.getReWriteBatchedInserts()).isTrue();
  }
}
