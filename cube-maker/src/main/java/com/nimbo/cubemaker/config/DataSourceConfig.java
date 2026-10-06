package com.nimbo.cubemaker.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataSourceConfig {

  private final PostgresConfig postgresConfig;

  public DataSourceConfig(PostgresConfig postgresConfig) {
    this.postgresConfig = postgresConfig;
  }

  @Bean
  public DataSource postgresDataSource() {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(postgresConfig.getJdbcUrl());
    config.setUsername(postgresConfig.getUsername());
    config.setPassword(postgresConfig.getPassword());

    config.setMaximumPoolSize(
        postgresConfig.getDriverPool().getSize()); // 2 for BatchStateDao + 1 for Flyway
    config.setConnectionTimeout(postgresConfig.getDriverPool().getConnectionTimeout());
    config.setPoolName(postgresConfig.getDriverPool().getPoolName());

    return new HikariDataSource(config);
  }
}
