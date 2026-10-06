package com.nimbo.analyticsquery.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqlExecutionDataSourceConfiguration {

  @Bean(name = "postgresSqlExecutionDataSource", destroyMethod = "close")
  @ConditionalOnProperty(
      prefix = "analytics-query.sql-execution.postgresql",
      name = "enabled",
      havingValue = "true")
  public DataSource postgresSqlExecutionDataSource(SqlExecutionProperties properties) {

    return createDataSource(
        properties.postgresql(), "org.postgresql.Driver", "analytics-query-postgresql-execution");
  }

  @Bean(name = "starRocksSqlExecutionDataSource", destroyMethod = "close")
  @ConditionalOnProperty(
      prefix = "analytics-query.sql-execution.starrocks",
      name = "enabled",
      havingValue = "true")
  public DataSource starRocksSqlExecutionDataSource(SqlExecutionProperties properties) {

    return createDataSource(
        properties.starrocks(), "com.mysql.cj.jdbc.Driver", "analytics-query-starrocks-execution");
  }

  private HikariDataSource createDataSource(
      SqlExecutionProperties.JdbcTarget properties, String driverClassName, String poolName) {

    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(properties.jdbcUrl());
    config.setUsername(properties.username());
    config.setPassword(properties.password());
    config.setDriverClassName(driverClassName);

    config.setMaximumPoolSize(properties.poolSize());
    config.setMinimumIdle(0);

    config.setReadOnly(true);
    config.setAutoCommit(true);

    config.setConnectionTimeout(properties.connectionTimeout().toMillis());

    config.setInitializationFailTimeout(-1);

    config.setPoolName(poolName);

    return new HikariDataSource(config);
  }
}
