package com.nimbo.restapi.client;

import com.nimbo.restapi.config.StarRocksQueryProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class StarRocksQueryClient {

  private final HikariDataSource dataSource;
  private final NamedParameterJdbcTemplate jdbcTemplate;

  public StarRocksQueryClient(StarRocksQueryProperties properties) {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(properties.getJdbcUrl());

    config.setUsername(properties.getUsername());

    config.setPassword(properties.getPassword());

    config.setDriverClassName("com.mysql.cj.jdbc.Driver");

    config.setMaximumPoolSize(properties.getMaximumPoolSize());

    config.setMinimumIdle(1);

    config.setPoolName("StarRocksQueryPool");

    config.setConnectionTimeout(5000);

    config.setInitializationFailTimeout(-1);

    this.dataSource = new HikariDataSource(config);

    JdbcTemplate coreJdbcTemplate = new JdbcTemplate(dataSource);

    coreJdbcTemplate.setQueryTimeout(properties.getQueryTimeoutSeconds());

    coreJdbcTemplate.setMaxRows(properties.getMaxRows());

    this.jdbcTemplate = new NamedParameterJdbcTemplate(coreJdbcTemplate);
  }

  public NamedParameterJdbcTemplate jdbc() {
    return jdbcTemplate;
  }

  @PreDestroy
  public void close() {
    dataSource.close();
  }
}
