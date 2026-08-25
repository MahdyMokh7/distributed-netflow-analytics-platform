package com.nimbo.batch.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

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

        config.setMaximumPoolSize(postgresConfig.getPool().getSize());
        config.setConnectionTimeout(postgresConfig.getPool().getConnectionTimeout());
        config.setPoolName(postgresConfig.getPool().getPoolName());

        return new HikariDataSource(config);
    }
}