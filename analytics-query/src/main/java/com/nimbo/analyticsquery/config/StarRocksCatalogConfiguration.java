package com.nimbo.analyticsquery.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "analytics-query.schema.starrocks", name = "enabled", havingValue = "true")
public class StarRocksCatalogConfiguration {

    @Bean(name = "starRocksCatalogDataSource", destroyMethod = "close")
    public HikariDataSource starRocksCatalogDataSource(StarRocksCatalogProperties properties) {

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(properties.jdbcUrl());
        config.setUsername(properties.username());
        config.setPassword(properties.password());
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(properties.poolSize());
        config.setMinimumIdle(0);
        config.setReadOnly(true);
        config.setConnectionTimeout(properties.connectionTimeout().toMillis());
        config.setInitializationFailTimeout(-1);
        config.setPoolName("analytics-query-starrocks-catalog");

        return new HikariDataSource(config);
    }

    @Bean(name = "starRocksCatalogJdbcTemplate")
    public JdbcTemplate starRocksCatalogJdbcTemplate(
            @Qualifier("starRocksCatalogDataSource")
            HikariDataSource dataSource,
            StarRocksCatalogProperties properties) {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.setQueryTimeout(properties.queryTimeoutSeconds());

        return jdbcTemplate;
    }
}
