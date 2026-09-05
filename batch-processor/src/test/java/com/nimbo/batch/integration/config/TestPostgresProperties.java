package com.nimbo.batch.integration.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "postgres")
public class TestPostgresProperties {
    private String database;
    private String username;
    private String password;
}