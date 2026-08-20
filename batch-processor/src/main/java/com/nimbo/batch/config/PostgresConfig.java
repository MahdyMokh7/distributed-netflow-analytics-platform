package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "postgres")
@Getter
@Setter
public class PostgresConfig {

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private int poolSize;

    public String getJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }
}