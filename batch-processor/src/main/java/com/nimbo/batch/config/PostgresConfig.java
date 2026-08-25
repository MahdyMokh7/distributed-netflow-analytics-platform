package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "postgres")
public class PostgresConfig {

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private int batchSize = 1000;

    private Pool pool = new Pool();
    private Retry retry = new Retry();

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    @Getter
    @Setter
    public static class Pool {
        private int size;
        private long connectionTimeout;
        private String poolName;
    }

    @Getter
    @Setter
    public static class Retry {
        private int maxAttempts;
        private long initialDelayMs;
        private long maxDelayMs;
    }
}