package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;  // ← ADD THIS IMPORT

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "postgres")
public class PostgresConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private int batchSize;
    private int writerPartitions;

    private DriverPool driverPool = new DriverPool();
    private Retry retry = new Retry();

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    @Getter
    @Setter
    public static class DriverPool implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int size;
        private long connectionTimeout;
        private String poolName;
    }

    @Getter
    @Setter
    public static class Retry implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private int maxAttempts;
        private long initialDelayMs;
        private long maxDelayMs;
    }
}
