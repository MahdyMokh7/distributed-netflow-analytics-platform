package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "postgres.executor-pool")
public class ExecutorPoolConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private int maxPoolSize;
    private int minimumIdle;
    private long connectionTimeoutMs;
    private long idleTimeoutMs;
    private long maxLifetimeMs;
    private String poolName;
}
