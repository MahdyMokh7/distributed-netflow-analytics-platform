package com.nimbo.batch.writer;

import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.PostgresConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresWriterTest {

    private TableRowMapper rowMapper;
    private BatchExecutor batchExecutor;
    private PostgresConfig config;
    private PostgresWriter postgresWriter;

    @BeforeEach
    void setUp() {
        rowMapper = new TableRowMapper();
        batchExecutor = new BatchExecutor();

        config = new PostgresConfig();
        config.setHost("localhost");
        config.setPort(5432);
        config.setDatabase("nimbo_db");
        config.setUsername("nimbo_user");
        config.setPassword("123456");
        config.setBatchSize(100);

        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(3);
        retry.setInitialDelayMs(100);
        retry.setMaxDelayMs(10000);
        config.setRetry(retry);

        ExecutorPoolConfig executorPoolConfig = new ExecutorPoolConfig();
        executorPoolConfig.setMaxPoolSize(1);
        executorPoolConfig.setConnectionTimeoutMs(3000);
        executorPoolConfig.setIdleTimeoutMs(30000);
        executorPoolConfig.setMaxLifetimeMs(60000);
        executorPoolConfig.setPoolName("test-executor-pool");

        JdbiDaoFactory daoFactory = new JdbiDaoFactory(config, executorPoolConfig);

        postgresWriter = new PostgresWriter(
                rowMapper,
                batchExecutor,
                daoFactory,
                config
        );
    }

    @Test
    void shouldInitializeWithoutError() {
        assertThat(postgresWriter).isNotNull();
    }

    @Test
    void batchExecutorShouldRetryOnFailure() {
        int[] callCount = {0};

        int[] results = batchExecutor.executeWithRetry(
                () -> {
                    callCount[0]++;
                    if (callCount[0] < 2) {
                        throw new RuntimeException("Simulated failure");
                    }
                    return new int[]{1, 1};
                },
                "test_table",
                3,
                10,
                10000
        );

        assertThat(callCount[0]).isEqualTo(2);
        assertThat(results).containsExactly(1, 1);
    }

    @Test
    void batchExecutorShouldFailAfterMaxRetries() {
        assertThatThrownBy(() -> batchExecutor.executeWithRetry(
                () -> { throw new RuntimeException("Always fails"); },
                "test_table",
                2,
                10,
                10000
        )).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("failed after 2 attempts");
    }

    @Test
    void batchExecutorShouldVerifyRowsAffected() {
        assertThatThrownBy(() -> batchExecutor.executeWithRetry(
                () -> new int[]{1, 0, 1},
                "test_table",
                1,
                10,
                10000
        )).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("failed after 1 attempts")
                .cause()
                .hasMessageContaining("expected 1 row affected, got 0");
    }

    @Test
    void batchExecutorShouldSumResultsCorrectly() {
        int[] results = {1, 1, 1, 1, 1};
        long sum = batchExecutor.sumResults(results);
        assertThat(sum).isEqualTo(5);
    }

}