package com.nimbo.restapi.support;

import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresTestContainer {

    private static final PostgreSQLContainer<?> INSTANCE = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("nimbo_test")
            .withUsername("nimbo_test")
            .withPassword("nimbo_test")
            .withInitScript("postgres-test-schema.sql");

    static {
        INSTANCE.start();
    }

    private PostgresTestContainer() {
    }

    public static PostgreSQLContainer<?> instance() {
        return INSTANCE;
    }
}
