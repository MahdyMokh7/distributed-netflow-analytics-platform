package com.nimbo.batch.util;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;

public final class TestPostgresExtension implements BeforeAllCallback {
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(TestPostgresExtension.class);

    private SharedPostgres database;

    @Override
    public void beforeAll(ExtensionContext context) {
        database = context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(SharedPostgres.class, ignored -> new SharedPostgres(), SharedPostgres.class);
    }

    public Jdbi jdbi() {
        return Jdbi.create(database.container.getJdbcUrl(), database.container.getUsername(), database.container.getPassword())
                .installPlugin(new SqlObjectPlugin());
    }

    public String host() { return database.container.getHost(); }
    public int port() { return database.container.getMappedPort(5432); }
    public String database() { return database.container.getDatabaseName(); }
    public String username() { return database.container.getUsername(); }
    public String password() { return database.container.getPassword(); }

    private static final class SharedPostgres implements ExtensionContext.Store.CloseableResource {
        private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16.2-alpine")
                .withDatabaseName("processor_test")
                .withUsername("processor_user")
                .withPassword("processor_password");

        private SharedPostgres() {
            container.start();
        }

        @Override
        public void close() {
            container.stop();
        }
    }
}
