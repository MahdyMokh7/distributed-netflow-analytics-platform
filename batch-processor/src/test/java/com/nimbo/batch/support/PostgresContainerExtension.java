package com.nimbo.batch.support;

import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresContainerExtension implements BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(PostgresContainerExtension.class);
    private static final Object DATABASE_KEY = new Object();

    private SharedPostgres database;

    @Override
    public void beforeAll(ExtensionContext context) {
        database = context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(
                        DATABASE_KEY, ignored -> SharedPostgresHolder.INSTANCE, SharedPostgres.class);
    }

    public Jdbi jdbi() {
        SharedPostgres sharedPostgres = database();
        return Jdbi.create(
                        sharedPostgres.container.getJdbcUrl(),
                        sharedPostgres.container.getUsername(),
                        sharedPostgres.container.getPassword())
                .installPlugin(new SqlObjectPlugin());
    }

    public String jdbcUrl() {
        return database().container.getJdbcUrl();
    }

    public String host() {
        return database().container.getHost();
    }

    public int port() {
        return database().container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    public String databaseName() {
        return database().container.getDatabaseName();
    }

    public String username() {
        return database().container.getUsername();
    }

    public String password() {
        return database().container.getPassword();
    }

    private SharedPostgres database() {
        if (database == null) {
            synchronized (PostgresContainerExtension.class) {
                if (database == null) {
                    database = SharedPostgresHolder.INSTANCE;
                }
            }
        }
        return database;
    }

    private static final class SharedPostgresHolder {
        private static final SharedPostgres INSTANCE = new SharedPostgres();
    }

    private static final class SharedPostgres implements ExtensionContext.Store.CloseableResource {
        private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16.2-alpine")
                .withDatabaseName("processor_test")
                .withUsername("processor_user")
                .withPassword("processor_password");

        private SharedPostgres() {
            container.start();
            Flyway.configure()
                    .dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
                    .locations("classpath:db.migration")
                    .load()
                    .migrate();
        }

        @Override
        public void close() {
            container.stop();
        }
    }
}
