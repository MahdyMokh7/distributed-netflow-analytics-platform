package com.nimbo.batch.support;

import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class PostgresContainerExtension implements BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(PostgresContainerExtension.class);
    private static final Object DATABASE_KEY = new Object();
    private static final String DATABASE_NAME = "nimbo_batch_processor";

    private SharedPostgres database;

    @Override
    public void beforeAll(ExtensionContext context) {
        database = context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(
                        DATABASE_KEY, ignored -> SharedPostgresHolder.INSTANCE, SharedPostgres.class);
    }

    public Jdbi jdbi() {
        SharedPostgres sharedPostgres = database();
        return Jdbi.create(jdbcUrl(), sharedPostgres.container.getUsername(), sharedPostgres.container.getPassword())
                .installPlugin(new SqlObjectPlugin());
    }

    public String jdbcUrl() {
        SharedPostgres sharedPostgres = database();
        return "jdbc:postgresql://" + sharedPostgres.container.getHost() + ":"
                + sharedPostgres.container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                + "/" + DATABASE_NAME;
    }

    public String host() {
        return database().container.getHost();
    }

    public int port() {
        return database().container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    public String databaseName() {
        return DATABASE_NAME;
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
        private static final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withReuse(true);

        private SharedPostgres() {
            container.start();
            createDatabaseIfMissing();
            Flyway.configure()
                    .dataSource(jdbcUrlFor(DATABASE_NAME), container.getUsername(), container.getPassword())
                    .locations("classpath:db.migration")
                    .load()
                    .migrate();
        }

        private void createDatabaseIfMissing() {
            try (Connection connection = DriverManager.getConnection(
                    container.getJdbcUrl(), container.getUsername(), container.getPassword());
                 Statement statement = connection.createStatement()) {
                try (ResultSet result = statement.executeQuery(
                        "SELECT 1 FROM pg_database WHERE datname = '" + DATABASE_NAME + "'")) {
                    if (!result.next()) {
                        statement.execute("CREATE DATABASE " + DATABASE_NAME);
                    }
                }
            } catch (SQLException exception) {
                throw new IllegalStateException("Failed to create rest-api test database", exception);
            }
        }

        private String jdbcUrlFor(String databaseName) {
            return "jdbc:postgresql://" + container.getHost() + ":"
                    + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                    + "/" + databaseName;
        }

        @Override
        public void close() {
            // keep reusable postgres alive
        }
    }
}