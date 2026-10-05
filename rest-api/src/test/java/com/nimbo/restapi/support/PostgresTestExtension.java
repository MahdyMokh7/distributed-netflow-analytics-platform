package com.nimbo.restapi.support;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class PostgresTestExtension implements BeforeAllCallback {

    private static final String DATABASE_NAME = "nimbo_restapi";

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withReuse(true);

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot().getStore(ExtensionContext.Namespace.create(PostgresTestExtension.class))
                .getOrComputeIfAbsent(PostgresResource.class, key -> new PostgresResource(), PostgresResource.class);
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://" + POSTGRES.getHost() + ":"
                + POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                + "/" + DATABASE_NAME;
    }

    private static final class PostgresResource implements ExtensionContext.Store.CloseableResource {

        private PostgresResource() {
            POSTGRES.start();
            createDatabaseIfMissing();
            applySchema();
        }

        private void createDatabaseIfMissing() {
            try (Connection connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                 Statement statement = connection.createStatement()) {
                try (ResultSet result = statement.executeQuery(
                        "SELECT 1 FROM pg_database WHERE datname = '" + DATABASE_NAME + "'")) {
                    if (!result.next()) {
                        statement.execute("CREATE DATABASE " + DATABASE_NAME);
                    }
                }
            } catch (SQLException exception) {
                throw new IllegalStateException("Failed to create cube-maker test database", exception);
            }
        }

        private void applySchema() {

            try {
                String schemaSql = Files.readString(
                        Path.of(
                                Objects.requireNonNull(PostgresTestExtension.class
                                        .getClassLoader()
                                        .getResource("postgres-test-schema.sql")).toURI())
                );

                try (Connection connection = DriverManager.getConnection(jdbcUrl(),
                        POSTGRES.getUsername(), POSTGRES.getPassword());
                     Statement statement = connection.createStatement()) {

                    for (String ddl : schemaSql.split(";")) {
                        if (!ddl.isBlank()) {
                            statement.execute(ddl);
                        }
                    }
                }

            } catch (SQLException exception) {
                throw new IllegalStateException("Failed to apply rest-api test schema using database connection",
                        exception);

            } catch (IOException | URISyntaxException exception) {
                throw new IllegalStateException("Failed to load rest-api test schema file", exception);
            }
        }

        @Override
        public void close() {
            POSTGRES.stop();
        }
    }

    public static final class DatasourceInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext context) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + jdbcUrl(),
                    "spring.datasource.username=" + POSTGRES.getUsername(),
                    "spring.datasource.password=" + POSTGRES.getPassword(),
                    "spring.datasource.driver-class-name=" + POSTGRES.getDriverClassName()
            ).applyTo(context.getEnvironment());
        }
    }
}