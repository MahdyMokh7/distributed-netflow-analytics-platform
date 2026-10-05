package com.nimbo.analyticsquery.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresSqlExecutionTestExtension
        implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(PostgresSqlExecutionTestExtension.class);

    private static final Infrastructure INFRASTRUCTURE = new Infrastructure();

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot()
                .getStore(NAMESPACE)
                .getOrComputeIfAbsent(
                        Infrastructure.class,
                        ignored -> {
                            INFRASTRUCTURE.start();
                            return INFRASTRUCTURE;
                        },
                        Infrastructure.class);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        INFRASTRUCTURE.resetData();
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext) {

        return parameterContext.getParameter().getType()
                == PostgresSqlExecutionTestExtension.class;
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext) {

        return this;
    }

    public String jdbcUrl() {
        INFRASTRUCTURE.start();
        return INFRASTRUCTURE.container.getJdbcUrl();
    }

    public String username() {
        INFRASTRUCTURE.start();
        return "analytics_query_app";
    }

    public String password() {
        INFRASTRUCTURE.start();
        return "analytics-query-test-password";
    }

    public int trafficRowCount() {
        INFRASTRUCTURE.start();

        try (Connection connection = DriverManager.getConnection(
                        INFRASTRUCTURE.container.getJdbcUrl(),
                        username(),
                        password());
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(
                        "SELECT COUNT(*) FROM traffic_aggregates")) {

            result.next();
            return result.getInt(1);

        } catch (SQLException exception) {
            throw new IllegalStateException(
                    "Failed to count PostgreSQL executor test rows",
                    exception);
        }
    }

    private static final class Infrastructure
            implements ExtensionContext.Store.CloseableResource {

        private final PostgreSQLContainer<?> container =
                new PostgreSQLContainer<>("postgres:16-alpine")
                        .withDatabaseName("nimbo")
                        .withUsername("postgres")
                        .withPassword("postgres");

        private boolean started;

        synchronized void start() {
            if (started) {
                return;
            }

            container.start();
            executeAsAdministrator("""
                    CREATE ROLE analytics_query_app
                        LOGIN
                        PASSWORD 'analytics-query-test-password';
                    CREATE TABLE traffic_aggregates (
                        src_ip TEXT NOT NULL,
                        window_start TIMESTAMP NOT NULL,
                        in_bytes BIGINT NOT NULL
                    );
                    GRANT USAGE ON SCHEMA public TO analytics_query_app;
                    GRANT SELECT ON traffic_aggregates TO analytics_query_app;
                    """);
            started = true;
        }

        void resetData() {
            executeAsAdministrator("""
                    TRUNCATE TABLE traffic_aggregates;
                    INSERT INTO traffic_aggregates (src_ip, window_start, in_bytes)
                    VALUES
                        ('10.0.0.1', TIMESTAMP '2026-10-01 08:15:00', 100),
                        ('10.0.0.2', TIMESTAMP '2026-10-01 08:30:00', 200);
                    """);
        }

        private void executeAsAdministrator(String sql) {
            try (Connection connection = DriverManager.getConnection(
                    container.getJdbcUrl(),
                    container.getUsername(),
                    container.getPassword());
                 Statement statement = connection.createStatement()) {

                statement.execute(sql);

            } catch (SQLException exception) {
                throw new IllegalStateException(
                        "Failed to prepare PostgreSQL executor test data",
                        exception);
            }
        }

        @Override
        public synchronized void close() {
            if (container.isRunning()) {
                container.stop();
            }
            started = false;
        }
    }
}
