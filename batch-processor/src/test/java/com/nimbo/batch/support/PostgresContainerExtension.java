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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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

    public void ensureDailyPartitions(LocalDate date) {
        String suffix = DateTimeFormatter.ofPattern("uuuu_MM_dd").format(date);
        jdbi().useHandle(handle -> {
            for (String table : new String[]{
                    "traffic_aggregates",
                    "connection_aggregates",
                    "protocol_usage_aggregates",
                    "ip_bandwidth_aggregates"}) {
                handle.execute("CREATE TABLE IF NOT EXISTS " + table + "_" + suffix
                        + " PARTITION OF " + table
                        + " FOR VALUES FROM ('" + date + "') TO ('" + date.plusDays(1) + "')");
            }
        });
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
            createAnalyticsReaderRoleIfMissing();
            Flyway.configure()
                    .dataSource(jdbcUrlFor(DATABASE_NAME), container.getUsername(), container.getPassword())
                    .locations("classpath:db.migration")
                    .load()
                    .migrate();
        }

        private void createAnalyticsReaderRoleIfMissing() {
            try (Connection connection = DriverManager.getConnection(
                    container.getJdbcUrl(), container.getUsername(), container.getPassword());
                 Statement statement = connection.createStatement()) {
                statement.execute("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_roles
                                WHERE rolname = 'analytics_query_reader'
                            ) THEN
                                CREATE ROLE analytics_query_reader NOLOGIN;
                            END IF;
                        END
                        $$
                        """);
            } catch (SQLException exception) {
                throw new IllegalStateException(
                        "Failed to create analytics-query reader role",
                        exception);
            }
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
