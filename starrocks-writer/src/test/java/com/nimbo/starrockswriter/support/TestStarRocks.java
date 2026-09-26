package com.nimbo.starrockswriter.support;

import org.awaitility.core.ConditionTimeoutException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public final class TestStarRocks {

    private static final String IMAGE = "starrocks/allin1-ubuntu:4.0.12";
    private static final int FE_HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;
    private static final int MYSQL_PORT = 9030;
    private static final String DATABASE = "nimbo";
    private static final String TABLE = "netflow";

    private final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(FE_HTTP_PORT, BE_HTTP_PORT, MYSQL_PORT)
            .waitingFor(Wait.forHttp("/api/health")
                    .forPort(FE_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(4)));

    GenericContainer<?> container() {
        return container;
    }

    public String host() {
        return container.getHost();
    }

    public int beHttpPort() {
        return container.getMappedPort(BE_HTTP_PORT);
    }

    public String database() {
        return DATABASE;
    }

    public String table() {
        return TABLE;
    }

    public String qualifiedTable() {
        return DATABASE + "." + TABLE;
    }

    public String jdbcUrl() {
        return "jdbc:mysql://" + host() + ":" + container.getMappedPort(MYSQL_PORT) + "/" + DATABASE;
    }

    private String adminJdbcUrl() {
        return "jdbc:mysql://" + host() + ":" + container.getMappedPort(MYSQL_PORT);
    }

    public Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), "root", "");
    }

    public void withDatabase(SqlConsumer action) throws Exception {
        try (Connection connection = connection()) {
            action.accept(connection);
        }
    }

    public int rowCount(String sourceIp) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable();
        if (sourceIp == null) {
            try (Connection connection = connection();
                 Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {
                result.next();
                return result.getInt(1);
            }
        }

        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(sql + " WHERE ipv4_src_addr = ?")) {
            statement.setString(1, sourceIp);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    public void awaitRowCount(int expected) {
        await().alias("Expected " + expected + " rows in " + qualifiedTable())
                .pollInterval(Duration.ofMillis(500))
                .atMost(Duration.ofMinutes(1))
                .until(() -> {
                    try {
                        return rowCount(null) == expected;
                    } catch (SQLException exception) {
                        return false;
                    }
                });
    }

    void resetSchema() {
        String createTableSql = loadCreateTableSql();
        AtomicReference<SQLException> lastFailure = new AtomicReference<>();
        try {
            await().alias("StarRocks BE capacity and integration-test schema")
                    .pollInterval(Duration.ofSeconds(1))
                    .atMost(Duration.ofMinutes(2))
                    .until(() -> {
                        try (Connection connection = DriverManager.getConnection(adminJdbcUrl(), "root", "");
                             Statement statement = connection.createStatement()) {
                            statement.execute("DROP DATABASE IF EXISTS " + DATABASE);
                            statement.execute("CREATE DATABASE " + DATABASE);
                            statement.execute(createTableSql);
                            return true;
                        } catch (SQLException exception) {
                            lastFailure.set(exception);
                            return false;
                        }
                    });
        } catch (ConditionTimeoutException exception) {
            throw new IllegalStateException(
                    "StarRocks integration-test schema was not created",
                    lastFailure.get() == null ? exception : lastFailure.get());
        }
    }

    private String loadCreateTableSql() {
        try {
            String sql = StreamUtils.copyToString(
                    new ClassPathResource("sql/netflow.sql").getInputStream(),
                    StandardCharsets.UTF_8);
            if (!sql.contains("BUCKETS 8") || !sql.contains("\"replication_num\" = \"2\"")) {
                throw new IllegalStateException("StarRocks test schema overrides no longer match netflow.sql");
            }
            return sql.replace("BUCKETS 8", "BUCKETS 1")
                    .replace("\"replication_num\" = \"2\"", "\"replication_num\" = \"1\"");
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load StarRocks schema", exception);
        }
    }

    @FunctionalInterface
    public interface SqlConsumer {
        void accept(Connection connection) throws Exception;
    }
}
