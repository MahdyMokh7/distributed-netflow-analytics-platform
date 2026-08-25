package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlywayConfigTest {

    @Test
    void shouldSkipMigrationWhenDisabled() throws SQLException {
        FlywayConfig config = new FlywayConfig();
        config.setEnabled(false);

        // A mocked DataSource is safe here: when disabled, flyway() never opens a connection.
        DataSource dataSource = mock(DataSource.class);

        org.flywaydb.core.Flyway flyway = config.flyway(dataSource);

        assertThat(flyway).isNotNull();
    }

    @Test
    void shouldAttemptMigrationWhenEnabledAndPropagateConnectionFailure() throws SQLException {
        FlywayConfig config = new FlywayConfig();
        config.setEnabled(true);
        config.setBaselineOnMigrate(true);
        config.setValidateOnMigrate(true);
        config.setLocations(new String[]{"classpath:db/migration"});

        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenThrow(new SQLException("no real database available"));

        assertThatThrownBy(() -> config.flyway(dataSource))
                .isInstanceOf(RuntimeException.class);
    }
}