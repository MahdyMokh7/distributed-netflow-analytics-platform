package com.nimbo.restapi.support;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

public final class PostgresTestExtension implements BeforeAllCallback {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("nimbo_test")
            .withUsername("nimbo_test")
            .withPassword("nimbo_test")
            .withInitScript("postgres-test-schema.sql");

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot().getStore(ExtensionContext.Namespace.create(PostgresTestExtension.class))
                .getOrComputeIfAbsent(PostgresResource.class, key -> new PostgresResource(), PostgresResource.class);
    }

    private static final class PostgresResource implements ExtensionContext.Store.CloseableResource {

        private PostgresResource() {
            POSTGRES.start();
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
                    "spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                    "spring.datasource.username=" + POSTGRES.getUsername(),
                    "spring.datasource.password=" + POSTGRES.getPassword(),
                    "spring.datasource.driver-class-name=" + POSTGRES.getDriverClassName()
            ).applyTo(context.getEnvironment());
        }
    }
}
