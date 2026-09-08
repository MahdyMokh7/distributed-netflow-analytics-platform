package com.nimbo.restapi.support;

import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.jpa.properties.hibernate.jdbc.time_zone=UTC"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public abstract class PostgresIntegrationTest {

    private static final PostgreSQLContainer<?> POSTGRES = PostgresTestContainer.instance();

    @DynamicPropertySource
    static void configurePostgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
    }
}
