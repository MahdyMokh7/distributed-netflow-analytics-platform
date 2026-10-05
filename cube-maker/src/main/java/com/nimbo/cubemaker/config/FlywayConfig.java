package com.nimbo.cubemaker.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConfigurationProperties(prefix = "spring.flyway")
@Getter
@Setter
@Slf4j
public class FlywayConfig {

    private boolean enabled;
    private boolean baselineOnMigrate;
    private boolean validateOnMigrate;
    private String[] locations;

    @Bean
    public Flyway flyway(DataSource dataSource) {
        if (!enabled) {
            log.info("Flyway migrations are disabled");
            return Flyway.configure().dataSource(dataSource).load();
        }

        log.info("Initializing Flyway migrations");

        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(locations)
                .baselineOnMigrate(baselineOnMigrate)
                .validateOnMigrate(validateOnMigrate)
                .load();

        flyway.migrate();

        log.info("Flyway migrations completed successfully");

        return flyway;
    }
}