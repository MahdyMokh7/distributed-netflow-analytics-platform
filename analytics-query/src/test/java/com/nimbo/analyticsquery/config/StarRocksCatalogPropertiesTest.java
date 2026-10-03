package com.nimbo.analyticsquery.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = StarRocksCatalogPropertiesTest.TestConfiguration.class)
@ActiveProfiles("test")
class StarRocksCatalogPropertiesTest {

    @Autowired
    private StarRocksCatalogProperties properties;

    @Test
    void givenDefaultConfigurationWhenPropertiesAreBoundThenRefreshRunsEveryFifteenMinutes() {
        // Given: the default StarRocks catalog configuration

        // When: Spring binds its refresh interval
        Duration refreshInterval = properties.refreshInterval();

        // Then: catalog refresh defaults to fifteen minutes
        assertThat(refreshInterval).isEqualTo(Duration.ofMinutes(15));
    }

    @EnableConfigurationProperties(StarRocksCatalogProperties.class)
    static class TestConfiguration {
    }
}
