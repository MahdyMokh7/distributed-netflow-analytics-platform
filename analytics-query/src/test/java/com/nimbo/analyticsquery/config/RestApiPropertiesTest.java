package com.nimbo.analyticsquery.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = RestApiPropertiesTest.TestConfiguration.class)
@ActiveProfiles("test")
class RestApiPropertiesTest {

  @Autowired private RestApiProperties properties;

  @Test
  void givenTestProfileWhenPropertiesAreBoundThenRestApiConfigurationMatches() {
    // Given: the test profile contains deterministic REST API configuration

    // When: Spring binds the REST API configuration properties
    RestApiProperties actual = properties;

    // Then: the base URL and timeouts are bound correctly
    assertThat(actual.baseUrl()).isEqualTo(URI.create("http://127.0.0.1:65535/api"));
    assertThat(actual.timeout().connect()).isEqualTo(Duration.ofSeconds(1));
    assertThat(actual.timeout().read()).isEqualTo(Duration.ofSeconds(5));
  }

  @EnableConfigurationProperties(RestApiProperties.class)
  static class TestConfiguration {}
}
