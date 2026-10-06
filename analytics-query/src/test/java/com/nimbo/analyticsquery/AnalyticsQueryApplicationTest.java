package com.nimbo.analyticsquery;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class AnalyticsQueryApplicationTest {

  private final ApplicationContext applicationContext;

  @Autowired
  AnalyticsQueryApplicationTest(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Test
  void givenTestProfileWhenContextStartsThenApplicationLoads() {
    // Given: test profile configuration is available

    // When: Spring creates the application context

    // Then: the application context is available
    assertThat(applicationContext).isNotNull();
  }
}
