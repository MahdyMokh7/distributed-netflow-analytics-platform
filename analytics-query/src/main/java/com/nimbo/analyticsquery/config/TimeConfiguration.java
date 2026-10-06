package com.nimbo.analyticsquery.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TimeConfiguration {

  @Bean
  public Clock utcClock() {
    return Clock.systemUTC();
  }
}
