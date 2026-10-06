package com.nimbo.analyticsquery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@ConfigurationPropertiesScan
public class AnalyticsQueryApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnalyticsQueryApplication.class, args);
  }
}
