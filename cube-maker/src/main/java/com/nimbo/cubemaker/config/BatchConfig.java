package com.nimbo.cubemaker.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "batch")
@Getter
@Setter
public class BatchConfig {

  private String schedule;
  private String windowDuration;
  private boolean enabled;
  private int maxFilesPerBatch;
  private Retention retention = new Retention();

  @Getter
  @Setter
  public static class Retention {
    private boolean enabled;
    private int previousDays;
  }
}
