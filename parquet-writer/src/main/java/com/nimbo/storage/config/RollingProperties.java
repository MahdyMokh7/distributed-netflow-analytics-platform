package com.nimbo.storage.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "rolling")
public class RollingProperties {

  private int maxRecordCount;
  private long maxSizeBytes;
  private Duration maxAge;
}
