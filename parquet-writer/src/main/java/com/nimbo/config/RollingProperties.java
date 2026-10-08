package com.nimbo.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Setter
@Getter
@Validated
@ConfigurationProperties(prefix = "rolling")
public class RollingProperties {

  private int maxRecordCount;
  private long maxSizeBytes;
  private Duration maxAge;

  @Min(0)
  @Max(24)
  private int allowedLatenessHours;

  private boolean lateRoutingEnabled;
}
