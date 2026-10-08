package com.nimbo.config;

import com.nimbo.storage.HourWindowPolicy;
import com.nimbo.storage.batching.BatchPolicy;
import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
    RollingProperties.class,
    HdfsProperties.class,
    ParquetProperties.class
})
public class StorageConfiguration {

  @Bean
  public BatchPolicy batchPolicy(RollingProperties properties) {
    return new BatchPolicy(
        properties.getMaxRecordCount(), properties.getMaxSizeBytes(), properties.getMaxAge());
  }

  @Bean
  public HourWindowPolicy hourWindowPolicy(RollingProperties properties) {
    return new HourWindowPolicy(
        properties.getAllowedLatenessHours(),
        properties.isLateRoutingEnabled(),
        Clock.systemUTC());
  }
}
