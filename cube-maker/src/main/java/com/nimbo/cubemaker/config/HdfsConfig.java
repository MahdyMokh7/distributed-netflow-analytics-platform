package com.nimbo.cubemaker.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "hdfs")
@Getter
@Setter
public class HdfsConfig {

  private String uri;
  private String parquetPath;
  private String sparkHistoryPath;

  public String getFullParquetPath() {
    return uri + parquetPath;
  }

  public String getFullSparkHistoryPath() {
    return uri + sparkHistoryPath;
  }
}
