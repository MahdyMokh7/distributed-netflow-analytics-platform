package com.nimbo.config;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "storage.hdfs")
public class HdfsProperties {

  private String uri;
  private String basePath;
  private String nameservice;
  private Map<String, String> nodes;
}
