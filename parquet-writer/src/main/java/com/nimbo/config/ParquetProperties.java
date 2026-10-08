package com.nimbo.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "storage.parquet")
public class ParquetProperties {

  private String compressionCodec;
  private int blockSizeBytes;
  private int pageSizeBytes;
  private boolean enableDictionary;
}
