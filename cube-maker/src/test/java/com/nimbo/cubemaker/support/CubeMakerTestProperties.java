package com.nimbo.cubemaker.support;

import java.time.Instant;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "test.parquet")
public class CubeMakerTestProperties {
  private int recordsPerFile;
  private Instant baseTimestamp;
  private List<String> knownIps;
  private int fullBatchFiles;
  private int incrementalFirstBatch;
  private int incrementalAdditional;
  private int maxFilesTestTotal;
}
