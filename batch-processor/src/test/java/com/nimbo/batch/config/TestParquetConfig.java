package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "test.parquet")
@Getter
@Setter
public class TestParquetConfig {

    private String localPath;
    private String emptyPath;
    private String invalidPath;
    private String samplePath;
    private int recordsPerFile;
    private int filesPerPartition;
}