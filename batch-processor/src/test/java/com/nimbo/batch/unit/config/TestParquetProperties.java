package com.nimbo.batch.unit.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Instant;
import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "test.parquet")
public class TestParquetProperties {
    private int recordsPerFile;
    private int filesPerPartition;
    private int totalPartitions;
    private Instant baseTimestamp;
    private List<String> knownIps;
    private List<Integer> knownProtocols;
    private int fullBatchFiles;
    private int incrementalFirstBatch;
    private int incrementalAdditional;
    private int maxFilesTestTotal;
    private int fallbackFiles;
}