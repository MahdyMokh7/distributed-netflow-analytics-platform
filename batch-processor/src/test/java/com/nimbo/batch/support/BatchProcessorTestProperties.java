package com.nimbo.batch.support;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Instant;
import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "test.parquet")
public class BatchProcessorTestProperties {
    private int recordsPerFile;
    private Instant baseTimestamp;
    private List<String> knownIps;
    private int fullBatchFiles;
    private int incrementalFirstBatch;
    private int incrementalAdditional;
    private int maxFilesTestTotal;
}
