package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.processor.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class BatchJobRunner {

    private final List<BaseProcessor> processors;
    private final BatchConfig batchConfig;
    private final OutputConfig outputConfig;

    public BatchJobRunner(List<BaseProcessor> processors,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig) {
        this.processors = processors;
        this.batchConfig = batchConfig;
        this.outputConfig = outputConfig;
    }

    @Scheduled(cron = "${batch.schedule:0 0 * * * ?}")
    public void runBatchJob() {
        if (!batchConfig.isEnabled()) {
            log.info("Batch job is disabled. Skipping...");
            return;
        }

        log.info("Starting batch job - Output mode: {}", outputConfig.getMode());
        long startTime = System.currentTimeMillis();

        try {
            log.info("=== RUNNING ALL PROCESSORS ({}) ===", processors.size());
            for (BaseProcessor processor : processors) {
                log.info("Running processor: {}", processor.getClass().getSimpleName());
                processor.process();
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch job completed successfully in {} ms ({} seconds)",
                    duration, duration / 1000);
        } catch (Exception e) {
            log.error("Batch job failed", e);
            throw new RuntimeException("Batch job execution failed", e);
        }
    }
}