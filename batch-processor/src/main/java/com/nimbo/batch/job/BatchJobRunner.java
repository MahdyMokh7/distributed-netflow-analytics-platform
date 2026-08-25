package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.processor.BaseProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

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

    @Scheduled(cron = "${batch.schedule}")
    public void runBatchJob() {
        if (!batchConfig.isEnabled()) {
            log.info("Batch job is disabled. Skipping...");
            return;
        }

        UUID batchId = UUID.randomUUID();
        log.info("=== BATCH JOB STARTED ===");
        log.info("Batch ID: {}", batchId);
        log.info("Output mode: {}", outputConfig.getMode());
        log.info("Window duration: {}", batchConfig.getWindowDuration());

        long startTime = System.currentTimeMillis();

        try {
            log.info("Running {} processors with batch_id: {}", processors.size(), batchId);

            for (BaseProcessor processor : processors) {
                String processorName = processor.getClass().getSimpleName();
                log.info("Starting processor: {} (batch_id: {})", processorName, batchId);
                long processorStartTime = System.currentTimeMillis();

                processor.process(batchId);

                long processorDuration = System.currentTimeMillis() - processorStartTime;
                log.info("Completed processor: {} in {} ms (batch_id: {})",
                        processorName, processorDuration, batchId);
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("=== BATCH JOB COMPLETED SUCCESSFULLY ===");
            log.info("Batch ID: {}", batchId);
            log.info("Total duration: {} ms ({} seconds)", duration, duration / 1000);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Batch job failed - Batch ID: {}, Duration: {} ms", batchId, duration, e);
            throw new RuntimeException("Batch job execution failed - Batch ID: " + batchId, e);
        }
    }
}