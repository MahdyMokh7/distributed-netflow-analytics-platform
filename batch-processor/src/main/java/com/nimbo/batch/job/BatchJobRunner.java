package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.processor.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BatchJobRunner {

    private final TrafficAggregationProcessor trafficProcessor;
    private final ConnectionProcessor connectionProcessor;
    private final ProtocolProcessor protocolProcessor;
    private final TopIpProcessor topIpProcessor;
    private final BatchConfig batchConfig;
    private final OutputConfig outputConfig;

    public BatchJobRunner(TrafficAggregationProcessor trafficProcessor,
                          ConnectionProcessor connectionProcessor,
                          ProtocolProcessor protocolProcessor,
                          TopIpProcessor topIpProcessor,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig) {
        this.trafficProcessor = trafficProcessor;
        this.connectionProcessor = connectionProcessor;
        this.protocolProcessor = protocolProcessor;
        this.topIpProcessor = topIpProcessor;
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
            log.info("=== RUNNING ALL PROCESSORS ===");

            trafficProcessor.process();
            connectionProcessor.process();
            protocolProcessor.process();
            topIpProcessor.process();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch job completed successfully in {} ms ({} seconds)",
                    duration, duration / 1000);

        } catch (Exception e) {
            log.error("Batch job failed", e);
            throw new RuntimeException("Batch job execution failed", e);
        }
    }
}