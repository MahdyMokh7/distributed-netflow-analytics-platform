package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.discovery.FileDiscoveryService;
import com.nimbo.batch.exception.BatchProcessingException;
import com.nimbo.batch.processor.BaseProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
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
    private final FileDiscoveryService fileDiscoveryService;
    private final SparkSession spark;

    public BatchJobRunner(List<BaseProcessor> processors,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig,
                          FileDiscoveryService fileDiscoveryService,
                          SparkSession spark) {
        this.processors = processors;
        this.batchConfig = batchConfig;
        this.outputConfig = outputConfig;
        this.fileDiscoveryService = fileDiscoveryService;
        this.spark = spark;
    }

    @Scheduled(cron = "${batch.schedule}")
    public void runBatchJob() {
        if (!batchConfig.isEnabled()) {
            log.info("Batch job is disabled. Skipping...");
            return;
        }

        UUID batchId = resolveBatchId();
        log.info("=== BATCH JOB STARTED ===");
        log.info("Batch ID: {}", batchId);
        log.info("Output mode: {}", outputConfig.getMode());
        log.info("Window duration: {}", batchConfig.getWindowDuration());

        long startTime = System.currentTimeMillis();
        Dataset<Row> cachedDf = null;

        try {
            List<Path> files = fileDiscoveryService.discoverNewFiles(batchId);

            if (files.isEmpty()) {
                log.info("No new files to process. Batch {} complete.", batchId);
                return;
            }

            log.info("Found {} new files to process (batch: {})", files, batchId);

            cachedDf = readAndCacheFiles(files);

            if (!hasValidRecords(cachedDf, files, batchId)) {
                markFilesAsProcessed(files, batchId);
                return;
            }

            runProcessors(cachedDf, batchId);
            markFilesAsProcessed(files, batchId);

            long duration = System.currentTimeMillis() - startTime;
            log.info("+++ BATCH JOB COMPLETED SUCCESSFULLY +++");
            log.info("Batch ID: {}", batchId);
            log.info("Processed {} files", files.size());
            log.info("Total duration: {} ms ({} seconds)", duration, duration / 1000);

        } catch (BatchProcessingException e) {
            handleBatchFailure(e, startTime);
        } catch (Exception e) {
            handleUnexpectedFailure(batchId, e, startTime);
        } finally {
            unpersistDataset(cachedDf, batchId);
        }
    }

    private UUID resolveBatchId() {
        UUID failedBatchId = fileDiscoveryService.getFailedBatchId();
        if (failedBatchId != null) {
            log.info("Found failed batch: {}, reusing for retry", failedBatchId);
            return failedBatchId;
        }
        return UUID.randomUUID();
    }

    private Dataset<Row> readAndCacheFiles(List<Path> files) {
        String[] paths = files.stream()
                .map(Path::toString)
                .toArray(String[]::new);

        log.info("Reading {} Parquet files into shared batch Dataset", paths.length);

        return spark.read()
                .parquet(paths)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }

    private boolean hasValidRecords(Dataset<Row> cachedDf, List<Path> files, UUID batchId) {
        long sourceRecordCount = cachedDf.count();
        log.info(
                "Loaded and cached {} source records from {} files (batch: {})",
                sourceRecordCount,
                files.size(),
                batchId
        );

        if (sourceRecordCount == 0) {
            log.info("Source files contain no records. Batch {} complete.", batchId);
            return false;
        }

        return true;
    }

    private void runProcessors(Dataset<Row> cachedDf, UUID batchId) {
        log.info("Running {} processors with batch_id: {}", processors.size(), batchId);

        for (BaseProcessor processor : processors) {
            String processorName = processor.getClass().getSimpleName();
            log.info("Starting processor: {} (batch_id: {})", processorName, batchId);

            long processorStartTime = System.currentTimeMillis();
            processor.process(cachedDf, batchId);
            long processorDuration = System.currentTimeMillis() - processorStartTime;

            log.info("Completed processor: {} in {} ms (batch_id: {})", processorName, processorDuration, batchId);
        }
    }

    private void markFilesAsProcessed(List<Path> files, UUID batchId) {
        fileDiscoveryService.markFilesAsProcessed(files, batchId);
        fileDiscoveryService.clearFailedBatch();
    }

    private void handleBatchFailure(BatchProcessingException e, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        UUID failedId = e.getBatchId();

        log.error("Batch job failed for batch: {}, Duration: {} ms", failedId, duration, e);
        fileDiscoveryService.storeFailedBatch(failedId);

        throw new RuntimeException("Batch job execution failed - Batch ID: " + failedId, e);
    }

    private void handleUnexpectedFailure(UUID batchId, Exception e, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        log.error("Batch job failed with unexpected error - Batch ID: {}, Duration: {} ms", batchId, duration, e);
        throw new RuntimeException("Batch job execution failed - Batch ID: " + batchId, e);
    }

    private void unpersistDataset(Dataset<Row> cachedDf, UUID batchId) {
        if (cachedDf != null) {
            log.info("Unpersisting cached source Dataset for batch: {}", batchId);
            cachedDf.unpersist(false);
        }
    }
}