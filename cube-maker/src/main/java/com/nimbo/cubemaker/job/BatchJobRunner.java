package com.nimbo.cubemaker.job;

import com.nimbo.cubemaker.config.BatchConfig;
import com.nimbo.cubemaker.config.OutputConfig;
import com.nimbo.cubemaker.discovery.FileDiscoveryService;
import com.nimbo.cubemaker.exception.BatchProcessingException;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.RunOutcome;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Stage;
import com.nimbo.cubemaker.processor.BaseProcessor;
import com.nimbo.cubemaker.retention.RetentionMaintenanceService;
import com.nimbo.cubemaker.retention.RetentionWindow;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_PROCESS_TIME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.timestamp_millis;

@Slf4j
@Component
public class BatchJobRunner {

    private final List<BaseProcessor> processors;
    private final BatchConfig batchConfig;
    private final OutputConfig outputConfig;
    private final FileDiscoveryService fileDiscoveryService;
    private final SparkSession spark;
    private final CubeMakerMetrics metrics;
    private final RetentionMaintenanceService retentionMaintenanceService;

    @Autowired
    public BatchJobRunner(List<BaseProcessor> processors,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig,
                          FileDiscoveryService fileDiscoveryService,
                          SparkSession spark,
                          CubeMakerMetrics metrics,
                          RetentionMaintenanceService retentionMaintenanceService) {
        this.processors = processors;
        this.batchConfig = batchConfig;
        this.outputConfig = outputConfig;
        this.fileDiscoveryService = fileDiscoveryService;
        this.spark = spark;
        this.metrics = metrics;
        this.retentionMaintenanceService = retentionMaintenanceService;
    }

    public BatchJobRunner(List<BaseProcessor> processors,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig,
                          FileDiscoveryService fileDiscoveryService,
                          SparkSession spark,
                          CubeMakerMetrics metrics) {
        this(processors, batchConfig, outputConfig, fileDiscoveryService, spark, metrics, null);
    }

    @Scheduled(cron = "${batch.schedule}")
    public void runBatchJob() {
        if (!batchConfig.isEnabled()) {
            log.info("Batch job is disabled. Skipping...");
            metrics.disabledRun();
            return;
        }

        UUID batchId = resolveBatchId();
        log.info("=== BATCH JOB STARTED ===");
        log.info("Batch ID: {}", batchId);
        log.info("Output mode: {}", outputConfig.getMode());
        log.info("Window duration: {}", batchConfig.getWindowDuration());

        long startTime = System.currentTimeMillis();
        Dataset<Row> cachedDf = null;
        Timer.Sample runTimer = metrics.startTimer();
        RunOutcome outcome = RunOutcome.FAILED;
        metrics.runStarted();

        try {
            RetentionWindow retentionWindow = retentionMaintenanceService == null
                    ? RetentionWindow.unbounded()
                    : retentionMaintenanceService.maintain();
            List<Path> files = executeStage(Stage.DISCOVERY,
                    () -> fileDiscoveryService.discoverNewFiles(batchId, retentionWindow));

            if (files.isEmpty()) {
                log.info("No new files to process. Batch {} complete.", batchId);
                outcome = RunOutcome.NO_DATA;
                return;
            }

            if (retentionMaintenanceService != null) {
                retentionMaintenanceService.ensurePartitionsForFiles(retentionWindow, files);
            }

            log.info("Found {} new files to process (batch: {})", files.size(), batchId);

            cachedDf = readAndCacheFiles(files, retentionWindow);
            long sourceRecordCount = executeStage(Stage.PARQUET_MATERIALIZATION, cachedDf::count);

            if (!hasValidRecords(sourceRecordCount, files, batchId)) {
                executeStage(Stage.STATE_COMMIT, () -> markFilesAsProcessed(files, batchId));
                outcome = RunOutcome.NO_DATA;
                return;
            }

            runProcessors(cachedDf, batchId);
            executeStage(Stage.STATE_COMMIT, () -> markFilesAsProcessed(files, batchId));
            metrics.sourceRecordsProcessed(sourceRecordCount);
            outcome = RunOutcome.SUCCESS;

            long duration = System.currentTimeMillis() - startTime;
            log.info("+++ BATCH JOB COMPLETED SUCCESSFULLY +++");
            log.info("Batch ID: {}", batchId);
            log.info("Processed {} files", files.size());
            log.info("Total duration: {} ms ({} seconds)", duration, duration / 1000);

        } catch (BatchProcessingException e) {
            handleBatchFailure(e, startTime);
        } catch (RuntimeException e) {
            handleUnexpectedFailure(batchId, e, startTime);
        } finally {
            try {
                unpersistDataset(cachedDf, batchId);
            } finally {
                metrics.runFinished(runTimer, outcome);
            }
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

    private Dataset<Row> readAndCacheFiles(List<Path> files, RetentionWindow retentionWindow) {
        String[] paths = files.stream()
                .map(Path::toString)
                .toArray(String[]::new);

        log.info("Reading {} Parquet files into shared batch Dataset", paths.length);

        Dataset<Row> source = spark.read().parquet(paths);
        if (retentionWindow.isBounded()) {
            source = source.filter(timestamp_millis(col(NETFLOW_PROCESS_TIME))
                    .geq(lit(java.sql.Timestamp.from(retentionWindow.cutoffInstant()))));
        }
        return source.persist(StorageLevel.MEMORY_AND_DISK());
    }

    private boolean hasValidRecords(long sourceRecordCount, List<Path> files, UUID batchId) {
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
            executeStage(processor.metricStage(), () -> processor.process(cachedDf, batchId));
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

    private void handleUnexpectedFailure(UUID batchId, RuntimeException e, long startTime) {
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

    private <T> T executeStage(Stage stage, Supplier<T> operation) {
        Timer.Sample sample = metrics.startTimer();
        try {
            return operation.get();
        } catch (RuntimeException | Error e) {
            metrics.stageFailed(stage);
            throw e;
        } finally {
            metrics.stageFinished(sample, stage);
        }
    }

    private void executeStage(Stage stage, Runnable operation) {
        executeStage(stage, () -> {
            operation.run();
            return null;
        });
    }
}
