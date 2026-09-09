package com.nimbo.batch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BatchProcessorMetrics {

    public enum RunOutcome {
        SUCCESS("success"),
        FAILED("failed"),
        NO_DATA("no_data"),
        DISABLED("disabled");

        private final String tagValue;

        RunOutcome(String tagValue) {
            this.tagValue = tagValue;
        }
    }

    public enum Stage {
        DISCOVERY("discovery"),
        PARQUET_MATERIALIZATION("parquet_materialization"),
        TRAFFIC("traffic"),
        CONNECTION("connection"),
        TOP_IP("top_ip"),
        PROTOCOL("protocol"),
        POSTGRES_TRAFFIC("postgres_write_traffic"),
        POSTGRES_CONNECTION("postgres_write_connection"),
        POSTGRES_TOP_IP("postgres_write_top_ip"),
        POSTGRES_PROTOCOL("postgres_write_protocol"),
        STATE_COMMIT("state_commit");

        private final String tagValue;

        Stage(String tagValue) {
            this.tagValue = tagValue;
        }
    }

    public enum Cube {
        TRAFFIC("traffic"),
        CONNECTION("connection"),
        TOP_IP("top_ip"),
        PROTOCOL("protocol");

        private final String tagValue;

        Cube(String tagValue) {
            this.tagValue = tagValue;
        }

        public String tagValue() {
            return tagValue;
        }
    }

    private final MeterRegistry registry;
    private final Map<RunOutcome, Counter> runCounters = new EnumMap<>(RunOutcome.class);
    private final Map<RunOutcome, Timer> runTimers = new EnumMap<>(RunOutcome.class);
    private final Map<Stage, Timer> stageTimers = new EnumMap<>(Stage.class);
    private final Map<Stage, Counter> stageFailureCounters = new EnumMap<>(Stage.class);
    private final Map<Cube, Counter> postgresRetryCounters = new EnumMap<>(Cube.class);
    private final Map<Cube, Counter> outputRowCounters = new EnumMap<>(Cube.class);
    private final Counter filesProcessed;
    private final Counter sourceRecords;
    private final AtomicInteger runInProgress = new AtomicInteger();
    private final AtomicInteger stateFallbackActive = new AtomicInteger();
    private final AtomicInteger failedBatchPending = new AtomicInteger();
    private final AtomicLong lastSuccessTimestamp = new AtomicLong();
    private final AtomicLong lastProcessedFileTimestamp = new AtomicLong();

    public BatchProcessorMetrics(MeterRegistry registry) {
        this.registry = registry;

        for (RunOutcome outcome : RunOutcome.values()) {
            runCounters.put(outcome, Counter.builder("batch.processor.runs")
                    .description("Batch processor scheduled runs")
                    .tag("outcome", outcome.tagValue)
                    .register(registry));
            runTimers.put(outcome, Timer.builder("batch.processor.run.duration")
                    .description("Batch processor run duration")
                    .tag("outcome", outcome.tagValue)
                    .register(registry));
        }

        for (Stage stage : Stage.values()) {
            stageTimers.put(stage, Timer.builder("batch.processor.stage.duration")
                    .description("Batch processor stage duration")
                    .tag("stage", stage.tagValue)
                    .register(registry));
            stageFailureCounters.put(stage, Counter.builder("batch.processor.stage.failures")
                    .description("Batch processor stage failures")
                    .tag("stage", stage.tagValue)
                    .register(registry));
        }

        for (Cube cube : Cube.values()) {
            postgresRetryCounters.put(cube, Counter.builder("batch.processor.postgres.retries")
                    .description("PostgreSQL write retry attempts")
                    .tag("cube", cube.tagValue)
                    .register(registry));
            outputRowCounters.put(cube, Counter.builder("batch.processor.output.rows")
                    .description("Rows written to PostgreSQL cube tables")
                    .tag("cube", cube.tagValue)
                    .register(registry));
        }

        filesProcessed = Counter.builder("batch.processor.files.processed")
                .description("Parquet files durably recorded as processed")
                .register(registry);
        sourceRecords = Counter.builder("batch.processor.source.records")
                .description("Source records completed by successful batch runs")
                .register(registry);

        Gauge.builder("batch.processor.run.in.progress", runInProgress, AtomicInteger::get)
                .description("Whether a batch processor run is active")
                .register(registry);
        Gauge.builder("batch.processor.last.success.timestamp.seconds", lastSuccessTimestamp, AtomicLong::get)
                .description("Epoch timestamp of the last successful non-empty batch")
                .register(registry);
        Gauge.builder("batch.processor.last.processed.file.timestamp.seconds", lastProcessedFileTimestamp, AtomicLong::get)
                .description("Timestamp from the newest durably processed Parquet filename")
                .register(registry);
        Gauge.builder("batch.processor.state.fallback.active", stateFallbackActive, AtomicInteger::get)
                .description("Whether processed-file state is using the in-memory fallback")
                .register(registry);
        Gauge.builder("batch.processor.failed.batch.pending", failedBatchPending, AtomicInteger::get)
                .description("Whether a failed batch identifier is pending retry")
                .register(registry);
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void runStarted() {
        runInProgress.set(1);
    }

    public void runFinished(Timer.Sample sample, RunOutcome outcome) {
        sample.stop(runTimers.get(outcome));
        runCounters.get(outcome).increment();
        runInProgress.set(0);
        if (outcome == RunOutcome.SUCCESS) {
            lastSuccessTimestamp.set(Instant.now().getEpochSecond());
        }
    }

    public void disabledRun() {
        runCounters.get(RunOutcome.DISABLED).increment();
    }

    public void stageFinished(Timer.Sample sample, Stage stage) {
        sample.stop(stageTimers.get(stage));
    }

    public void stageFailed(Stage stage) {
        stageFailureCounters.get(stage).increment();
    }

    public void postgresRetries(Cube cube, long retries) {
        if (retries > 0) {
            postgresRetryCounters.get(cube).increment(retries);
        }
    }

    public void outputRows(Cube cube, long rows) {
        if (rows > 0) {
            outputRowCounters.get(cube).increment(rows);
        }
    }

    public void filesProcessed(long files, Instant newestFileTimestamp) {
        if (files > 0) {
            filesProcessed.increment(files);
        }
        if (newestFileTimestamp != null) {
            lastProcessedFileTimestamp.accumulateAndGet(newestFileTimestamp.getEpochSecond(), Math::max);
        }
    }

    public void sourceRecordsProcessed(long records) {
        if (records > 0) {
            sourceRecords.increment(records);
        }
    }

    public void stateFallbackActive(boolean active) {
        stateFallbackActive.set(active ? 1 : 0);
    }

    public void failedBatchPending(boolean pending) {
        failedBatchPending.set(pending ? 1 : 0);
    }
}
