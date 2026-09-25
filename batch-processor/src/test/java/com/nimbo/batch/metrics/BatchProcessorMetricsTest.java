package com.nimbo.batch.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.nimbo.batch.metrics.BatchProcessorMetrics.Cube.CONNECTION;
import static com.nimbo.batch.metrics.BatchProcessorMetrics.RunOutcome.SUCCESS;
import static com.nimbo.batch.metrics.BatchProcessorMetrics.Stage.DISCOVERY;
import static org.assertj.core.api.Assertions.assertThat;

class BatchProcessorMetricsTest {

    private SimpleMeterRegistry registry;
    private BatchProcessorMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new BatchProcessorMetrics(registry);
    }

    @AfterEach
    void closeRegistry() {
        registry.close();
    }

    @Test
    void givenASuccessfulRun_whenFinished_thenLifecycleMetricsDescribeItsFinalState() {
        // Given: a newly started timed batch run
        long earliestExpectedTimestamp = Instant.now().getEpochSecond();
        Timer.Sample sample = metrics.startTimer();

        // When: the run finishes successfully
        metrics.runStarted();
        metrics.runFinished(sample, SUCCESS);

        // Then: counters, duration, progress, and success time describe completion
        assertThat(registry.get("batch.processor.runs").tag("outcome", "success").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.get("batch.processor.run.duration").tag("outcome", "success").timer().count())
                .isEqualTo(1L);
        assertThat(registry.get("batch.processor.run.in.progress").gauge().value()).isZero();
        assertThat(registry.get("batch.processor.last.success.timestamp.seconds").gauge().value())
                .isGreaterThanOrEqualTo(earliestExpectedTimestamp);
    }

    @Test
    void givenAStageFailure_whenRecorded_thenDurationAndFailureUseTheSameBoundedStage() {
        // Given: a timed discovery stage
        Timer.Sample sample = metrics.startTimer();

        // When: discovery records failure and completion
        metrics.stageFailed(DISCOVERY);
        metrics.stageFinished(sample, DISCOVERY);

        // Then: failure and duration use the bounded discovery label
        assertThat(registry.get("batch.processor.stage.failures").tag("stage", "discovery").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.get("batch.processor.stage.duration").tag("stage", "discovery").timer().count())
                .isEqualTo(1L);
    }

    @Test
    void givenDurableWorkAndExecutorRetries_whenRecorded_thenOperationalTotalsAndGaugesAreAccurate() {
        // Given: deterministic committed work and retry totals
        Instant newestFileTimestamp = Instant.parse("2026-09-08T13:45:00Z");

        // When: operational counters and state gauges are recorded
        metrics.filesProcessed(2, newestFileTimestamp);
        metrics.sourceRecordsProcessed(4_000);
        metrics.outputRows(CONNECTION, 750);
        metrics.postgresRetries(CONNECTION, 2);
        metrics.stateFallbackActive(true);
        metrics.failedBatchPending(true);
        metrics.hdfsParquetFiles(12);

        // Then: each metric exposes its exact expected value
        assertThat(registry.get("batch.processor.files.processed").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("batch.processor.source.records").counter().count()).isEqualTo(4_000.0);
        assertThat(registry.get("batch.processor.output.rows").tag("cube", "connection").counter().count())
                .isEqualTo(750.0);
        assertThat(registry.get("batch.processor.postgres.retries").tag("cube", "connection").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.get("batch.processor.last.processed.file.timestamp.seconds").gauge().value())
                .isEqualTo(newestFileTimestamp.getEpochSecond());
        assertThat(registry.get("batch.processor.state.fallback.active").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get("batch.processor.failed.batch.pending").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get("batch.processor.hdfs.parquet.files").gauge().value()).isEqualTo(12.0);
    }

    @Test
    void givenNoPositiveWork_whenRecorded_thenMonotonicMetricsDoNotMove() {
        // Given: zero completed files, records, rows, and retries
        long noWork = 0L;

        // When: zero work is submitted to monotonic metrics
        metrics.filesProcessed(noWork, null);
        metrics.sourceRecordsProcessed(noWork);
        metrics.outputRows(CONNECTION, noWork);
        metrics.postgresRetries(CONNECTION, noWork);
        metrics.hdfsParquetFiles(noWork);

        // Then: all monotonic metrics and the file timestamp remain unchanged
        assertThat(registry.get("batch.processor.files.processed").counter().count()).isZero();
        assertThat(registry.get("batch.processor.source.records").counter().count()).isZero();
        assertThat(registry.get("batch.processor.output.rows").tag("cube", "connection").counter().count())
                .isZero();
        assertThat(registry.get("batch.processor.postgres.retries").tag("cube", "connection").counter().count())
                .isZero();
        assertThat(registry.get("batch.processor.last.processed.file.timestamp.seconds").gauge().value()).isZero();
        assertThat(registry.get("batch.processor.hdfs.parquet.files").gauge().value()).isZero();
    }
}
