package com.nimbo.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.ingester.model.DlqReason;
import com.nimbo.storage.service.batching.BatchPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetWriterMetricsTest {

  private SimpleMeterRegistry registry;
  private ParquetWriterMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new ParquetWriterMetrics(registry);
  }

  @Test
  void givenRegisteredInstance_whenRegisteredAgain_thenMetricSeriesRemainUnique() {
    // Given: one registered consumer instance
    metrics.registerInstance(1);

    // When: the same instance is registered a second time
    metrics.registerInstance(1);

    // Then: registration remains idempotent for every instance metric
    assertThat(registry.find("ingester_records_written_total").tag("instance", "1").counters())
        .hasSize(1);
    assertThat(registry.find("ingester_parquet_files_committed_total").tag("instance", "1").counters()).hasSize(1);
    assertThat(registry.find("ingester_consumer_running").tag("instance", "1").gauges()).hasSize(1);
    assertThat(registry.find("ingester_rollover_total").tag("instance", "1").counters()).hasSize(6);
  }

  @Test
  void givenRegisteredConsumer_whenLifecycleChanges_thenRunningGaugeTracksState() {
    // Given: one registered consumer whose initial state is stopped
    metrics.registerInstance(2);

    // When: the consumer starts and then stops
    metrics.consumerStarted(2);
    double running = registry.get("ingester_consumer_running").tag("instance", "2").gauge().value();
    metrics.consumerStopped(2);

    // Then: the gauge exposes both lifecycle states exactly
    assertThat(running).isEqualTo(1.0);
    assertThat(registry.get("ingester_consumer_running").tag("instance", "2").gauge().value())
        .isZero();
  }

  @Test
  void givenPollsAndWriteOutcomes_whenRecorded_thenThroughputDistributionsAreExact() {
    // Given: one registered instance and deterministic poll sizes
    metrics.registerInstance(3);

    // When: two polls, two writes, and one failure are recorded
    metrics.recordsPolled(3, 5);
    metrics.recordsPolled(3, 2);
    metrics.recordWritten(3);
    metrics.recordWritten(3);
    metrics.parquetFileCommitted(3);
    metrics.parquetFileCommitted(3);
    metrics.writeFailure(3);

    // Then: summaries and counters expose the exact observed work
    var polls = registry.get("ingester_poll_records").tag("instance", "3").summary();
    assertThat(polls.count()).isEqualTo(2);
    assertThat(polls.totalAmount()).isEqualTo(7.0);
    assertThat(polls.max()).isEqualTo(5.0);
    assertThat(
            registry.get("ingester_records_written_total").tag("instance", "3").counter().count())
        .isEqualTo(2.0);
    assertThat(
            registry
                .get("ingester_parquet_files_committed_total")
                .tag("instance", "3")
                .counter()
                .count())
        .isEqualTo(2.0);
    assertThat(registry.get("ingester_write_failures_total").tag("instance", "3").counter().count())
        .isEqualTo(1.0);
  }

  @Test
  void givenKnownInvalidRecords_whenRecorded_thenBoundedDlqReasonsRemainSeparate() {
    // Given: one registered instance and the supported failure classifications
    metrics.registerInstance(4);

    // When: validation, deserialization, unsupported-schema, and late failures are recorded
    metrics.invalidRecord(4, DlqReason.VALIDATION_ERROR);
    metrics.invalidRecord(4, DlqReason.VALIDATION_ERROR);
    metrics.invalidRecord(4, DlqReason.DESERIALIZATION_ERROR);
    metrics.invalidRecord(4, DlqReason.UNSUPPORTED_SCHEMA);
    metrics.invalidRecord(4, DlqReason.LATE_RECORD);

    // Then: each bounded reason has its exact independent count
    assertThat(
            registry
                .get("ingester_dlq_records_total")
                .tags("instance", "4", "reason", "VALIDATION_ERROR")
                .counter()
                .count())
        .isEqualTo(2.0);
    assertThat(
            registry
                .get("ingester_dlq_records_total")
                .tags("instance", "4", "reason", "DESERIALIZATION_ERROR")
                .counter()
                .count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get("ingester_dlq_records_total")
                .tags("instance", "4", "reason", "UNSUPPORTED_SCHEMA")
                .counter()
                .count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get("ingester_dlq_records_total")
                .tags("instance", "4", "reason", "LATE_RECORD")
                .counter()
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void givenSuccessfulRollover_whenRecorded_thenReasonSizeAndCountAreUpdatedTogether() {
    // Given: one registered instance and a record-count rollover
    metrics.registerInstance(5);

    // When: the finalized batch metrics are recorded
    metrics.rollover(5, BatchPolicy.FlushReason.RECORD_COUNT, 500, 1_048_576);

    // Then: the reason counter, distributions, gauges, and timestamp describe one batch
    assertThat(
            registry
                .get("ingester_rollover_total")
                .tags("instance", "5", "reason", "record_count")
                .counter()
                .count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get("ingester_batch_record_count")
                .tag("instance", "5")
                .summary()
                .totalAmount())
        .isEqualTo(500.0);
    assertThat(
            registry.get("ingester_batch_size_bytes").tag("instance", "5").summary().totalAmount())
        .isEqualTo(1_048_576.0);
    assertThat(
            registry.get("ingester_last_batch_record_count").tag("instance", "5").gauge().value())
        .isEqualTo(500.0);
    assertThat(
            registry
                .get("ingester_last_successful_rollover_timestamp")
                .tag("instance", "5")
                .gauge()
                .value())
        .isPositive();
  }

  @Test
  void givenRolloverTimer_whenStopped_thenOneDurationIsRecordedWithoutWaiting() {
    // Given: one registered instance and an active timer sample
    metrics.registerInstance(6);
    var sample = metrics.startRolloverTimer();

    // When: rollover timing is completed immediately
    metrics.stopRolloverTimer(6, sample);

    // Then: exactly one nonnegative duration is retained
    var timer = registry.get("ingester_rollover_duration_seconds").tag("instance", "6").timer();
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS)).isNotNegative();
  }

  @Test
  void givenUnregisteredInstance_whenMetricIsRecorded_thenUsageFailsClearly() {
    // Given: an instance identifier that has not been registered
    int instanceId = 99;

    // When: an instance-specific metric is recorded
    var invocation =
        org.assertj.core.api.Assertions.catchThrowable(() -> metrics.recordWritten(instanceId));

    // Then: the lifecycle contract reports the missing registration
    assertThat(invocation)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Instance 99 not registered");
  }

  @Test
  void givenMutableOperationalState_whenGaugeIsRegistered_thenInstanceValueRemainsObservable() {
    // Given: deterministic mutable state for a custom operational gauge
    AtomicInteger queueDepth = new AtomicInteger(3);

    // When: the gauge is registered and its source value changes
    metrics.registerGauge("ingester_test_queue_depth", 7, queueDepth, AtomicInteger::get);
    queueDepth.set(8);

    // Then: the gauge exposes the current value under the correct instance
    assertThat(registry.get("ingester_test_queue_depth").tag("instance", "7").gauge().value())
        .isEqualTo(8.0);
  }
}
