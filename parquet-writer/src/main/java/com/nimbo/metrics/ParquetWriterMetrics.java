package com.nimbo.metrics;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import com.nimbo.storage.service.batching.BatchPolicy;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ParquetWriterMetrics {

    private final MeterRegistry registry;
    private final Map<Integer, InstanceState> instances = new ConcurrentHashMap<>();
    private static final String INSTANCE_TAG = "instance";
    private static final String REASON_TAG = "reason";
    private static final List<String> ROLLOVER_REASONS =
            List.of("record_count", "size", "age", "time_bucket_boundary", "schema_change", "lifecycle");
    private static final List<String> DLQ_REASONS =
            List.of("VALIDATION_ERROR", "DESERIALIZATION_ERROR", "UNSUPPORTED_SCHEMA");

    public ParquetWriterMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void registerInstance(int instanceId) {
        instances.computeIfAbsent(instanceId, id -> {
            InstanceState s = new InstanceState(registry, id);
            String instance = String.valueOf(id);

            for (String reason : ROLLOVER_REASONS) {
                s.rolloverCounters.put(reason, registry.counter("ingester_rollover_total",
                        INSTANCE_TAG, instance, REASON_TAG, reason));
            }
            for (String reason : DLQ_REASONS) {
                s.dlqCounters.put(reason, registry.counter("ingester_dlq_records_total",
                        INSTANCE_TAG, instance, REASON_TAG, reason));
            }
            return s;
        });
    }

    public void bindKafkaConsumer(KafkaConsumer<?, ?> consumer) {
        new KafkaClientMetrics(consumer).bindTo(registry);
    }

    public <T> void registerGauge(String name, int instanceId, T target, ToDoubleFunction<T> valueFn) {
        Gauge.builder(name, target, valueFn)
                .tags(Tags.of(INSTANCE_TAG, String.valueOf(instanceId)))
                .register(registry);
    }

    private InstanceState state(int instanceId) {
        InstanceState s = instances.get(instanceId);
        if (s == null) {
            throw new IllegalStateException(
                    "Instance " + instanceId + " not registered — call registerInstance() first");
        }
        return s;
    }

    public void consumerStarted(int instanceId) {
        state(instanceId).running.set(1);
    }

    public void consumerStopped(int instanceId) {
        state(instanceId).running.set(0);
    }

    public void recordsPolled(int instanceId, int count) {
        state(instanceId).pollRecords.record(count);
    }

    public void recordWritten(int instanceId) {
        state(instanceId).recordsWritten.increment();
    }

    public void writeFailure(int instanceId) {
        state(instanceId).writeFailures.increment();
    }

    public void invalidRecord(int instanceId, String reason) {
        InstanceState s = state(instanceId);
        Counter c = s.dlqCounters.get(reason);
        if (c != null) {
            c.increment();
        } else {
            log.warn("Unrecognized DLQ reason '{}' for instance {} — falling back to dynamic counter lookup",
                    reason, instanceId);

            registry.counter("ingester_dlq_records_total",
                    INSTANCE_TAG, String.valueOf(instanceId), REASON_TAG, reason).increment();
        }
    }

    public void rollover(int instanceId, BatchPolicy.FlushReason reason, long recordCount, long sizeBytes) {
        InstanceState s = state(instanceId);
        String reasonKey = reason.name().toLowerCase();
        Counter c = s.rolloverCounters.get(reasonKey);
        if (c != null) {
            c.increment();
        } else {
            log.warn("Unrecognized rollover reason '{}' for instance {} — falling back to dynamic counter lookup",
                    reasonKey, instanceId);

            registry.counter("ingester_rollover_total",
                    INSTANCE_TAG, String.valueOf(instanceId), REASON_TAG, reasonKey).increment();
        }
        s.batchRecordCount.record(recordCount);
        s.batchSizeBytes.record(sizeBytes);
        s.lastBatchRecordCount.set(recordCount);
        s.lastBatchSizeBytes.set(sizeBytes);
        s.lastSuccessfulRollover.set(System.currentTimeMillis() / 1000);
    }

    public Timer.Sample startRolloverTimer() {
        return Timer.start(registry);
    }

    public void stopRolloverTimer(int instanceId, Timer.Sample sample) {
        sample.stop(state(instanceId).rolloverDuration);
    }

    private static class InstanceState {
        final Counter recordsWritten;
        final Counter writeFailures;
        final Timer rolloverDuration;
        final DistributionSummary pollRecords;
        final DistributionSummary batchRecordCount;
        final DistributionSummary batchSizeBytes;
        final AtomicInteger running = new AtomicInteger(0);
        final AtomicLong lastSuccessfulRollover = new AtomicLong(0);
        final AtomicLong lastBatchRecordCount = new AtomicLong(0);
        final AtomicLong lastBatchSizeBytes = new AtomicLong(0);
        final Map<String, Counter> rolloverCounters = new ConcurrentHashMap<>();
        final Map<String, Counter> dlqCounters = new ConcurrentHashMap<>();

        InstanceState(MeterRegistry registry, int instanceId) {
            Tags tags = Tags.of(INSTANCE_TAG, String.valueOf(instanceId));

            recordsWritten = Counter.builder("ingester_records_written_total")
                    .tags(tags).register(registry);

            writeFailures = Counter.builder("ingester_write_failures_total")
                    .tags(tags).register(registry);

            rolloverDuration = Timer.builder("ingester_rollover_duration_seconds")
                    .tags(tags).register(registry);

            pollRecords = DistributionSummary.builder("ingester_poll_records")
                    .tags(tags).register(registry);

            batchRecordCount = DistributionSummary.builder("ingester_batch_record_count")
                    .tags(tags).register(registry);

            batchSizeBytes = DistributionSummary.builder("ingester_batch_size_bytes")
                    .tags(tags).register(registry);

            Gauge.builder("ingester_consumer_running", running, AtomicInteger::get)
                    .tags(tags).register(registry);

            Gauge.builder("ingester_last_successful_rollover_timestamp", lastSuccessfulRollover, AtomicLong::get)
                    .tags(tags).register(registry);

            Gauge.builder("ingester_last_batch_record_count", lastBatchRecordCount, AtomicLong::get)
                    .tags(tags).register(registry);

            Gauge.builder("ingester_last_batch_size_bytes", lastBatchSizeBytes, AtomicLong::get)
                    .tags(tags).register(registry);
        }
    }
}
