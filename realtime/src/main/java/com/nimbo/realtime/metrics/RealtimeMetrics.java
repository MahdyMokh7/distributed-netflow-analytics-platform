package com.nimbo.realtime.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RealtimeMetrics {

    private static final String WORKER = "worker";
    private final MeterRegistry registry;
    private final Map<Integer, WorkerState> workers = new ConcurrentHashMap<>();
    private final Map<Integer, KafkaClientMetrics> kafkaBinders = new ConcurrentHashMap<>();

    public RealtimeMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void registerWorker(int worker) {
        workers.computeIfAbsent(worker, id -> new WorkerState(registry, id));
    }

    public void bindKafkaConsumer(int worker, KafkaConsumer<?, ?> consumer) {
        KafkaClientMetrics binder = new KafkaClientMetrics(consumer);
        KafkaClientMetrics old = kafkaBinders.put(worker, binder);
        if (old != null) old.close();
        binder.bindTo(registry);
    }

    public void unbindKafkaConsumer(int worker) {
        KafkaClientMetrics binder = kafkaBinders.remove(worker);
        if (binder != null) binder.close();
    }

    public void workerStarted(int worker) { state(worker).running.set(1); }
    public void workerStopped(int worker) { state(worker).running.set(0); }
    public void polled(int worker, int count) {
        WorkerState state = state(worker);
        state.recordsPolled.increment(count);
        state.pollRecords.record(count);
    }
    public Timer.Sample startPollTimer() { return Timer.start(registry); }
    public void pollCompleted(int worker, String type, Timer.Sample sample) {
        sample.stop(registry.timer("nimbo.realtime.kafka.poll.duration", WORKER, String.valueOf(worker),
                "type", type));
    }
    public void recordProcessed(int worker, String outcome) {
        counter("nimbo.realtime.records.processed", worker, "outcome", outcome).increment();
    }
    public void batchSubmitted(int worker, String reason, int rows) {
        counter("nimbo.realtime.batches.submitted", worker, "reason", reason).increment();
        state(worker).batchRows.record(rows);
    }
    public void loadAttempt(int worker) { state(worker).loadAttempts.increment(); }
    public void loadRetry(int worker, String reason) {
        counter("nimbo.realtime.stream.load.retries", worker, "reason", reason).increment();
    }
    public Timer.Sample startLoadTimer() { return Timer.start(registry); }
    public void loadCompleted(int worker, String outcome, Timer.Sample sample) {
        counter("nimbo.realtime.stream.load.completed", worker, "outcome", outcome).increment();
        sample.stop(registry.timer("nimbo.realtime.stream.load.duration", WORKER
                , String.valueOf(worker), "outcome", outcome));
    }
    public void streamLoadResult(int worker, Long rows) {
        if (rows != null && rows >= 0) state(worker).loadedRows.increment(rows);
    }
    public void commit(int worker, String outcome, int rows) {
        counter("nimbo.realtime.kafka.commits", worker, "outcome", outcome).increment();
        if ("success".equals(outcome)) state(worker).committedRows.increment(rows);
    }
    public void setOpenBatch(int worker, int rows, long ageSeconds) {
        WorkerState state = state(worker);
        state.openBatchRows.set(rows);
        state.openBatchAge.set(ageSeconds);
    }
    public void setInFlight(int worker, int count) { state(worker).inFlight.set(count); }
    public void futureSubmitted(int worker) { state(worker).activeFutures.incrementAndGet(); }
    public void futureCompleted(int worker, boolean failed) {
        WorkerState state = state(worker);
        state.activeFutures.decrementAndGet();
        state.completedFutures.incrementAndGet();
        if (failed) state.failedFutures.increment();
    }
    public void futureDrained(int worker, boolean failed, long delayNanos) {
        state(worker).completedFutures.decrementAndGet();
        registry.timer("nimbo.realtime.future.drain.delay", WORKER, String.valueOf(worker), "outcome",
                failed ? "failed" : "success").record(delayNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }
    public void setOldestInFlightAge(int worker, long ageSeconds) {
        state(worker).oldestInFlightAge.set(ageSeconds);
    }
    public void backpressure(int worker, boolean paused) {
        counter("nimbo.realtime.backpressure.events", worker,
                "event", paused ? "pause" : "resume").increment();
    }
    public void workerException(int worker, String type) {
        counter("nimbo.realtime.worker.exceptions", worker, "type", type).increment();
    }
    public void emptyBatch() {
        registry.counter("nimbo.realtime.empty.batches").increment();
    }
    public void backendSelected(int backend) {
        registry.counter("nimbo.realtime.backend.selections", "backend", "backend_" + backend).increment();
    }

    private Counter counter(String name, int worker, String key, String value) {
        return registry.counter(name, WORKER, String.valueOf(worker), key, value);
    }

    private WorkerState state(int worker) {
        WorkerState state = workers.get(worker);
        if (state == null) throw new IllegalStateException("Worker " + worker + " has not been registered");
        return state;
    }

    private static final class WorkerState {
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger openBatchRows = new AtomicInteger();
        final AtomicLong openBatchAge = new AtomicLong();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger activeFutures = new AtomicInteger();
        final AtomicInteger completedFutures = new AtomicInteger();
        final AtomicLong oldestInFlightAge = new AtomicLong();
        final Counter recordsPolled;
        final Counter loadAttempts;
        final Counter committedRows;
        final Counter loadedRows;
        final Counter failedFutures;
        final DistributionSummary pollRecords;
        final DistributionSummary batchRows;

        WorkerState(MeterRegistry registry, int worker) {
            Tags tags = Tags.of(WORKER, String.valueOf(worker));
            recordsPolled = Counter.builder("nimbo.realtime.records.polled").tags(tags).register(registry);
            loadAttempts = Counter.builder("nimbo.realtime.stream.load.attempts").tags(tags).register(registry);
            committedRows = Counter.builder("nimbo.realtime.committed.rows").tags(tags).register(registry);
            loadedRows = Counter.builder("nimbo.realtime.stream.load.rows").tags(tags).register(registry);
            failedFutures = Counter.builder("nimbo.realtime.futures.failed").tags(tags).register(registry);
            pollRecords = DistributionSummary.builder("nimbo.realtime.poll.records").tags(tags).register(registry);
            batchRows = DistributionSummary.builder("nimbo.realtime.batch.rows").tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.consumer.running", running,
                    AtomicInteger::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.open.batch.rows", openBatchRows,
                    AtomicInteger::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.open.batch.age.seconds", openBatchAge,
                    AtomicLong::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.inflight.loads", inFlight,
                    AtomicInteger::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.futures.active", activeFutures,
                    AtomicInteger::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.futures.completed.waiting.drain",
                    completedFutures, AtomicInteger::get).tags(tags).register(registry);
            Gauge.builder("nimbo.realtime.oldest.inflight.age.seconds",
                    oldestInFlightAge, AtomicLong::get).tags(tags).register(registry);
        }
    }
}
