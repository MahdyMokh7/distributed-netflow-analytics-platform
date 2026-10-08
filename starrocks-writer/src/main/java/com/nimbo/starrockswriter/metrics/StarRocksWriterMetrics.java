package com.nimbo.starrockswriter.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

@Component
public class StarRocksWriterMetrics {

  private static final String WORKER = "worker";
  private final MeterRegistry registry;
  private final Map<Integer, WorkerState> workers = new ConcurrentHashMap<>();
  private final Map<Integer, KafkaClientMetrics> kafkaBinders = new ConcurrentHashMap<>();

  public StarRocksWriterMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  public void registerWorker(int worker) {
    workers.computeIfAbsent(worker, id -> new WorkerState(registry, id));
  }

  public void bindKafkaConsumer(int worker, KafkaConsumer<?, ?> consumer) {
    KafkaClientMetrics binder = new KafkaClientMetrics(consumer);
    KafkaClientMetrics old = kafkaBinders.put(worker, binder);
    if (old != null) {
      old.close();
    }
    binder.bindTo(registry);
  }

  public void unbindKafkaConsumer(int worker) {
    KafkaClientMetrics binder = kafkaBinders.remove(worker);
    if (binder != null) {
      binder.close();
    }
  }

  public void workerStarted(int worker) {
    state(worker).running.set(1);
  }

  public void workerStopped(int worker) {
    state(worker).running.set(0);
  }

  public void recordProcessed(int worker, String outcome) {
    counter("nimbo.StarRocksWriter.records.processed", worker, "outcome", outcome).increment();
  }

  public void batchSubmitted(int worker, int rows) {
    state(worker).batchRows.record(rows);
  }

  public void loadCompleted(int worker, String outcome) {
    counter("nimbo.StarRocksWriter.stream.load.completed", worker, "outcome", outcome).increment();
  }

  public void commit(int worker, String outcome, int rows) {
    counter("nimbo.StarRocksWriter.kafka.commits", worker, "outcome", outcome).increment();
    if ("success".equals(outcome)) {
      state(worker).committedRows.increment(rows);
    }
  }

  public void setInFlight(int worker, int count) {
    state(worker).inFlight.set(count);
  }

  public void futureCompleted(int worker, boolean failed) {
    WorkerState state = state(worker);
    state.completedFutures.incrementAndGet();
    if (failed) {
      state.failedFutures.increment();
    }
  }

  public void futureDrained(int worker) {
    state(worker).completedFutures.decrementAndGet();
  }

  private Counter counter(String name, int worker, String key, String value) {
    return registry.counter(name, WORKER, String.valueOf(worker), key, value);
  }

  private WorkerState state(int worker) {
    WorkerState state = workers.get(worker);
    if (state == null) {
      throw new IllegalStateException("Worker " + worker + " has not been registered");
    }
    return state;
  }

  private static final class WorkerState {

    final AtomicInteger running = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger completedFutures = new AtomicInteger();
    final Counter committedRows;
    final Counter failedFutures;
    final DistributionSummary batchRows;

    WorkerState(MeterRegistry registry, int worker) {
      Tags tags = Tags.of(WORKER, String.valueOf(worker));
      committedRows = Counter.builder("nimbo.StarRocksWriter.committed.rows").tags(tags).register(registry);
      failedFutures = Counter.builder("nimbo.StarRocksWriter.futures.failed").tags(tags).register(registry);
      batchRows = DistributionSummary.builder("nimbo.StarRocksWriter.batch.rows").tags(tags).register(registry);
      Gauge.builder("nimbo.StarRocksWriter.consumer.running", running, AtomicInteger::get).tags(tags)
          .register(registry);
      Gauge.builder("nimbo.StarRocksWriter.inflight.loads", inFlight, AtomicInteger::get).tags(tags)
          .register(registry);
      Gauge.builder("nimbo.StarRocksWriter.futures.completed.waiting.drain", completedFutures, AtomicInteger::get)
          .tags(tags).register(registry);
    }
  }
}
