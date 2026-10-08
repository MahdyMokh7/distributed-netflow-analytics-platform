package com.nimbo.starrockswriter.processing;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.model.PendingLoad;
import com.nimbo.starrockswriter.model.StreamLoadResult;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@RequiredArgsConstructor
@Slf4j
public class InFlightLoadManager {

  private final StarRocksProperties properties;
  private final int workerNumber;
  private final StarRocksWriterMetrics metrics;
  private final Deque<PendingLoad> pendingLoads = new ArrayDeque<>();

  public void add(PendingLoad pendingLoad) {
    pendingLoads.addLast(pendingLoad);
    pendingLoad
        .future()
        .whenComplete(
            (result, error) -> {
              pendingLoad.markCompleted();
              metrics.futureCompleted(workerNumber, error != null);
            });
    metrics.setInFlight(workerNumber, pendingLoads.size());
  }

  public boolean isBackpressured() {
    return pendingLoads.size() >= properties.getMaxInFlightRequests();
  }

  public void drain(KafkaConsumer<?, ?> consumer) {
    drain(consumer, false);
  }

  public void awaitAndDrain(KafkaConsumer<?, ?> consumer) {
    drain(consumer, true);
  }

  private void drain(KafkaConsumer<?, ?> consumer, boolean awaitCompletion) {
    while (!pendingLoads.isEmpty()) {
      PendingLoad pendingLoad = pendingLoads.peekFirst();

      if (!awaitCompletion && !pendingLoad.future().isDone()) {
        return;
      }
      try {
        StreamLoadResult result = pendingLoad.future().join();
        consumer.commitSync(pendingLoad.offsets());
        pendingLoads.removeFirst();
        metrics.futureDrained(workerNumber);
        metrics.setInFlight(workerNumber, pendingLoads.size());
        metrics.commit(workerNumber, "success", pendingLoad.rowCount());
        log.info("Completed Stream Load label={}, rows={}, {}, inFlights={}", pendingLoad.label(),
            pendingLoad.rowCount(), result.status(), pendingLoads.size());
      } catch (CompletionException e) {
        log.error("Stream load failed label={}", pendingLoad.label(), e.getCause());
        return;
      } catch (RuntimeException e) {
        metrics.commit(workerNumber, "failure", 0);
        throw e;
      }
    }
  }
}
