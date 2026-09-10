package com.nimbo.realtime.processing;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.PendingLoad;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.metrics.RealtimeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletionException;

@RequiredArgsConstructor
@Slf4j
public class InFlightLoadManager {

    private final StarRocksProperties properties;
    private final int workerNumber;
    private final RealtimeMetrics metrics;
    private final Deque<PendingLoad> pendingLoads = new ArrayDeque<>();

    public void add(PendingLoad pendingLoad) {
        pendingLoads.addLast(pendingLoad);
        metrics.futureSubmitted(workerNumber);
        pendingLoad.future().whenComplete((result, error) -> {
            pendingLoad.markCompleted();
            metrics.futureCompleted(workerNumber, error != null);
        });
        metrics.setInFlight(workerNumber, pendingLoads.size());
        updateOldestAge();
    }

    public boolean isBackpressured() {
        updateOldestAge();
        return pendingLoads.size() >= properties.getMaxInFlightRequests();
    }

    public void drain(KafkaConsumer<byte[], byte[]> consumer) {
        while (!pendingLoads.isEmpty()) {
            PendingLoad pendingLoad = pendingLoads.peekFirst();

            if (!pendingLoad.future().isDone()) {
                return;
            }
            try {
                StreamLoadResult result = pendingLoad.future().join();
                consumer.commitSync(pendingLoad.offsets());
                pendingLoads.removeFirst();
                metrics.futureDrained(workerNumber, false, pendingLoad.drainDelayNanos());
                metrics.setInFlight(workerNumber, pendingLoads.size());
                metrics.commit(workerNumber, "success", pendingLoad.rowCount());
                log.info("Completed Stream Load label={}, rows={}, status={}",
                        pendingLoad.label(), pendingLoad.rowCount(), result.status());
            }
            catch (CompletionException e) {
                metrics.setOldestInFlightAge(workerNumber, pendingLoad.ageSeconds());
                log.error("Stream load failed label={}", pendingLoad.label(), e.getCause());
                return;
            } catch (RuntimeException e) {
                metrics.commit(workerNumber, "failure", 0);
                throw e;
            }
        }
        updateOldestAge();
    }

    private void updateOldestAge() {
        PendingLoad oldest = pendingLoads.peekFirst();
        metrics.setOldestInFlightAge(workerNumber, oldest == null ? 0 : oldest.ageSeconds());
    }
}
