package com.nimbo.realtime.processing;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.PendingLoad;
import com.nimbo.realtime.model.StreamLoadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletionException;

@RequiredArgsConstructor
@Slf4j
public class InFlightLoadManager {

    private final StarRocksProperties properties;
    private final Deque<PendingLoad> pendingLoads = new ArrayDeque<>();

    public void add(PendingLoad pendingLoad) {
        pendingLoads.addLast(pendingLoad);
    }

    public boolean isBackpressured() {
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
                log.info("Completed Stream Load label={}, rows={}, status={}",
                        pendingLoad.label(), pendingLoad.rowCount(), result.status());
            }
            catch (CompletionException e) {
                log.error("Stream load failed label={}", pendingLoad.label(), e.getCause());
            }
        }
    }
}
