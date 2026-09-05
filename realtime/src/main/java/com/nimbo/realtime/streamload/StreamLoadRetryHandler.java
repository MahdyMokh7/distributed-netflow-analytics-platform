package com.nimbo.realtime.streamload;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.metrics.RealtimeMetrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@RequiredArgsConstructor
@Slf4j
public class StreamLoadRetryHandler {

    private static final String PUBLISH_TIMEOUT = "Publish Timeout";
    private final StarRocksProperties properties;
    private final StarRocksStreamLoadClient client;
    private final int workerNumber;
    private final RealtimeMetrics metrics;

    public CompletableFuture<StreamLoadResult> sendRequest(List<StarRocksRow> rows, String label) {
        if (rows == null || rows.isEmpty()) metrics.emptyBatch();
        Timer.Sample sample = metrics.startLoadTimer();
        return sendAttempt(rows, label).whenComplete((result, error) -> {
            metrics.loadCompleted(workerNumber, outcome(result, error), sample);
            if (error == null && result != null) {
                metrics.streamLoadResult(workerNumber, result.numberLoadedRows());
            }
        });
    }

    private CompletableFuture<StreamLoadResult> sendAttempt(List<StarRocksRow> rows, String label) {
        metrics.loadAttempt(workerNumber);
        return client.sendAsync(rows, label)
                .handle((result, error) -> handle(rows, label, result, error))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<StreamLoadResult> handle(List<StarRocksRow> rows, String label, StreamLoadResult result, Throwable error) {
        if (error == null && success(result)) {
            return CompletableFuture.completedFuture(result);
        }

        log.error("Stream Load failed. label={}", label, error);
        metrics.loadRetry(workerNumber, retryReason(result, error));
        return retry(rows, label);
    }

    private boolean success(StreamLoadResult result) {
        return result.isSuccessful() || PUBLISH_TIMEOUT.equalsIgnoreCase(result.status());
    }

    private CompletableFuture<StreamLoadResult> retry(List<StarRocksRow> rows, String label) {
        return CompletableFuture.runAsync(() -> {}, CompletableFuture.delayedExecutor(properties.getRetryBackoffMs(), TimeUnit.MILLISECONDS))
                .thenCompose(ignored -> sendAttempt(rows, label));
    }

    private String outcome(StreamLoadResult result, Throwable error) {
        if (error != null || result == null) return "failed";
        return PUBLISH_TIMEOUT.equalsIgnoreCase(result.status()) ? "publish_timeout" : "success";
    }

    private String retryReason(StreamLoadResult result, Throwable error) {
        if (error != null) return "transport";
        return result == null ? "response" : "status";
    }
}
