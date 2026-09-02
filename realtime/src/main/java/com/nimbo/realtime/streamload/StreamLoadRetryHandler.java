package com.nimbo.realtime.streamload;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@RequiredArgsConstructor
@Slf4j
public class StreamLoadRetryHandler {

    private static final String PUBLISH_TIMEOUT = "Publish Timeout";
    private final StarRocksProperties properties;
    private final StarRocksStreamLoadClient client;

    public CompletableFuture<StreamLoadResult> sendRequest(List<StarRocksRow> rows, String label) {
        return client.sendAsync(rows, label)
                .handle((result, error) -> handle(rows, label, result, error))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<StreamLoadResult> handle(List<StarRocksRow> rows, String label, StreamLoadResult result, Throwable error) {
        if (error == null && success(result)) {
            return CompletableFuture.completedFuture(result);
        }

        log.error("Stream Load failed. label={}", label, error);
        return retry(rows, label);
    }

    private boolean success(StreamLoadResult result) {
        return result.isSuccessful() || PUBLISH_TIMEOUT.equalsIgnoreCase(result.status());
    }

    private CompletableFuture<StreamLoadResult> retry(List<StarRocksRow> rows, String label) {
        return CompletableFuture.runAsync(() -> {}, CompletableFuture.delayedExecutor(properties.getRetryBackoffMs(), TimeUnit.MILLISECONDS))
                .thenCompose(ignored -> sendRequest(rows, label));
    }
}
