package com.nimbo.starrockswriter.streamload;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.model.StarRocksRow;
import com.nimbo.starrockswriter.model.StreamLoadResult;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StreamLoadRetryHandler {

  private static final String PUBLISH_TIMEOUT = "Publish Timeout";
  private final StarRocksProperties properties;
  private final StarRocksStreamLoadClient client;
  private final int workerNumber;
  private final StarRocksWriterMetrics metrics;

  public CompletableFuture<StreamLoadResult> sendRequest(List<StarRocksRow> rows, String label) {
    return sendAttempt(rows, label).whenComplete(
        (result, error) -> metrics.loadCompleted(workerNumber, outcome(result, error)));
  }

  private CompletableFuture<StreamLoadResult> sendAttempt(List<StarRocksRow> rows, String label) {
    return client.sendAsync(rows, label).handle((result, error) -> handle(rows, label, result, error))
        .thenCompose(Function.identity());
  }

  private CompletableFuture<StreamLoadResult> handle(
      List<StarRocksRow> rows, String label, StreamLoadResult result, Throwable error) {
    if (error == null && success(result)) {
      return CompletableFuture.completedFuture(result);
    }
    log.error("Stream Load failed. label={}, error={}, result={}", label, error, result);
    return retry(rows, label);
  }

  private boolean success(StreamLoadResult result) {
    return result.isSuccessful() || PUBLISH_TIMEOUT.equalsIgnoreCase(result.status());
  }

  private CompletableFuture<StreamLoadResult> retry(List<StarRocksRow> rows, String label) {
    return CompletableFuture.runAsync(() -> {}, CompletableFuture.delayedExecutor(
                properties.getRetryBackoffMs(), TimeUnit.MILLISECONDS))
        .thenCompose(ignored -> sendAttempt(rows, label));
  }

  private String outcome(StreamLoadResult result, Throwable error) {
    if (error != null || result == null) {
      return "failed";
    }
    return PUBLISH_TIMEOUT.equalsIgnoreCase(result.status()) ? "publish_timeout" : "success";
  }
}
