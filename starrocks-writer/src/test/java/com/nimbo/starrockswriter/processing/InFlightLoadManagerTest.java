package com.nimbo.starrockswriter.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.model.PendingLoad;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class InFlightLoadManagerTest {

  @Test
  void newManagerIsNotBackpressured() {
    InFlightLoadManager manager = manager(2);

    assertThat(manager.isBackpressured()).isFalse();
  }

  @Test
  void shouldBecomeBackpressuredWhenLimitReached() {
    InFlightLoadManager manager = manager(1);

    manager.add(pendingLoad());

    assertThat(manager.isBackpressured()).isTrue();
  }

  @Test
  void shouldAllowMultiplePendingLoadsBelowLimit() {
    InFlightLoadManager manager = manager(5);

    manager.add(pendingLoad());
    manager.add(pendingLoad());

    assertThat(manager.isBackpressured()).isFalse();
  }

  private InFlightLoadManager manager(int maxRequests) {
    StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(new SimpleMeterRegistry());
    metrics.registerWorker(1);
    return new InFlightLoadManager(properties(maxRequests), 1, metrics);
  }

  private StarRocksProperties properties(int maxRequests) {
    StarRocksProperties properties = new StarRocksProperties();
    properties.setMaxInFlightRequests(maxRequests);
    return properties;
  }

  private PendingLoad pendingLoad() {
    return new PendingLoad("label", 10, Map.of(), CompletableFuture.completedFuture(null));
  }
}
