package com.nimbo.realtime.processing;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.PendingLoad;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class InFlightLoadManagerTest {

    private StarRocksProperties properties(int maxRequests) {
        StarRocksProperties properties = new StarRocksProperties();
        properties.setMaxInFlightRequests(maxRequests);
        return properties;
    }

    private PendingLoad pendingLoad() {
        return new PendingLoad(
                "label", 10, Map.of(),
                CompletableFuture.completedFuture(null)
        );
    }

    @Test
    void shouldNotBeBackpressuredInitially() {
        InFlightLoadManager manager = new InFlightLoadManager(properties(2));

        assertThat(manager.isBackpressured()).isFalse();
    }

    @Test
    void shouldBecomeBackpressuredWhenLimitReached() {
        InFlightLoadManager manager = new InFlightLoadManager(properties(1));

        manager.add(pendingLoad());

        assertThat(manager.isBackpressured()).isTrue();
    }

    @Test
    void shouldAllowMultiplePendingLoads() {
        InFlightLoadManager manager = new InFlightLoadManager(properties(5));

        manager.add(pendingLoad());
        manager.add(pendingLoad());

        assertThat(manager.isBackpressured()).isFalse();
    }
}
