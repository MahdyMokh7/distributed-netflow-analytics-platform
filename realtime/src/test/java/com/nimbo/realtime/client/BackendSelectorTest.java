package com.nimbo.realtime.client;

import com.nimbo.realtime.config.StarRocksProperties;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BackendSelectorTest {

    @Test
    void cyclesThroughBackendsInRoundRobinOrder() {
        StarRocksProperties properties = new StarRocksProperties();
        properties.setStreamLoadBackends(List.of("http://be1:8040", "http://be2:8040"));

        BackendSelector selector = new BackendSelector(properties);

        assertEquals("http://be1:8040", selector.nextBackend());
        assertEquals("http://be2:8040", selector.nextBackend());
        assertEquals("http://be1:8040", selector.nextBackend());
    }

    @Test
    void rejectsEmptyBackendList() {
        StarRocksProperties properties = new StarRocksProperties();
        properties.setStreamLoadBackends(List.of());

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new BackendSelector(properties)
        );

        assertEquals("At least one StarRocks backend must be configured", exception.getMessage());
    }
}
