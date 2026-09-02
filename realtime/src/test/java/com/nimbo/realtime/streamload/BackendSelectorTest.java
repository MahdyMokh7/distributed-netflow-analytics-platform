package com.nimbo.realtime.streamload;

import com.nimbo.realtime.config.StarRocksProperties;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackendSelectorTest {

    private StarRocksProperties properties(List<String> backends) {
        StarRocksProperties properties = new StarRocksProperties();
        properties.setStreamLoadBackends(backends);
        return properties;
    }

    @Test
    void shouldReturnConfiguredBackend() {
        BackendSelector selector = new BackendSelector(properties(List.of("backend-1")));

        assertThat(selector.nextBackend()).isEqualTo("backend-1");
    }

    @Test
    void shouldRotateBackendsInRoundRobinOrder() {
        BackendSelector selector = new BackendSelector(
                properties(List.of("backend-1", "backend-2", "backend-3"))
        );

        assertThat(selector.nextBackend()).isEqualTo("backend-1");
        assertThat(selector.nextBackend()).isEqualTo("backend-2");
        assertThat(selector.nextBackend()).isEqualTo("backend-3");
    }

    @Test
    void shouldWrapAroundAfterLastBackend() {
        BackendSelector selector = new BackendSelector(
                properties(List.of("backend-1", "backend-2"))
        );

        selector.nextBackend();
        selector.nextBackend();

        assertThat(selector.nextBackend()).isEqualTo("backend-1");
    }

    @Test
    void shouldFailWhenNoBackendConfigured() {
        assertThatThrownBy(() -> new BackendSelector(properties(List.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("At least one StarRocks backend must be configured");
    }

    @Test
    void shouldNotDependOnOriginalBackendListMutation() {
        List<String> backends = new ArrayList<>();
        backends.add("backend-1");

        StarRocksProperties properties = properties(backends);
        BackendSelector selector = new BackendSelector(properties);

        backends.add("backend-2");

        assertThat(selector.nextBackend()).isEqualTo("backend-1");
    }

    @Test
    void shouldHandleManyRotations() {
        BackendSelector selector = new BackendSelector(
                properties(List.of("backend-1", "backend-2"))
        );

        for (int i = 0; i < 100; i++) {
            assertThat(selector.nextBackend()).isIn("backend-1", "backend-2");
        }
    }
}
