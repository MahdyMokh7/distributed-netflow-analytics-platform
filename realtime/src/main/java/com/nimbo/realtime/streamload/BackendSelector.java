package com.nimbo.realtime.streamload;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.metrics.RealtimeMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class BackendSelector {

    private final List<String> backends;
    private final AtomicInteger index = new AtomicInteger();
    private final RealtimeMetrics metrics;

    public BackendSelector(StarRocksProperties properties) {
        this(properties, null);
    }

    @Autowired
    public BackendSelector(StarRocksProperties properties, RealtimeMetrics metrics) {

        this.metrics = metrics;

        this.backends = List.copyOf(
                properties.getStreamLoadBackends()
        );

        if (backends.isEmpty()) {
            throw new IllegalStateException(
                    "At least one StarRocks backend must be configured"
            );
        }
    }

    public String nextBackend() {
        int position = Math.floorMod(
                index.getAndIncrement(),
                backends.size()
        );
        if (metrics != null) metrics.backendSelected(position);
        return backends.get(position);
    }
}
