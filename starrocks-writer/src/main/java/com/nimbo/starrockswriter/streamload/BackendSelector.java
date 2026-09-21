package com.nimbo.starrockswriter.streamload;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class BackendSelector {

    private final List<String> backends;
    private final AtomicInteger index = new AtomicInteger();
    private final StarRocksWriterMetrics metrics;

    public BackendSelector(StarRocksProperties properties) {
        this(properties, null);
    }

    @Autowired
    public BackendSelector(StarRocksProperties properties, StarRocksWriterMetrics metrics) {

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
