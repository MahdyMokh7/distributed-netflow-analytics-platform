package com.nimbo.realtime.streamload;

import com.nimbo.realtime.config.StarRocksProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class BackendSelector {

    private final List<String> backends;
    private final AtomicInteger index = new AtomicInteger();

    public BackendSelector(StarRocksProperties properties) {

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
        return backends.get(position);
    }
}
