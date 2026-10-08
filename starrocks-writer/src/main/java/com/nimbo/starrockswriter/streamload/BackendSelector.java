package com.nimbo.starrockswriter.streamload;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class BackendSelector {

  private final List<String> backends;
  private final AtomicInteger index = new AtomicInteger();

  public BackendSelector(StarRocksProperties properties) {
    this.backends = List.copyOf(properties.getStreamLoadBackends());

    if (backends.isEmpty()) {
      throw new IllegalStateException("At least one StarRocks backend must be configured");
    }
  }

  public String nextBackend() {
    return backends.get(Math.floorMod(index.getAndIncrement(), backends.size()));
  }
}
