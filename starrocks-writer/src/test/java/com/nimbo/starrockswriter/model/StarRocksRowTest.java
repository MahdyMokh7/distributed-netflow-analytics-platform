package com.nimbo.starrockswriter.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StarRocksRowTest {

  @Test
  void shouldCombineKafkaMetadataWithRequestedNetflowColumns() {
    StarRocksRow row =
        new StarRocksRow("topic", 3, 20L, 30L, Map.of("src", "10.0.0.1", "bytes", 100L));

    assertThat(row.values(List.of("src", "bytes", "new_field")))
        .containsExactly("topic", 3, 20L, 30L, "10.0.0.1", 100L, null);
  }

  @Test
  void shouldProtectOriginalNetflowMap() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("value", "before");

    StarRocksRow row = new StarRocksRow("topic", 0, 1L, 2L, source);
    source.put("value", "after");

    assertThat(row.netFlowValues()).containsEntry("value", "before");
  }
}
