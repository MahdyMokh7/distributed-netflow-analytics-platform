package com.nimbo.starrockswriter.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StarRocksRowTest {

    @Test
    void shouldCombineKafkaMetadataAndNetflowValues() {
        StarRocksRow row = new StarRocksRow("topic", 3, 20L, 30L, List.of("src", 100));

        assertThat(row.values()).containsExactly("topic", 3, 20L, 30L, "src", 100);
    }

    @Test
    void shouldProtectOriginalNetflowList() {
        List<Object> source = new ArrayList<>();
        source.add("value");

        StarRocksRow row = new StarRocksRow("topic", 0, 1L, 2L, source);
        source.add("changed");

        assertThat(row.netFlowValues()).containsExactly("value");
    }

    @Test
    void valuesShouldCreateNewListEachTime() {
        StarRocksRow row = new StarRocksRow("topic", 0, 1L, 2L, List.of("a"));

        List<Object> first = row.values();
        List<Object> second = row.values();

        assertThat(first).isNotSameAs(second);
        assertThat(first).containsExactlyElementsOf(second);
    }
}
