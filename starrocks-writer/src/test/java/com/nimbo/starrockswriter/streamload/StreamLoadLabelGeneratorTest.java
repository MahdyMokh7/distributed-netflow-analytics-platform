package com.nimbo.starrockswriter.streamload;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class StreamLoadLabelGeneratorTest {

  private final StreamLoadLabelGenerator generator = new StreamLoadLabelGenerator();

  @Test
  void shouldGenerateLabelWithWorkerNumberOnlyWhenNoOffsets() {

    String label = generator.generateLabel(2, Map.of());

    assertThat(label).isEqualTo("nimbo-w2");
  }

  @Test
  void shouldAppendSinglePartitionOffset() {

    Map<TopicPartition, OffsetAndMetadata> offsets =
        Map.of(new TopicPartition("topic", 3), new OffsetAndMetadata(100));

    String label = generator.generateLabel(1, offsets);

    assertThat(label).isEqualTo("nimbo-w1-p3-o100");
  }

  @Test
  void shouldSortPartitionsAscending() {

    Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();

    offsets.put(new TopicPartition("topic", 5), new OffsetAndMetadata(500));

    offsets.put(new TopicPartition("topic", 1), new OffsetAndMetadata(100));

    offsets.put(new TopicPartition("topic", 3), new OffsetAndMetadata(300));

    String label = generator.generateLabel(4, offsets);

    assertThat(label).isEqualTo("nimbo-w4-p1-o100-p3-o300-p5-o500");
  }

  @Test
  void shouldHandleZeroOffset() {

    Map<TopicPartition, OffsetAndMetadata> offsets =
        Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(0));

    String label = generator.generateLabel(0, offsets);

    assertThat(label).isEqualTo("nimbo-w0-p0-o0");
  }

  @Test
  void shouldIgnoreTopicNameAndUsePartitionOnly() {

    Map<TopicPartition, OffsetAndMetadata> offsets =
        Map.of(new TopicPartition("another-topic", 2), new OffsetAndMetadata(50));

    String label = generator.generateLabel(7, offsets);

    assertThat(label).isEqualTo("nimbo-w7-p2-o50");
  }
}
