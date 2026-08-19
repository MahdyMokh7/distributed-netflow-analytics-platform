package com.nimbo.ingester.consumer;

import com.nimbo.storage.model.TopicPartitionKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class KafkaOffsetCommitter {

    public void commit(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                       Map<TopicPartitionKey, Long> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        offsets.forEach((key, offset) -> {
            TopicPartition partition = new TopicPartition(key.topic(), key.partition());
            commitMap.put(partition, new OffsetAndMetadata(offset));
        });

        // kafkaConsumer.commitSync(commitMap);
        kafkaConsumer.commitAsync(commitMap, (committedOffsets, exception) -> {
            if (exception != null) {
                log.error("Async commit failed for offsets: {}", committedOffsets, exception);
            }
        });
    }
}