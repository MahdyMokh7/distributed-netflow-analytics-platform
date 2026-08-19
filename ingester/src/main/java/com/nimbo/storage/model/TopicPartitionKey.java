package com.nimbo.storage.model;

import java.util.Objects;

public record TopicPartitionKey(
        String topic,
        int partition
) {

    public TopicPartitionKey {
        Objects.requireNonNull(topic, "topic must not be null");

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "partition must not be negative"
            );
        }
    }
}