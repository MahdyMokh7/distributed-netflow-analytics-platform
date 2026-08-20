package com.nimbo.ingester.model;


import org.apache.avro.generic.GenericRecord;

import java.util.Objects;

public record IngestedRecord(
        GenericRecord record,
        String topic,
        int partition,
        long offset,
        int serializedSizeBytes
) {

    public IngestedRecord {
        Objects.requireNonNull(record, "record must not be null");
        Objects.requireNonNull(topic, "topic must not be null");

        if (serializedSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "serializedSizeBytes must not be negative"
            );
        }
    }
}