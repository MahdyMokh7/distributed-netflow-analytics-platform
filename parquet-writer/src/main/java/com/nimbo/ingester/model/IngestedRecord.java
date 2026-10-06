package com.nimbo.ingester.model;

import java.util.Objects;
import org.apache.avro.generic.GenericRecord;

public record IngestedRecord(
    GenericRecord record,
    String topic,
    int partition,
    long offset,
    long kafkaTimestamp,
    int serializedSizeBytes) {

  public IngestedRecord {
    Objects.requireNonNull(record, "record must not be null");
    Objects.requireNonNull(topic, "topic must not be null");

    if (kafkaTimestamp < 0) {
      throw new IllegalArgumentException("kafkaTimestamp must not be negative");
    }

    if (serializedSizeBytes < 0) {
      throw new IllegalArgumentException("serializedSizeBytes must not be negative");
    }
  }
}
