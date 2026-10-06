package com.nimbo.ingester.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.support.NetFlowTestData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IngestedRecordTest {

  private GenericRecord record;
  private static final long KAFKA_TIMESTAMP = 1_700_000_000_000L;

  @BeforeEach
  void setUp() {
    record = new NetFlowTestData().validRecord();
  }

  @Test
  void givenValidKafkaMetadata_whenCreated_thenRecordPreservesOffsetAndSize() {
    // Given: deterministic Kafka metadata and a production-schema record
    String topic = "netflow-v2";

    // When: the consumer creates an ingested record
    IngestedRecord ingested = new IngestedRecord(record, topic, 2, 41L, KAFKA_TIMESTAMP, 1_024);

    // Then: storage receives the exact payload and Kafka coordinates
    assertThat(ingested.record()).isSameAs(record);
    assertThat(ingested.topic()).isEqualTo(topic);
    assertThat(ingested.partition()).isEqualTo(2);
    assertThat(ingested.offset()).isEqualTo(41L);
    assertThat(ingested.serializedSizeBytes()).isEqualTo(1_024);
  }

  @Test
  void givenZeroLengthPayload_whenCreated_thenSizeRemainsValid() {
    // Given: a valid record representing a zero-length metadata edge case
    int serializedSize = 0;

    // When: the record is created at the lower supported size boundary
    IngestedRecord ingested =
        new IngestedRecord(record, "netflow-v2", 0, 0L, KAFKA_TIMESTAMP, serializedSize);

    // Then: zero is retained as a permitted nonnegative size
    assertThat(ingested.serializedSizeBytes()).isZero();
  }

  @Test
  void givenNullRequiredComponents_whenCreated_thenConstructionFailsFast() {
    // Given: null payload and topic values
    GenericRecord missingRecord = null;

    // When: callers construct records without required components
    var missingRecordFailure =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> new IngestedRecord(missingRecord, "netflow-v2", 0, 0L, KAFKA_TIMESTAMP, 1));
    var missingTopicFailure =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> new IngestedRecord(record, null, 0, 0L, KAFKA_TIMESTAMP, 1));

    // Then: both invalid records fail with specific invariant messages
    assertThat(missingRecordFailure)
        .isInstanceOf(NullPointerException.class)
        .hasMessage("record must not be null");
    assertThat(missingTopicFailure)
        .isInstanceOf(NullPointerException.class)
        .hasMessage("topic must not be null");
  }

  @Test
  void givenNegativeSerializedSize_whenCreated_thenConstructionRejectsInvalidMetadata() {
    // Given: a serialized payload size below zero
    int invalidSize = -1;

    // When: the consumer attempts to preserve invalid size metadata
    var invocation =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> new IngestedRecord(record, "netflow-v2", 0, 0L, KAFKA_TIMESTAMP, invalidSize));

    // Then: construction rejects the impossible serialized size
    assertThat(invocation)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("serializedSizeBytes must not be negative");
  }
}
