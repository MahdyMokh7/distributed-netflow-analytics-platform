package com.nimbo.starrockswriter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.starrockswriter.support.TestExtension;
import com.nimbo.starrockswriter.support.TestSchemaRegistry;
import com.nimbo.starrockswriter.support.annotations.TestAnnotations;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

@TestAnnotations
class StarRocksWriterIT {

  private final TestExtension infrastructure;

  StarRocksWriterIT(TestExtension infrastructure) {
    this.infrastructure = infrastructure;
  }

  @Test
  void consumesMixedKafkaRecordsAndPersistsValidRecords() throws SQLException {
    infrastructure.kafka().awaitConsumerGroupMembers(2);

    Schema schema = loadAvroSchema();
    long firstTimestamp = System.currentTimeMillis();
    GenericRecord first = validNetflowRecord(schema, "192.0.2.10", 1234L);
    GenericRecord invalid = validNetflowRecord(schema, "not-an-ip", 9999L);
    GenericRecord second = validNetflowRecord(schema, "192.0.2.11", 5678L);
    GenericRecord otherPartitionFirst = validNetflowRecord(schema, "192.0.2.20", 2000L);
    GenericRecord otherPartitionSecond = validNetflowRecord(schema, "192.0.2.21", 2100L);

    assertThat(produce(first, schema, firstTimestamp)).isZero();
    assertThat(produce(invalid, schema, firstTimestamp + 1)).isEqualTo(1L);
    assertThat(produceBytes(new byte[0], firstTimestamp + 2)).isEqualTo(2L);
    assertThat(produce(second, schema, firstTimestamp + 3)).isEqualTo(3L);
    assertThat(produce(otherPartitionFirst, schema, firstTimestamp + 10, 1)).isZero();
    assertThat(produce(otherPartitionSecond, schema, firstTimestamp + 11, 1)).isEqualTo(1L);

    infrastructure.starRocks().awaitRowCount(4);
    infrastructure.kafka().awaitCommittedOffset(0, 4L);
    infrastructure.kafka().awaitCommittedOffset(1, 2L);
    assertPersistedRow("192.0.2.10", 0, 0L, firstTimestamp, 1234L);
    assertPersistedRow("192.0.2.11", 0, 3L, firstTimestamp + 3, 5678L);
    assertPersistedRow("192.0.2.20", 1, 0L, firstTimestamp + 10, 2000L);
    assertPersistedRow("192.0.2.21", 1, 1L, firstTimestamp + 11, 2100L);
    assertThat(infrastructure.starRocks().rowCount("not-an-ip")).isZero();

    GenericRecord third = validNetflowRecord(schema, "192.0.2.12", 9012L);
    GenericRecord otherPartitionThird = validNetflowRecord(schema, "192.0.2.22", 2200L);
    assertThat(produce(third, schema, firstTimestamp + 4)).isEqualTo(4L);
    assertThat(produce(otherPartitionThird, schema, firstTimestamp + 12, 1)).isEqualTo(2L);

    infrastructure.starRocks().awaitRowCount(6);
    infrastructure.kafka().awaitCommittedOffset(0, 5L);
    infrastructure.kafka().awaitCommittedOffset(1, 3L);
    assertPersistedRow("192.0.2.12", 0, 4L, firstTimestamp + 4, 9012L);
    assertPersistedRow("192.0.2.22", 1, 2L, firstTimestamp + 12, 2200L);
  }

  @Test
  void flushesBatchOnTimeThresholdWhenBelowMaxBatchSize() throws SQLException {
    infrastructure.kafka().awaitConsumerGroupMembers(2);

    Schema schema = loadAvroSchema();
    long timestamp = System.currentTimeMillis();
    GenericRecord genericRecord = validNetflowRecord(schema, "192.0.2.30", 4242L);
    assertThat(produce(genericRecord, schema, timestamp)).isZero();

    infrastructure.starRocks().awaitRowCount(1);
    infrastructure.kafka().awaitCommittedOffset(0, 1L);
    assertPersistedRow("192.0.2.30", 0, 0L, timestamp, 4242L);
  }

  @Test
  void addsNullableColumnWhenSchemaV2ArrivesAndKeepsV1RowsReadable() throws SQLException {
    infrastructure.kafka().awaitConsumerGroupMembers(2);

    long timestamp = System.currentTimeMillis();
    Schema schemaV1 = infrastructure.schemaRegistry().schema();
    Schema schemaV2 = infrastructure.schemaRegistry().schemaV2();
    GenericRecord v1 = validNetflowRecord(schemaV1, "192.0.2.50", 1111L);
    GenericRecord v2 = validNetflowRecord(schemaV2, "192.0.2.51", 2222L);
    v2.put(TestSchemaRegistry.ADDED_FIELD, "added-by-v2");

    assertThat(produceBytes(serialize(v1, schemaV1, TestSchemaRegistry.SCHEMA_ID), timestamp, 0)).isZero();
    infrastructure.starRocks().awaitRowCount(1);

    assertThat(produceBytes(serialize(v2, schemaV2, TestSchemaRegistry.SCHEMA_ID_V2), timestamp + 1, 0))
        .isEqualTo(1L);
    infrastructure.starRocks().awaitRowCount(2);
    infrastructure.kafka().awaitCommittedOffset(0, 2L);

    assertAddedFieldValue("192.0.2.50", 1111L, null);
    assertAddedFieldValue("192.0.2.51", 2222L, "added-by-v2");
  }

  private void assertAddedFieldValue(String sourceIp, long expectedBytes, String expectedValue)
      throws SQLException {
    infrastructure.starRocks().withDatabase(connection -> {
      try (Statement statement = connection.createStatement();
          ResultSet row = statement.executeQuery(
              "SELECT in_bytes, integration_added_field FROM nimbo.netflow "
                  + "WHERE ipv4_src_addr = '" + sourceIp + "'")) {
        assertThat(row.next()).isTrue();
        assertThat(row.getLong("in_bytes")).isEqualTo(expectedBytes);
        assertThat(row.getString("integration_added_field")).isEqualTo(expectedValue);
        assertThat(row.next()).isFalse();
      }
    });
  }

  private long produce(GenericRecord record, Schema schema, long timestamp) {
    return produce(record, schema, timestamp, 0);
  }

  private long produce(GenericRecord record, Schema schema, long timestamp, int partition) {
    return produceBytes(serialize(record, schema), timestamp, partition);
  }

  private long produceBytes(byte[] payload, long timestamp) {
    return produceBytes(payload, timestamp, 0);
  }

  private long produceBytes(byte[] payload, long timestamp, int partition) {
    return infrastructure.kafka().produce(payload, timestamp, partition);
  }

  private void assertPersistedRow(String sourceIp, int expectedPartition, long expectedOffset,
      long expectedTimestamp, long expectedBytes) throws SQLException {
    infrastructure.starRocks().withDatabase(connection -> {
      try (Statement statement = connection.createStatement();
          ResultSet row = statement.executeQuery("""
              SELECT kafka_topic, kafka_partition, kafka_offset, PROCESS_TIME,
                 ipv4_dst_addr, in_bytes
              FROM nimbo.netflow
              WHERE ipv4_src_addr = '%s'
              """.formatted(sourceIp))) {
        assertThat(row.next()).isTrue();
        assertThat(row.getString("kafka_topic")).isEqualTo(infrastructure.kafka().topic());
        assertThat(row.getInt("kafka_partition")).isEqualTo(expectedPartition);
        assertThat(row.getLong("kafka_offset")).isEqualTo(expectedOffset);
        assertThat(row.getLong("PROCESS_TIME")).isEqualTo(expectedTimestamp);
        assertThat(row.getString("ipv4_dst_addr")).isEqualTo("198.51.100.20");
        assertThat(row.getLong("in_bytes")).isEqualTo(expectedBytes);
        assertThat(row.next()).isFalse();
      }
    });
  }

  private Schema loadAvroSchema() {
    return infrastructure.schemaRegistry().schema();
  }

  private GenericRecord validNetflowRecord(Schema schema, String sourceIp, long inBytes) {
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      Object value = switch (field.schema().getType()) {
        case STRING -> "value";
        case INT -> 1;
        case LONG -> 1L;
        case DOUBLE -> 1.0d;
        case UNION -> null;
        case FLOAT -> 1.0f;
        default -> throw new IllegalArgumentException("Unsupported test field: " + field.name());
      };
      record.put(field.name(), value);
    }
    record.put("IPV4_SRC_ADDR", sourceIp);
    record.put("IPV4_DST_ADDR", "198.51.100.20");
    record.put("IN_BYTES", inBytes);
    return record;
  }

  private byte[] serialize(GenericRecord record, Schema schema) {
    return serialize(record, schema, TestSchemaRegistry.SCHEMA_ID);
  }

  private byte[] serialize(GenericRecord record, Schema schema, int schemaId) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
      new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
      encoder.flush();

      byte[] avro = output.toByteArray();
      return ByteBuffer.allocate(5 + avro.length)
          .put((byte) 0)
          .putInt(schemaId)
          .put(avro)
          .array();
    } catch (IOException exception) {
      throw new UncheckedIOException("Could not serialize integration-test Avro record", exception);
    }
  }
}
