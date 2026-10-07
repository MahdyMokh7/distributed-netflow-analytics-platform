package com.nimbo.common.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroSchemaSupportTest {

  @Test
  void shouldDetectFieldsPresentInWriterSchema() {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"R","fields":[{"name":"A","type":"int"}]}
        """);
    GenericRecord record = new GenericData.Record(schema);

    assertThat(AvroSchemaSupport.hasField(record, "A")).isTrue();
    assertThat(AvroSchemaSupport.hasField(record, "B")).isFalse();
  }

  @Test
  void shouldUnwrapNullableUnion() {
    Schema schema = new Schema.Parser().parse("[\"null\",\"long\"]");

    assertThat(AvroSchemaSupport.unwrapNullable(schema).getType()).isEqualTo(Schema.Type.LONG);
    assertThat(AvroSchemaSupport.allowsNull(schema)).isTrue();
  }

  @Test
  void shouldRejectUnionWithMultipleNonNullTypes() {
    Schema schema = new Schema.Parser().parse("[\"null\",\"int\",\"long\"]");

    assertThatThrownBy(() -> AvroSchemaSupport.unwrapNullable(schema))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRecognizeAvroNumericWideningCompatibility() {
    assertThat(AvroSchemaSupport.isReaderWriterCompatible(
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.INT)))
        .isTrue();
  }
}
