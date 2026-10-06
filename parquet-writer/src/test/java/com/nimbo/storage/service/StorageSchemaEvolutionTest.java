package com.nimbo.storage.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbo.storage.exception.UnsupportedSchemaException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class StorageSchemaEvolutionTest {

  private final StorageSchemaEvolution evolution = new StorageSchemaEvolution();

  @Test
  void givenAddedField_whenOldVersionReturns_thenMergedSchemaDoesNotShrink() {
    Schema v1 = record("{\"name\":\"value\",\"type\":\"string\"}");
    Schema v2 =
        record(
            """
            {"name":"value","type":"string"},
            {"name":"extra","type":"string"}
            """);

    Schema expanded = evolution.merge(evolution.merge(null, v1), v2);
    Schema afterOldVersion = evolution.merge(expanded, v1);
    GenericRecord oldRecord = new GenericData.Record(v1);
    oldRecord.put("value", "old");

    assertThat(afterOldVersion).isSameAs(expanded);
    assertThat(evolution.valueFor(oldRecord, expanded.getField("extra"))).isNull();
  }

  @Test
  void givenRemovedField_whenSchemasAreMerged_thenRemovedFieldBecomesNullable() {
    Schema v1 = record("{\"name\":\"value\",\"type\":\"string\"}");
    Schema v2 = record("");

    Schema merged = evolution.merge(evolution.merge(null, v1), v2);
    GenericRecord v2Record = new GenericData.Record(v2);

    assertThat(merged.getField("value").schema().isNullable()).isTrue();
    assertThat(evolution.valueFor(v2Record, merged.getField("value"))).isNull();
  }

  @Test
  void givenNumericPromotion_whenOldVersionReturns_thenValuesUseWiderType() {
    Schema v1 = record("{\"name\":\"count\",\"type\":\"int\"}");
    Schema v2 = record("{\"name\":\"count\",\"type\":\"long\"}");
    Schema promoted = evolution.merge(evolution.merge(null, v1), v2);
    Schema afterOldVersion = evolution.merge(promoted, v1);
    GenericRecord oldRecord = new GenericData.Record(v1);
    oldRecord.put("count", 7);

    assertThat(afterOldVersion).isSameAs(promoted);
    assertThat(promoted.getField("count").schema().getType()).isEqualTo(Schema.Type.LONG);
    assertThat(evolution.valueFor(oldRecord, promoted.getField("count"))).isEqualTo(7L);
  }

  @Test
  void givenIncompatibleFieldReuse_whenSchemasAreMerged_thenChangeIsRejected() {
    Schema v1 = record("{\"name\":\"value\",\"type\":\"string\"}");
    Schema incompatible = record("{\"name\":\"value\",\"type\":\"boolean\"}");

    assertThatThrownBy(() -> evolution.merge(v1, incompatible))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Incompatible schema change for field value");
  }

  @Test
  void givenRequiredFieldWithNullValue_whenValueIsRead_thenChangeIsRejected() {
    Schema schema = record("{\"name\":\"value\",\"type\":\"string\"}");
    GenericRecord sourceRecord = new GenericData.Record(schema);

    assertThatThrownBy(() -> evolution.valueFor(sourceRecord, schema.getField("value")))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Required field value must not be null");
  }

  @Test
  void givenNullableFieldWithNullValue_whenValueIsRead_thenNullIsPreserved() {
    Schema schema = record("{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}");
    GenericRecord sourceRecord = new GenericData.Record(schema);

    assertThat(evolution.valueFor(sourceRecord, schema.getField("value"))).isNull();
  }

  @Test
  void givenMissingRequiredStorageField_whenValueIsRead_thenChangeIsRejected() {
    Schema sourceSchema = record("");
    Schema storageSchema = record("{\"name\":\"required\",\"type\":\"long\"}");
    GenericRecord sourceRecord = new GenericData.Record(sourceSchema);

    assertThatThrownBy(() -> evolution.valueFor(sourceRecord, storageSchema.getField("required")))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Record is missing required field required");
  }

  @Test
  void givenNumericValues_whenStorageUsesWiderTypes_thenValuesAreConverted() {
    Schema sourceSchema = record("{\"name\":\"count\",\"type\":\"int\"}");
    GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
    sourceRecord.put("count", 7);
    Schema floatSchema = record("{\"name\":\"count\",\"type\":\"float\"}");
    Schema doubleSchema = record("{\"name\":\"count\",\"type\":\"double\"}");

    assertThat(evolution.valueFor(sourceRecord, floatSchema.getField("count"))).isEqualTo(7.0F);
    assertThat(evolution.valueFor(sourceRecord, doubleSchema.getField("count"))).isEqualTo(7.0D);
  }

  @Test
  void givenAmbiguousNumericUnion_whenValueIsRead_thenChangeIsRejected() {
    Schema sourceSchema = record("{\"name\":\"count\",\"type\":\"int\"}");
    GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
    sourceRecord.put("count", 7);
    Schema storageSchema = record("{\"name\":\"count\",\"type\":[\"long\",\"double\"]}");

    assertThatThrownBy(() -> evolution.valueFor(sourceRecord, storageSchema.getField("count")))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Field count has an unsupported numeric union");
  }

  @Test
  void givenNumericValueAndNonNumericTarget_whenValueIsRead_thenChangeIsRejected() {
    Schema sourceSchema = record("{\"name\":\"count\",\"type\":\"int\"}");
    GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
    sourceRecord.put("count", 7);
    Schema storageSchema = record("{\"name\":\"count\",\"type\":\"string\"}");

    assertThatThrownBy(() -> evolution.valueFor(sourceRecord, storageSchema.getField("count")))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Field count has an incompatible numeric type");
  }

  @Test
  void givenNonNumericIncompatibleValue_whenValueIsRead_thenChangeIsRejected() {
    Schema sourceSchema = record("{\"name\":\"value\",\"type\":\"string\"}");
    GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
    sourceRecord.put("value", "not-a-boolean");
    Schema storageSchema = record("{\"name\":\"value\",\"type\":\"boolean\"}");

    assertThatThrownBy(() -> evolution.valueFor(sourceRecord, storageSchema.getField("value")))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Field value cannot be written using the storage schema");
  }

  @Test
  void givenNonRecordSchema_whenSchemasAreMerged_thenChangeIsRejected() {
    Schema scalar = Schema.create(Schema.Type.STRING);

    assertThatThrownBy(() -> evolution.merge(null, scalar))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Top-level Avro schema must be a record");
  }

  @Test
  void givenReservedProcessTime_whenSchemasAreMerged_thenChangeIsRejected() {
    Schema schema = record("{\"name\":\"PROCESS_TIME\",\"type\":\"long\"}");

    assertThatThrownBy(() -> evolution.merge(null, schema))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Producer schema must not contain reserved field PROCESS_TIME");
  }

  @Test
  void givenRenamedRecord_whenSchemasAreMerged_thenChangeIsRejected() {
    Schema current = record("{\"name\":\"value\",\"type\":\"string\"}");
    Schema renamed =
        new Schema.Parser()
            .parse(
                """
                {"type":"record","name":"RenamedRecord","fields":[]}
                """);

    assertThatThrownBy(() -> evolution.merge(current, renamed))
        .isInstanceOf(UnsupportedSchemaException.class)
        .hasMessage("Avro record name changed from TestRecord to RenamedRecord");
  }

  private Schema record(String fields) {
    return new Schema.Parser()
        .parse(
            """
            {
              "type": "record",
              "name": "TestRecord",
              "fields": [%s]
            }
            """
                .formatted(fields));
  }
}
