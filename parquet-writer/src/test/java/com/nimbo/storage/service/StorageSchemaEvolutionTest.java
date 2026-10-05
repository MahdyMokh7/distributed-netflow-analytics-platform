package com.nimbo.storage.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.nimbo.storage.exception.UnsupportedSchemaException;

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
