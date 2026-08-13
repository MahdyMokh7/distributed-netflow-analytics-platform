package com.nimbo.ingester.deserializer;

import com.nimbo.ingester.exception.DeserializationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AvroDeserializer Tests")
class AvroDeserializerTest {

    private static final String SCHEMA_JSON = """
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "long"}
                ]
            }
            """;

    private Schema schema;
    private AvroDeserializer deserializer;

    @BeforeEach
    void setUp() {
        schema = new Schema.Parser().parse(SCHEMA_JSON);
        deserializer = new AvroDeserializer(schema);
    }

    @Test
    @DisplayName("Should deserialize valid Avro data successfully")
    void shouldDeserializeValidAvroData() throws IOException {
        GenericRecord expected = new GenericData.Record(schema);
        expected.put("id", "test-123");
        expected.put("value", 42L);

        byte[] data = serializeRecord(expected);

        GenericRecord actual = deserializer.deserialize(data);

        assertThat(actual).isNotNull();
        assertThat(actual.get("id").toString()).isEqualTo("test-123");
        assertThat(actual.get("value")).isEqualTo(42L);
    }

    @Test
    @DisplayName("Should throw DeserializationException when data is null")
    void shouldThrowExceptionWhenDataIsNull() {
        assertThatThrownBy(() -> deserializer.deserialize(null))
                .isInstanceOf(DeserializationException.class);
    }

    @Test
    @DisplayName("Should return correct schema")
    void shouldReturnCorrectSchema() {
        assertThat(deserializer.getSchema()).isSameAs(schema);
        assertThat(deserializer.getSchema().getFullName()).isEqualTo("TestRecord");
    }

    private byte[] serializeRecord(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}