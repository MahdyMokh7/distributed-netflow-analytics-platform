package com.nimbo.starrockswriter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroDeserializerTest {

    private final Schema schema = new Schema.Parser().parse("""
            {
              "type":"record",
              "name":"TestRecord",
              "fields":[
                {
                  "name":"name",
                  "type":"string"
                },
                {
                  "name":"count",
                  "type":"int"
                }
              ]
            }
            """);

    @Test
    void shouldDeserializeValidAvroPayload() throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "test");
        record.put("count", 10);

        byte[] payload = serialize(record);
        AvroDeserializer deserializer = new AvroDeserializer(schema);

        GenericRecord result = deserializer.deserialize(payload);

        assertThat(result.get("name").toString()).isEqualTo("test");
        assertThat(result.get("count")).isEqualTo(10);
    }

    @Test
    void shouldRejectNullPayload() {
        AvroDeserializer deserializer = new AvroDeserializer(schema);

        assertThatThrownBy(() -> deserializer.deserialize(null))
                .isInstanceOf(Exception.class);
    }

    @Test
    void shouldRejectCorruptedPayload() {
        AvroDeserializer deserializer = new AvroDeserializer(schema);

        assertThatThrownBy(() -> deserializer.deserialize(new byte[]{1, 2, 3, 4}))
                .isInstanceOf(Exception.class);
    }

    private byte[] serialize(GenericRecord record) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        writer.write(record, encoder);
        encoder.flush();

        return output.toByteArray();
    }
}
