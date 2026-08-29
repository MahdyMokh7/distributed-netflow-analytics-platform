package com.nimbo.realtime.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroDeserializerTest {

    private final Schema schema = new Schema.Parser().parse("""
            {
              "type":"record",
              "name":"Payload",
              "fields":[
                {"name":"name","type":"string"},
                {"name":"count","type":"long"}
              ]
            }
            """);

    @Test
    void deserializesValidBinaryAvroPayload() throws Exception {
        GenericRecord input = new GenericData.Record(schema);
        input.put("name", "flow-1");
        input.put("count", 42L);

        byte[] bytes = serialize(input);
        GenericRecord output = new AvroDeserializer(schema).deserialize(bytes);

        assertEquals("flow-1", output.get("name").toString());
        assertEquals(42L, output.get("count"));
    }

    @Test
    void rejectsNullAndEmptyPayloads() {
        AvroDeserializer deserializer = new AvroDeserializer(schema);

        assertThrows(AvroDeserializationException.class, () -> deserializer.deserialize(null));
        assertThrows(AvroDeserializationException.class, () -> deserializer.deserialize(new byte[0]));
    }

    @Test
    void wrapsMalformedPayloadFailure() {
        AvroDeserializer deserializer = new AvroDeserializer(schema);

        assertThrows(
                AvroDeserializationException.class,
                () -> deserializer.deserialize(new byte[]{1})
        );
    }

    private byte[] serialize(GenericRecord record) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        writer.write(record, encoder);
        encoder.flush();
        return output.toByteArray();
    }
}
