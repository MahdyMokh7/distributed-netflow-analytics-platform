package com.nimbo.starrockswriter.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AvroDeserializer {

    private final Schema schema;

    public AvroDeserializer(Schema netflowSchema) {
        this.schema = netflowSchema;
    }

    public GenericRecord deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            throw new AvroDeserializationException(
                    "Cannot deserialize empty Avro payload",
                    new IllegalArgumentException("Avro payload is null or empty")
            );
        }

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

        try {
            return reader.read(null, decoder);
        } catch (IOException | AvroRuntimeException e) {
            throw new AvroDeserializationException("Failed to deserialize NetFlow Avro record", e);
        }
    }
}
