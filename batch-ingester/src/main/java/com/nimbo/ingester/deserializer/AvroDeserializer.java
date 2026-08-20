package com.nimbo.ingester.deserializer;

import com.nimbo.ingester.exception.DeserializationException;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

public class AvroDeserializer {

    @Getter
    private final Schema schema;
    private final GenericDatumReader<GenericRecord> reader;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public AvroDeserializer(Schema schema) {
        this.schema = schema;
        this.reader = new GenericDatumReader<>(schema);
    }

    public GenericRecord deserialize(byte[] data) {
        try {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new DeserializationException("Failed to deserialize Avro record", e);
        }
    }

}