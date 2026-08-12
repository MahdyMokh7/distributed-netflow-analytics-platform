package com.nimbo.ingester.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;

public final class AvroUtils {

    private AvroUtils() {
        // Utility class - prevent instantiation
    }

    public static Schema loadSchema(String path) throws IOException {
        try (InputStream schemaStream = new ClassPathResource(path).getInputStream()) {
            if (schemaStream == null) {
                throw new IllegalStateException("Avro schema not found at: " + path);
            }
            return new Schema.Parser().parse(schemaStream);
        }
    }

    public static String extractString(GenericRecord record, String field) {
        Object value = record.get(field);
        return value != null ? value.toString() : null;
    }

    public static Long extractLong(GenericRecord record, String field) {
        Object value = record.get(field);
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        return Long.parseLong(value.toString());
    }
}