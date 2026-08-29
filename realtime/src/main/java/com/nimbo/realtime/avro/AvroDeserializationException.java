package com.nimbo.realtime.avro;

public class AvroDeserializationException extends RuntimeException {

    public AvroDeserializationException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
