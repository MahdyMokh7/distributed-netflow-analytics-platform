package com.nimbo.starrockswriter.avro;

public class AvroDeserializationException extends RuntimeException {

    public AvroDeserializationException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
