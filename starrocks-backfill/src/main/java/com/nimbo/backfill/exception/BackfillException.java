package com.nimbo.backfill.exception;

public class BackfillException extends RuntimeException {

    public BackfillException(String message) {
        super(message);
    }

    public BackfillException(String message, Throwable cause) {
        super(message, cause);
    }
}
