package com.nimbo.starrockswriter.ingestion;

public class DlqPublishException extends RuntimeException {

    public DlqPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
