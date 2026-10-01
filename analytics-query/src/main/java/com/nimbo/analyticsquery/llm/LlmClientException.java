package com.nimbo.analyticsquery.llm;

public class LlmClientException extends RuntimeException {

    private final Kind kind;

    public LlmClientException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public LlmClientException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public enum Kind {
        UNAVAILABLE,
        INVALID_RESPONSE
    }
}