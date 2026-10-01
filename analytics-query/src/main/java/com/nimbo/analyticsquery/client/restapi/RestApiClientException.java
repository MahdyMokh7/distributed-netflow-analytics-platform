package com.nimbo.analyticsquery.client.restapi;

public class RestApiClientException extends RuntimeException {

    private final Kind kind;
    private final Integer httpStatus;

    public RestApiClientException(
            Kind kind,
            Integer httpStatus,
            String message) {

        super(message);
        this.kind = kind;
        this.httpStatus = httpStatus;
    }

    public RestApiClientException(
            Kind kind,
            Integer httpStatus,
            String message,
            Throwable cause) {

        super(message, cause);
        this.kind = kind;
        this.httpStatus = httpStatus;
    }

    public Kind kind() {
        return kind;
    }

    public Integer httpStatus() {
        return httpStatus;
    }

    public enum Kind {
        NO_DATA,
        BAD_REQUEST,
        UNAVAILABLE,
        INVALID_RESPONSE
    }
}