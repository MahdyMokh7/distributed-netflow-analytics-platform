package com.nimbo.analyticsquery.sql.execution;

import java.util.Objects;

public class SqlExecutionException extends RuntimeException {

    private final Kind kind;

    public SqlExecutionException(
            Kind kind,
            String message) {

        super(message);
        this.kind = Objects.requireNonNull(
                kind,
                "kind must not be null");
    }

    public SqlExecutionException(
            Kind kind,
            String message,
            Throwable cause) {

        super(message, cause);
        this.kind = Objects.requireNonNull(
                kind,
                "kind must not be null");
    }

    public Kind kind() {
        return kind;
    }

    public enum Kind {
        TIMEOUT,
        RESULT_TOO_LARGE,
        RESULT_SERIALIZATION_FAILED,
        DATASOURCE_ERROR
    }
}