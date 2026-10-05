package com.nimbo.analyticsquery.sql.execution;

import java.util.Objects;

public class QueryPlanSafetyException extends RuntimeException {

    private final Reason reason;

    public QueryPlanSafetyException(
            Reason reason,
            String message) {

        super(message);
        this.reason = Objects.requireNonNull(
                reason,
                "reason must not be null");
    }

    public QueryPlanSafetyException(
            Reason reason,
            String message,
            Throwable cause) {

        super(message, cause);
        this.reason = Objects.requireNonNull(
                reason,
                "reason must not be null");
    }

    public Reason reason() {
        return reason;
    }

    public enum Reason {
        EXPLAIN_TIMEOUT,
        EXPLAIN_FAILED,
        PLAN_OUTPUT_TOO_LARGE,
        COST_LIMIT_EXCEEDED,
        ROW_ESTIMATE_LIMIT_EXCEEDED
    }
}
