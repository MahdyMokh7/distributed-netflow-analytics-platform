package com.nimbo.analyticsquery.sql;

public class    SqlValidationException extends RuntimeException {

    private final SqlPolicyViolation violation;

    public SqlValidationException(SqlPolicyViolation violation, String message) {
        super(message);
        this.violation = violation;
    }

    public SqlPolicyViolation violation() {
        return violation;
    }
}
