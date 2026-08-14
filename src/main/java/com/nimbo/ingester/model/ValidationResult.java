package com.nimbo.ingester.model;

public record ValidationResult(
        boolean valid,
        String errorMessage
) {

    public static ValidationResult ok() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult invalid(String errorMessage) {
        return new ValidationResult(false, errorMessage);
    }
}