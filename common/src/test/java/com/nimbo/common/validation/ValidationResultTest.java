package com.nimbo.common.validation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationResultTest {

    @Test
    void shouldCreateSuccessResult() {
        ValidationResult result = ValidationResult.success();

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isNull();
    }

    @Test
    void shouldCreateFailureResult() {
        ValidationResult result = ValidationResult.failure("Invalid IPv4 address");

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("Invalid IPv4 address");
    }

    @Test
    void shouldUseRecordEquality() {
        ValidationResult first = new ValidationResult(false, "error");
        ValidationResult second = new ValidationResult(false, "error");

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }
}
