package com.nimbo.starrockswriter.validation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationResultTest {

    @Test
    void shouldCreateValidResult() {
        ValidationResult result = new ValidationResult(true, null);

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isNull();
    }

    @Test
    void shouldCreateInvalidResultWithReason() {
        ValidationResult result = new ValidationResult(false, "Invalid IPv4 address");

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("Invalid IPv4 address");
    }

    @Test
    void shouldSupportEquality() {
        ValidationResult first = new ValidationResult(false, "error");
        ValidationResult second = new ValidationResult(false, "error");

        assertThat(first).isEqualTo(second);
    }
}
