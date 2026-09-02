package com.nimbo.realtime.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StreamLoadResultTest {

    @Test
    void shouldReturnTrueForSuccessStatus() {
        StreamLoadResult result = new StreamLoadResult(
                1L, "label", "Success", null, 10L, 10L, 0L, 100L,
                null, null, null, null, null, null
        );

        assertThat(result.isSuccessful()).isTrue();
    }

    @Test
    void shouldReturnFalseForFailedStatus() {
        StreamLoadResult result = new StreamLoadResult(
                1L, "label", "Fail", "error", 10L, 0L, 10L, 100L,
                null, null, null, null, null, null
        );

        assertThat(result.isSuccessful()).isFalse();
    }
}
