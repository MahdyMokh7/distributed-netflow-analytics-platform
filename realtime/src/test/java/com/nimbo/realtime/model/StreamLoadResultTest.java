package com.nimbo.realtime.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamLoadResultTest {

    @Test
    void successStatusIsCaseInsensitive() {
        assertTrue(result("Success").isSuccessful());
        assertTrue(result("success").isSuccessful());
        assertTrue(result("SUCCESS").isSuccessful());
    }

    @Test
    void nullOrNonSuccessStatusIsNotSuccessful() {
        assertFalse(result(null).isSuccessful());
        assertFalse(result("Fail").isSuccessful());
        assertFalse(result("Publish Timeout").isSuccessful());
    }

    private StreamLoadResult result(String status) {
        return new StreamLoadResult(
                null, null, status, null,
                null, null, null, null,
                null, null, null, null,
                null, null
        );
    }
}
