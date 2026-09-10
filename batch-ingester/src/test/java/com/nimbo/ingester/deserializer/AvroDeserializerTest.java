package com.nimbo.ingester.deserializer;

import com.nimbo.ingester.exception.DeserializationException;
import com.nimbo.support.NetFlowTestData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AvroDeserializerTest {

    private NetFlowTestData testData;
    private AvroDeserializer deserializer;

    @BeforeEach
    void setUp() {
        testData = new NetFlowTestData();
        deserializer = new AvroDeserializer(testData.sourceSchema());
    }

    @Test
    void givenCompleteBinaryNetFlow_whenDeserialized_thenEveryRepresentativeValueSurvives() {
        // Given: a complete record encoded with the production Avro schema
        GenericRecord expected = testData.validRecord("172.16.1.10", "172.16.1.20", 24_000L);
        byte[] encoded = testData.serialize(expected);

        // When: production deserialization reads the binary payload
        GenericRecord actual = deserializer.deserialize(encoded);

        // Then: identifying and numeric values match the encoded record
        assertThat(actual.get("IPV4_SRC_ADDR").toString()).isEqualTo("172.16.1.10");
        assertThat(actual.get("IPV4_DST_ADDR").toString()).isEqualTo("172.16.1.20");
        assertThat(actual.get("IN_BYTES")).isEqualTo(24_000L);
        assertThat(actual.get("OUT_BYTES")).isEqualTo(12_000L);
        assertThat(actual.get("L4_SRC_PORT")).isEqualTo(443);
    }

    @Test
    void givenMalformedBinaryPayload_whenDeserialized_thenDomainFailurePreservesCause() {
        // Given: deterministic bytes that cannot represent the production record
        byte[] malformed = {1, 2, 3, 4};

        // When: production deserialization attempts to decode the payload
        var invocation = org.assertj.core.api.Assertions.catchThrowable(() -> deserializer.deserialize(malformed));

        // Then: callers receive the domain exception with its decoding cause
        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize Avro record")
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void givenNullPayload_whenDeserialized_thenExplicitDomainFailureIsReturned() {
        // Given: a null Kafka record payload
        byte[] payload = null;

        // When: production deserialization receives the null payload
        var invocation = org.assertj.core.api.Assertions.catchThrowable(() -> deserializer.deserialize(payload));

        // Then: callers receive an intentional null-payload domain failure
        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Avro payload must not be null")
                .hasNoCause();
    }
}
