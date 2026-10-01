package com.nimbo.ingester.deserializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.nimbo.ingester.exception.DeserializationException;
import com.nimbo.ingester.exception.SchemaRegistryException;
import com.nimbo.support.KafkaContainerExtension;
import com.nimbo.support.NetFlowTestData;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

class AvroDeserializerTest {

    @RegisterExtension static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

    private static final String TOPIC = "netflow";

    private NetFlowTestData testData;
    private KafkaAvroSerializer serializer;
    private AvroDeserializer deserializer;

    @BeforeEach
    void setUp() {
        testData = new NetFlowTestData();
        Map<String, Object> registryConfig = registryConfig(KAFKA.schemaRegistryUrl());
        serializer = new KafkaAvroSerializer();
        serializer.configure(registryConfig, false);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(registryConfig, false);
        deserializer = new AvroDeserializer(kafkaAvroDeserializer);
    }

    @AfterEach
    void tearDown() {
        deserializer.close();
        serializer.close();
    }

    @Test
    void givenCompleteRegistryMessage_whenDeserialized_thenEveryRepresentativeValueSurvives() {
        GenericRecord expected = testData.validRecord("172.16.1.10", "172.16.1.20", 24_000L);
        byte[] encoded = serializer.serialize(TOPIC, expected);

        GenericRecord actual = deserializer.deserialize(TOPIC, encoded);

        assertThat(actual.get("IPV4_SRC_ADDR").toString()).isEqualTo("172.16.1.10");
        assertThat(actual.get("IPV4_DST_ADDR").toString()).isEqualTo("172.16.1.20");
        assertThat(actual.get("IN_BYTES")).isEqualTo(24_000L);
        assertThat(actual.get("OUT_BYTES")).isEqualTo(12_000L);
        assertThat(actual.get("L4_SRC_PORT")).isEqualTo(443);
    }

    @Test
    void givenEvolvedWriterSchema_whenDeserialized_thenNewFieldIsPreserved() {
        Schema newSchema =
                new Schema.Parser()
                        .parse(
                                """
                {"type":"record","name":"EvolvingRecord","fields":[
                  {"name":"id","type":"long"},
                  {"name":"label","type":["null","string"],"default":null}
                ]}
                """);
        GenericRecord newRecord = new GenericData.Record(newSchema);
        newRecord.put("id", 42L);
        newRecord.put("label", "preserved");
        byte[] encoded = serializer.serialize("evolving-netflow", newRecord);

        GenericRecord actual = deserializer.deserialize("evolving-netflow", encoded);

        assertThat(actual.get("id")).isEqualTo(42L);
        assertThat(actual.get("label").toString()).isEqualTo("preserved");
        assertThat(actual.getSchema()).isEqualTo(newSchema);
    }

    @Test
    void givenRegistryEncodedPrimitive_whenDeserialized_thenNonRecordIsRejected() {
        byte[] encoded = serializer.serialize("primitive-netflow", "not-a-record");

        Throwable invocation = catchThrowable(() -> deserializer.deserialize("primitive-netflow", encoded));

        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Decoded Avro value is not a record");
    }

    @Test
    void givenMalformedMessage_whenDeserialized_thenDomainFailurePreservesCause() {
        byte[] malformed = {1, 2, 3, 4};

        Throwable invocation = catchThrowable(() -> deserializer.deserialize(TOPIC, malformed));

        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize Avro record")
                .hasCauseInstanceOf(SerializationException.class);
    }

    @Test
    void givenNullPayload_whenDeserialized_thenExplicitDomainFailureIsReturned() {
        Throwable invocation = catchThrowable(() -> deserializer.deserialize(TOPIC, null));

        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Avro payload must not be null")
                .hasNoCause();
    }

    @Test
    void givenRegistryFailure_whenDeserialized_thenFailureRemainsRetryable() {
        KafkaAvroDeserializer unavailableRegistryDeserializer = new KafkaAvroDeserializer();
        unavailableRegistryDeserializer.configure(unavailableRegistryConfig(), false);
        AvroDeserializer failingDeserializer = new AvroDeserializer(unavailableRegistryDeserializer);

        Throwable invocation;
        try (failingDeserializer) {
            invocation = catchThrowable(() -> failingDeserializer.deserialize(TOPIC, registryPayload(1)));
        }

        assertThat(invocation)
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessage("Schema Registry lookup failed");
    }

    @Test
    void givenUnknownSchemaId_whenDeserialized_thenMessageCanGoToDlq() {
        Throwable invocation = catchThrowable(
                () -> deserializer.deserialize(TOPIC, registryPayload(Integer.MAX_VALUE)));

        assertThat(invocation)
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize Avro record");
    }

    private Map<String, Object> registryConfig(String url) {
        return Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    }

    private Map<String, Object> unavailableRegistryConfig() {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://127.0.0.1:1",
                "http.connect.timeout.ms", 100,
                "http.read.timeout.ms", 100,
                "max.retries", 0);
    }

    private byte[] registryPayload(int schemaId) {
        return new byte[] {
            0,
            (byte) (schemaId >>> 24),
            (byte) (schemaId >>> 16),
            (byte) (schemaId >>> 8),
            (byte) schemaId
        };
    }
}
