package com.nimbo.ingester.config;

import java.util.List;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IngesterConfigurationTest {

    private IngesterConfiguration configuration;

    @BeforeEach
    void setUp() {
        configuration = new IngesterConfiguration();
    }

    @Test
    void givenProductionSchemaResource_whenLoaded_thenExpectedNetFlowContractIsPresent() {
        // Given: the schema resource packaged with the ingester
        String expectedFullName = "org.nimbo.netflow.avro.NetFlowV2Record";

        // When: application configuration loads the schema
        Schema schema = configuration.avroSchema();

        // Then: the production identity and representative fields are present
        assertThat(schema.getFullName()).isEqualTo(expectedFullName);
        assertThat(schema.getField("IPV4_SRC_ADDR").schema().getType()).isEqualTo(Schema.Type.STRING);
        assertThat(schema.getField("IN_BYTES").schema().getType()).isEqualTo(Schema.Type.LONG);
    }

    @Test
    void givenSourceSchema_whenStorageSchemaIsBuilt_thenProcessingTimeIsAppendedWithoutMutation() {
        // Given: the immutable production source-field contract
        Schema source = configuration.avroSchema();
        List<String> originalNames = source.getFields().stream().map(Schema.Field::name).toList();

        // When: storage enrichment creates its Parquet schema
        Schema storage = configuration.storageSchema(source);

        // Then: source fields remain unchanged and nullable epoch micros are appended once
        assertThat(source.getField("PROCESSING_TIME")).isNull();
        assertThat(storage.getFields()).hasSize(source.getFields().size() + 1);
        assertThat(storage.getFields().subList(0, originalNames.size()))
                .extracting(Schema.Field::name)
                .containsExactlyElementsOf(originalNames);
        assertThat(storage.getFields().get(storage.getFields().size() - 1).name()).isEqualTo("PROCESSING_TIME");
        assertThat(storage.getField("PROCESSING_TIME").schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
    }

    @Test
    void givenLoadedSchema_whenDeserializerBeanIsCreated_thenItUsesThatExactSchema() {
        // Given: one production schema instance
        Schema schema = configuration.avroSchema();

        // When: configuration creates the application deserializer
        var deserializer = configuration.avroDeserializer(schema);

        // Then: deserialization is bound to the configured schema instance
        assertThat(deserializer.getSchema()).isSameAs(schema);
    }
}
