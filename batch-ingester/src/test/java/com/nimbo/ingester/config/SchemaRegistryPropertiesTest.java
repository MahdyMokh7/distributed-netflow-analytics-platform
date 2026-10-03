package com.nimbo.ingester.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SchemaRegistryPropertiesTest {

    private static Validator validator;

    @BeforeAll
    static void createValidator() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    void givenCompleteRegistryProperties_whenValidated_thenConfigurationIsAccepted() {
        SchemaRegistryProperties properties = new SchemaRegistryProperties();
        properties.setUrl("http://schema-registry:8081");
        properties.setSubject("netflow-value");

        assertThat(validator.validate(properties)).isEmpty();
    }

    @Test
    void givenBlankRegistryUrl_whenValidated_thenRequiredValueIsRejected() {
        SchemaRegistryProperties properties = new SchemaRegistryProperties();
        properties.setUrl(" ");
        properties.setSubject("netflow-value");

        assertThat(validator.validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("url");
    }

    @Test
    void givenBlankRegistrySubject_whenValidated_thenRequiredValueIsRejected() {
        SchemaRegistryProperties properties = new SchemaRegistryProperties();
        properties.setUrl("http://schema-registry:8081");
        properties.setSubject(" ");

        assertThat(validator.validate(properties))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("subject");
    }
}
