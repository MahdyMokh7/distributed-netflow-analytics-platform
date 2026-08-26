package com.nimbo.realtime.validation;

import com.nimbo.realtime.mapper.SchemaMapping;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NetflowValidationService {

    private final SchemaMapping schemaMapping;

    public ValidationResult validate(GenericRecord record
    ) {

        if (record == null) {
            return ValidationResult.failure(
                    "Record is null"
            );
        }

        for (Schema.Field field : schemaMapping.fields()) {

            Object value = record.get(field.name());

            if (value == null) {
                return ValidationResult.failure(
                        "Missing required field: " + field.name()
                );
            }

            if (!matchesType(value, field.schema())) {
                return ValidationResult.failure(
                        "Invalid type for field " + field.name()
                                + ". expected=" + field.schema().getType()
                                + ", actual=" + value.getClass().getSimpleName()
                );
            }
        }

        return ValidationResult.success();
    }

    private boolean matchesType(
            Object value,
            Schema schema
    ) {

        return switch (schema.getType()) {

            case STRING ->
                    value instanceof CharSequence;

            case INT ->
                    value instanceof Integer;

            case LONG ->
                    value instanceof Long;

            case FLOAT ->
                    value instanceof Float;

            case DOUBLE ->
                    value instanceof Double;

            default ->
                    true;
        };
    }
}