package com.nimbo.storage.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.nimbo.storage.exception.UnsupportedSchemaException;

final class StorageSchemaEvolution {

    Schema merge(Schema current, Schema incoming) {
        requireRecord(incoming);
        requireAvailableProcessTime(incoming);

        if (current == null) {
            return incoming;
        }

        requireSameRecord(current, incoming);

        List<Schema.Field> mergedFields = new ArrayList<>();
        for (Schema.Field currentField : current.getFields()) {
            Schema.Field incomingField = incoming.getField(currentField.name());
            mergedFields.add(incomingField == null
                            ? nullableCopy(currentField)
                            : mergeField(currentField, incomingField));
        }

        for (Schema.Field incomingField : incoming.getFields()) {
            if (current.getField(incomingField.name()) == null) {
                mergedFields.add(nullableCopy(incomingField));
            }
        }

        Schema merged =
                Schema.createRecord(
                        current.getName(),
                        current.getDoc(),
                        current.getNamespace(),
                        current.isError(),
                        mergedFields);
        return merged.equals(current) ? current : merged;
    }

    Object valueFor(GenericRecord record, Schema.Field storageField) {
        Schema.Field sourceField = record.getSchema().getField(storageField.name());
        if (sourceField == null) {
            if (allowsNull(storageField.schema())) {
                return null;
            }
            throw new UnsupportedSchemaException("Record is missing required field " + storageField.name());
        }

        Object value = record.get(sourceField.pos());
        if (value == null || GenericData.get().validate(storageField.schema(), value)) {
            return value;
        }

        if (value instanceof Number number) {
            return widenNumber(number, storageField.schema(), storageField.name());
        }

        throw new UnsupportedSchemaException(
                "Field " + storageField.name() + " cannot be written using the storage schema");
    }

    private Schema.Field mergeField(Schema.Field current, Schema.Field incoming) {
        if (current.schema().equals(incoming.schema())) {
            return copy(current, current.schema());
        }

        boolean incomingReadsCurrent = isCompatible(incoming.schema(), current.schema());
        boolean currentReadsIncoming = isCompatible(current.schema(), incoming.schema());

        if (incomingReadsCurrent && !currentReadsIncoming) {
            return copy(incoming, incoming.schema());
        }
        if (currentReadsIncoming) {
            return copy(current, current.schema());
        }

        throw new UnsupportedSchemaException(
                "Incompatible schema change for field " + current.name());
    }

    private boolean isCompatible(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType()
                == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
    }

    private Schema.Field nullableCopy(Schema.Field field) {
        return copy(field, nullable(field.schema()), JsonProperties.NULL_VALUE);
    }

    private Schema nullable(Schema schema) {
        if (allowsNull(schema)) {
            return schema;
        }
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    private boolean allowsNull(Schema schema) {
        return schema.getType() == Schema.Type.NULL
                || schema.getType() == Schema.Type.UNION
                        && schema.getTypes().stream()
                                .anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private Schema.Field copy(Schema.Field field, Schema schema) {
        return new Schema.Field(field, schema);
    }

    private Schema.Field copy(Schema.Field field, Schema schema, Object defaultValue) {
        Schema.Field copy = new Schema.Field(field.name(), schema, field.doc(), defaultValue, field.order());
        field.aliases().forEach(copy::addAlias);
        field.getObjectProps().forEach(copy::addProp);
        return copy;
    }

    private Object widenNumber(Number value, Schema target, String fieldName) {
        Schema numericSchema = target;
        if (target.getType() == Schema.Type.UNION) {
            List<Schema> nonNullTypes =
                    target.getTypes().stream()
                            .filter(type -> type.getType() != Schema.Type.NULL)
                            .toList();
            if (nonNullTypes.size() != 1) {
                throw new UnsupportedSchemaException(
                        "Field " + fieldName + " has an unsupported numeric union");
            }
            numericSchema = nonNullTypes.get(0);
        }

        return switch (numericSchema.getType()) {
            case LONG -> value.longValue();
            case FLOAT -> value.floatValue();
            case DOUBLE -> value.doubleValue();
            default -> throw new UnsupportedSchemaException(
                    "Field " + fieldName + " has an incompatible numeric type");
        };
    }

    private void requireRecord(Schema schema) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new UnsupportedSchemaException("Top-level Avro schema must be a record");
        }
    }

    private void requireAvailableProcessTime(Schema schema) {
        if (schema.getField("PROCESS_TIME") != null) {
            throw new UnsupportedSchemaException(
                    "Producer schema must not contain reserved field PROCESS_TIME");
        }
    }

    private void requireSameRecord(Schema current, Schema incoming) {
        if (!current.getFullName().equals(incoming.getFullName())) {
            throw new UnsupportedSchemaException(
                    "Avro record name changed from " + current.getFullName() + " to " + incoming.getFullName());
        }
    }
}
