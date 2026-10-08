package com.nimbo.storage.schema;

import com.nimbo.common.avro.AvroSchemaSupport;
import com.nimbo.common.netflow.NetflowMetadata;
import com.nimbo.storage.exception.UnsupportedSchemaException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class StorageSchemaEvolution {

  public Schema merge(Schema current, Schema incoming) {
    requireRecord(incoming);
    requireNoReservedFields(incoming);

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

  public Object valueFor(GenericRecord sourceRecord, Schema.Field storageField) {
    Schema.Field sourceField = sourceRecord.getSchema().getField(storageField.name());
    if (sourceField == null) {
      if (AvroSchemaSupport.allowsNull(storageField.schema())) {
        return null;
      }
      throw new UnsupportedSchemaException("Record is missing required field " + storageField.name());
    }

    Object value = sourceRecord.get(sourceField.pos());
    if (value == null) {
      if (AvroSchemaSupport.allowsNull(storageField.schema())) {
        return null;
      }
      throw new UnsupportedSchemaException(
          "Required field " + storageField.name() + " must not be null");
    }
    if (GenericData.get().validate(storageField.schema(), value)) {
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

    boolean incomingReadsCurrent = AvroSchemaSupport.isReaderWriterCompatible(incoming.schema(), current.schema());
    boolean currentReadsIncoming = AvroSchemaSupport.isReaderWriterCompatible(current.schema(), incoming.schema());

    if (incomingReadsCurrent && !currentReadsIncoming) {
      return copy(incoming, incoming.schema());
    }
    if (currentReadsIncoming) {
      return copy(current, current.schema());
    }

    throw new UnsupportedSchemaException(
        "Incompatible schema change for field " + current.name());
  }

  private Schema.Field nullableCopy(Schema.Field field) {
    return copy(field, AvroSchemaSupport.nullable(field.schema()), JsonProperties.NULL_VALUE);
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
    final Schema numericSchema;
    try {
      numericSchema = AvroSchemaSupport.unwrapNullable(target);
    } catch (IllegalArgumentException exception) {
      throw new UnsupportedSchemaException(
          "Field " + fieldName + " has an unsupported numeric union");
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

  private void requireNoReservedFields(Schema schema) {
    for (String reservedField : NetflowMetadata.RESERVED_FIELDS) {
      if (schema.getField(reservedField) != null) {
        throw new UnsupportedSchemaException(
            "Producer schema must not contain reserved field " + reservedField);
      }
    }
  }

  private void requireSameRecord(Schema current, Schema incoming) {
    if (!current.getFullName().equals(incoming.getFullName())) {
      throw new UnsupportedSchemaException(
          "Avro record name changed from " + current.getFullName() + " to " + incoming.getFullName());
    }
  }
}
