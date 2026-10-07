package com.nimbo.common.avro;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;

public final class AvroSchemaSupport {

  private AvroSchemaSupport() {
  }

  public static boolean hasField(GenericRecord record, String fieldName) {
    return record != null && record.getSchema().getField(fieldName) != null;
  }

  public static boolean allowsNull(Schema schema) {
    return schema.getType() == Schema.Type.NULL
        || schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream()
        .anyMatch(type -> type.getType() == Schema.Type.NULL);
  }

  public static Schema nullable(Schema schema) {
    if (allowsNull(schema)) {
      return schema;
    }
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  public static Schema unwrapNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> nonNullTypes = schema.getTypes().stream()
        .filter(type -> type.getType() != Schema.Type.NULL).toList();

    if (nonNullTypes.size() != 1) {
      throw new IllegalArgumentException(
          "Only nullable unions with exactly one non-null Avro type are supported: " + schema);
    }
    return nonNullTypes.get(0);
  }

  public static boolean isReaderWriterCompatible(Schema reader, Schema writer) {
    return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType()
        == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
  }
}
