package com.nimbo.starrockswriter.mapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
public class SchemaMapping {

  public static final List<String> KAFKA_COLUMNS =
      List.of("kafka_topic", "kafka_partition", "kafka_offset", "PROCESS_TIME");

  public String starRocksColumns(List<String> netFlowColumns) {
    return String.join(",", concatColumns(netFlowColumns));
  }

  public List<String> concatColumns(List<String> netFlowColumns) {
    return java.util.stream.Stream.concat(KAFKA_COLUMNS.stream(), netFlowColumns.stream()).toList();
  }

  public Map<String, Object> extractValues(GenericRecord genericRecord) {
    Map<String, Object> values = new LinkedHashMap<>();
    for (Schema.Field field : genericRecord.getSchema().getFields()) {
      String column = field.name().toLowerCase(Locale.ROOT);
      Object value = genericRecord.get(field.name());
      values.put(column, normalizeValue(value, field.schema()));
    }
    return values;
  }

  public String starRocksType(Schema schema) {
    Schema effective = unwrapNullable(schema);
    return switch (effective.getType()) {
      case STRING, ENUM -> "VARCHAR(65533)";
      case INT -> "INT";
      case LONG -> "BIGINT";
      case FLOAT -> "FLOAT";
      case DOUBLE -> "DOUBLE";
      case BOOLEAN -> "BOOLEAN";
      default ->
          throw new IllegalArgumentException(
              "Unsupported Avro field type for automatic StarRocks schema evolution: "
                  + effective.getType());
    };
  }

  public TypeEvolution typeEvolution(String existingColumnType, Schema avroSchema) {
    String existing = normalizeStarRocksType(existingColumnType);
    String desired = normalizeStarRocksType(starRocksType(avroSchema));

    if (existing.equals(desired)) {
      return TypeEvolution.COMPATIBLE;
    }

    if (existing.startsWith("VARCHAR(") && desired.startsWith("VARCHAR(")) {
      return TypeEvolution.COMPATIBLE;
    }

    if ((existing.equals("BIGINT") && desired.equals("INT"))
        || (existing.equals("DOUBLE") && desired.equals("FLOAT"))) {
      return TypeEvolution.COMPATIBLE;
    }

    if ((existing.equals("INT") && desired.equals("BIGINT"))
        || (existing.equals("FLOAT") && desired.equals("DOUBLE"))) {
      return TypeEvolution.WIDEN;
    }

    return TypeEvolution.INCOMPATIBLE;
  }

  public String normalizeStarRocksType(String columnType) {
    if (columnType == null) {
      return "";
    }

    String normalized = columnType.trim().toUpperCase(Locale.ROOT).replace(" ", "");

    // StarRocks exposes MySQL-compatible integer display widths through
    // information_schema (for example INT(11) and BIGINT(20)). The width
    // is display metadata, not a different storage type, so schema
    // evolution must compare the canonical integer type instead.
    if (normalized.matches("(?:INT|INTEGER)\\(\\d+\\)")) {
      return "INT";
    }
    if (normalized.matches("BIGINT\\(\\d+\\)")) {
      return "BIGINT";
    }
    if (normalized.equals("INTEGER")) {
      return "INT";
    }

    return normalized;
  }

  public String fieldComment(Schema.Field field) {
    return field.doc() == null ? "" : field.doc();
  }

  private Object normalizeValue(Object value, Schema fieldSchema) {
    if (value == null) {
      return null;
    }

    Schema effective = unwrapNullable(fieldSchema);
    return switch (effective.getType()) {
      case STRING, ENUM -> value.toString();
      case INT -> ((Number) value).intValue();
      case LONG -> ((Number) value).longValue();
      case DOUBLE -> ((Number) value).doubleValue();
      case FLOAT -> ((Number) value).floatValue();
      case BOOLEAN -> (Boolean) value;
      default ->
          throw new IllegalArgumentException(
              "Unsupported Avro field type for StarRocks mapping: " + effective.getType());
    };
  }

  private Schema unwrapNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> nonNullTypes =
        schema.getTypes().stream().filter(type -> type.getType() != Schema.Type.NULL).toList();

    if (nonNullTypes.size() != 1) {
      throw new IllegalArgumentException(
          "Only nullable unions with exactly one non-null Avro type are supported: " + schema);
    }
    return nonNullTypes.get(0);
  }

  public enum TypeEvolution {
    COMPATIBLE,
    WIDEN,
    INCOMPATIBLE
  }
}
