package com.nimbo.realtime.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Component
public class SchemaMapping {

    private static final List<String> KAFKA_COLUMNS = List.of(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp"
    );

    private final List<Schema.Field> fields;
    private final String starRocksColumns;

    public SchemaMapping(Schema schema) {
        this.fields = List.copyOf(schema.getFields());

        List<String> columns = new ArrayList<>(KAFKA_COLUMNS.size() + fields.size());
        columns.addAll(KAFKA_COLUMNS);

        fields.stream()
                .map(Schema.Field::name)
                .map(name -> name.toLowerCase(Locale.ROOT))
                .forEach(columns::add);

        this.starRocksColumns = String.join(",", columns);
    }

    public String starRocksColumns() {
        return starRocksColumns;
    }

    public List<Object> extractValues(GenericRecord genericRecord) {
        List<Object> values = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            Object value = genericRecord.get(field.name());
            values.add(normalizeValue(value, field.schema()));
        }
        return values;
    }

    private Object normalizeValue(Object value, Schema fieldSchema) {
        if (value == null) return null;

        return switch (fieldSchema.getType()) {
            case STRING -> value.toString();
            case INT -> ((Number) value).intValue();
            case LONG -> ((Number) value).longValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case FLOAT -> ((Number) value).floatValue();
            default -> value;
        };
    }
}