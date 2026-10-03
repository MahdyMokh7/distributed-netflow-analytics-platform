package com.nimbo.backfill.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.BackfillProjection.HdfsPathProjection;
import com.nimbo.backfill.model.HdfsSchema;
import com.nimbo.backfill.model.SchemaColumn;

public final class BackfillSchemaMapper {

    private static final String PROCESS_TIME = "process_time";
    private static final String KAFKA_TIMESTAMP = "kafka_timestamp";
    private static final Set<String> NUMBER_TYPES = Set.of("tinyint", "smallint", "int", "integer", "bigint",
            "largeint", "float", "double", "decimal", "decimal32", "decimal64", "decimal128", "decimal256");

    public BackfillProjection map(String qualifiedTable, List<SchemaColumn> tableColumns, List<HdfsSchema> hdfsSchemas,
            List<String> skippedPaths) {

        if (hdfsSchemas.isEmpty()) {
            throw new BackfillException("No Parquet files exist in the requested hourly paths");
        }

        Map<String, SchemaColumn> target = index(tableColumns, "StarRocks");
        List<HdfsPathProjection> pathProjections = new ArrayList<>();

        for (HdfsSchema hdfsSchema : hdfsSchemas) {
            pathProjections.add(mapPath(qualifiedTable, tableColumns, target, hdfsSchema));
        }

        return new BackfillProjection(tableColumns.stream().map(SchemaColumn::name).toList(), pathProjections,
                skippedPaths);
    }

    private HdfsPathProjection mapPath(String qualifiedTable, List<SchemaColumn> tableColumns,
            Map<String, SchemaColumn> target, HdfsSchema hdfsSchema) {

        Map<String, SchemaColumn> source = index(hdfsSchema.columns(), hdfsSchema.path());
        rejectSourceOnlyColumns(qualifiedTable, target, source, hdfsSchema.path());

        List<String> expressions = new ArrayList<>();
        List<String> generated = new ArrayList<>();
        int matched = 0;
        int nullFilled = 0;

        for (SchemaColumn targetColumn : tableColumns) {

            String normalizedName = normalize(targetColumn.name());
            SchemaColumn sourceColumn = source.get(normalizedName);

            if (KAFKA_TIMESTAMP.equals(normalizedName)) {
                SchemaColumn processTime = requireProcessTime(source, hdfsSchema.path());
                requireCompatible(processTime, targetColumn, hdfsSchema.path());

                expressions.add("CAST(" + identifier(processTime.name()) + " / 1000 AS " + targetColumn.type() + ")");
                generated.add("kafka_timestamp <- PROCESS_TIME/1000");

            } else if (sourceColumn != null) {
                requireCompatible(sourceColumn, targetColumn, hdfsSchema.path());
                expressions.add("CAST(" + identifier(sourceColumn.name()) + " AS " + targetColumn.type() + ")");
                matched++;
            } else if (targetColumn.nullable()) {
                expressions.add("CAST(NULL AS " + targetColumn.type() + ")");
                nullFilled++;
            } else {
                throw missingRequiredColumn(qualifiedTable, targetColumn, hdfsSchema.path());
            }
        }

        String summary = matched + " matched, " + generated.size() + " generated"
                + (nullFilled == 0 ? "" : ", " + nullFilled + " NULL-filled")
                + (generated.isEmpty() ? "" : " (" + String.join(", ", generated) + ")");
        return new HdfsPathProjection(hdfsSchema.path(), expressions, summary);
    }

    private void rejectSourceOnlyColumns(String qualifiedTable, Map<String, SchemaColumn> target,
            Map<String, SchemaColumn> source, String path) {
        List<SchemaColumn> missing =
                source.values().stream().filter(column -> !PROCESS_TIME.equals(normalize(column.name())))
                        .filter(column -> !target.containsKey(normalize(column.name()))).toList();
        if (missing.isEmpty()) {
            return;
        }

        String names = missing.stream().map(SchemaColumn::name).collect(Collectors.joining(", "));
        String alterations = missing.stream()
                .map(column -> "ALTER TABLE " + qualifiedTable + " ADD COLUMN "
                        + identifier(column.name().toLowerCase(Locale.ROOT)) + " " + column.type() + " NULL;")
                .collect(Collectors.joining(" "));
        throw new BackfillException("StarRocks is missing HDFS column(s) " + names + " from " + path
                + ". Add them before backfill, for example: " + alterations);
    }

    private BackfillException missingRequiredColumn(String qualifiedTable, SchemaColumn targetColumn, String path) {
        return new BackfillException("HDFS path " + path + " is missing required StarRocks column "
                + targetColumn.name() + ". Old Parquet files cannot be changed; allow NULL for old versions, "
                + "for example: ALTER TABLE " + qualifiedTable + " MODIFY COLUMN " + identifier(targetColumn.name())
                + " " + targetColumn.type() + " NULL;");
    }

    private SchemaColumn requireProcessTime(Map<String, SchemaColumn> source, String path) {
        SchemaColumn processTime = source.get(PROCESS_TIME);
        if (processTime == null) {
            throw new BackfillException("HDFS path " + path + " has no PROCESS_TIME for kafka_timestamp");
        }
        return processTime;
    }

    private void requireCompatible(SchemaColumn source, SchemaColumn target, String path) {
        if (!typeFamily(source.type()).equals(typeFamily(target.type()))) {
            throw new BackfillException("Incompatible column types in " + path + ": HDFS " + source.name() + " is "
                    + source.type() + ", but StarRocks " + target.name() + " is " + target.type());
        }
    }

    private String typeFamily(String type) {
        String normalizedType = normalize(type).replace(" unsigned", "");
        String base = normalizedType;
        int parameters = base.indexOf('(');
        if (parameters >= 0) {
            base = base.substring(0, parameters);
        }
        if (NUMBER_TYPES.contains(base)) {
            return base.startsWith("decimal") ? normalizedType : base.equals("integer") ? "int" : base;
        }
        if (base.equals("char") || base.equals("varchar") || base.equals("string")) {
            return "string";
        }
        if (base.equals("date") || base.equals("datetime") || base.equals("timestamp")) {
            return "temporal";
        }
        return base;
    }

    private Map<String, SchemaColumn> index(List<SchemaColumn> columns, String owner) {
        Map<String, SchemaColumn> indexed = new LinkedHashMap<>();
        for (SchemaColumn column : columns) {
            SchemaColumn previous = indexed.put(normalize(column.name()), column);
            if (previous != null) {
                throw new BackfillException(owner + " exposes duplicate column " + column.name() + " ignoring case");
            }
        }
        return indexed;
    }

    private String normalize(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private String identifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }
}
