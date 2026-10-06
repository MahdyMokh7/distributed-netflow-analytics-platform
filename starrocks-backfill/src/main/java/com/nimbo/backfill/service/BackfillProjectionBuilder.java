package com.nimbo.backfill.service;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.HdfsPathColumnMapping;
import com.nimbo.backfill.model.HdfsSchema;
import com.nimbo.backfill.model.SchemaColumn;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class BackfillProjectionBuilder {

  private static final Set<String> NUMBER_TYPES =
      Set.of(
          "tinyint",
          "smallint",
          "int",
          "integer",
          "bigint",
          "largeint",
          "float",
          "double",
          "decimal",
          "decimal32",
          "decimal64",
          "decimal128",
          "decimal256");

  public BackfillProjection build(
      String starRocksTable,
      List<SchemaColumn> starRocksColumns,
      List<HdfsSchema> hdfsSchemas,
      List<String> skippedPaths) {

    if (hdfsSchemas.isEmpty()) {
      throw new BackfillException("No Parquet files exist in the requested hourly paths");
    }

    Map<String, SchemaColumn> starRocksColumnsByName = index(starRocksColumns, "StarRocks");
    List<HdfsPathColumnMapping> hdfsColumnMappings = new ArrayList<>();

    for (HdfsSchema hdfsSchema : hdfsSchemas) {
      hdfsColumnMappings.add(
          buildHdfsColumnMapping(
              starRocksTable, starRocksColumns, starRocksColumnsByName, hdfsSchema));
    }

    return new BackfillProjection(
        starRocksColumns.stream().map(SchemaColumn::name).toList(),
        hdfsColumnMappings,
        skippedPaths);
  }

  private HdfsPathColumnMapping buildHdfsColumnMapping(
      String starRocksTable,
      List<SchemaColumn> starRocksColumns,
      Map<String, SchemaColumn> starRocksColumnsByName,
      HdfsSchema hdfsSchema) {

    Map<String, SchemaColumn> hdfsColumnsByName = index(hdfsSchema.columns(), hdfsSchema.path());
    rejectHdfsOnlyColumns(
        starRocksTable, starRocksColumnsByName, hdfsColumnsByName, hdfsSchema.path());

    List<String> expressions = new ArrayList<>();
    int directlyMappedCount = 0;
    int nullFilledCount = 0;

    for (SchemaColumn starRocksColumn : starRocksColumns) {

      String normalizedName = normalize(starRocksColumn.name());
      SchemaColumn hdfsColumn = hdfsColumnsByName.get(normalizedName);

      if (hdfsColumn != null) {
        requireCompatibleTypes(hdfsColumn, starRocksColumn, hdfsSchema.path());
        expressions.add(
            "CAST(" + identifier(hdfsColumn.name()) + " AS " + starRocksColumn.type() + ")");
        directlyMappedCount++;
      } else if (starRocksColumn.nullable()) {
        expressions.add("CAST(NULL AS " + starRocksColumn.type() + ")");
        nullFilledCount++;
      } else {
        throw missingRequiredStarRocksColumn(starRocksTable, starRocksColumn, hdfsSchema.path());
      }
    }

    String mappingSummary =
        directlyMappedCount
            + " matched"
            + (nullFilledCount == 0 ? "" : ", " + nullFilledCount + " NULL-filled");
    return new HdfsPathColumnMapping(hdfsSchema.path(), expressions, mappingSummary);
  }

  private void rejectHdfsOnlyColumns(
      String starRocksTable,
      Map<String, SchemaColumn> starRocksColumnsByName,
      Map<String, SchemaColumn> hdfsColumnsByName,
      String path) {
    List<SchemaColumn> missing =
        hdfsColumnsByName.values().stream()
            .filter(column -> !starRocksColumnsByName.containsKey(normalize(column.name())))
            .toList();
    if (missing.isEmpty()) {
      return;
    }

    String names = missing.stream().map(SchemaColumn::name).collect(Collectors.joining(", "));
    String alterations =
        missing.stream()
            .map(
                column ->
                    "ALTER TABLE "
                        + starRocksTable
                        + " ADD COLUMN "
                        + identifier(column.name().toLowerCase(Locale.ROOT))
                        + " "
                        + column.type()
                        + " NULL;")
            .collect(Collectors.joining(" "));
    throw new BackfillException(
        "StarRocks is missing HDFS column(s) "
            + names
            + " from "
            + path
            + ". Add them before backfill, for example: "
            + alterations);
  }

  private BackfillException missingRequiredStarRocksColumn(
      String starRocksTable, SchemaColumn starRocksColumn, String path) {
    return new BackfillException(
        "HDFS path "
            + path
            + " is missing required StarRocks column "
            + starRocksColumn.name()
            + ". Old Parquet files cannot be changed; allow NULL for old versions, "
            + "for example: ALTER TABLE "
            + starRocksTable
            + " MODIFY COLUMN "
            + identifier(starRocksColumn.name())
            + " "
            + starRocksColumn.type()
            + " NULL;");
  }

  private void requireCompatibleTypes(
      SchemaColumn hdfsColumn, SchemaColumn starRocksColumn, String path) {
    if (!typeCompatibilityGroup(hdfsColumn.type())
        .equals(typeCompatibilityGroup(starRocksColumn.type()))) {
      throw new BackfillException(
          "Incompatible column types in "
              + path
              + ": HDFS "
              + hdfsColumn.name()
              + " is "
              + hdfsColumn.type()
              + ", but StarRocks "
              + starRocksColumn.name()
              + " is "
              + starRocksColumn.type());
    }
  }

  private String typeCompatibilityGroup(String type) {
    String normalizedType = normalize(type).replace(" unsigned", "");
    String base = normalizedType;
    int parameters = base.indexOf('(');
    if (parameters >= 0) {
      base = base.substring(0, parameters);
    }
    if (NUMBER_TYPES.contains(base)) {
      if (base.startsWith("decimal")) {
        return normalizedType;
      }
      return base.equals("integer") ? "int" : base;
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
        throw new BackfillException(
            owner + " exposes duplicate column " + column.name() + " ignoring case");
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
