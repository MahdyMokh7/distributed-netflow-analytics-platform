package com.nimbo.backfill.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public final class BackfillParquetTestData {

    private static final String TARGET_TABLE = "nimbo.netflow";
    private static final String PROCESS_TIME = "PROCESS_TIME";

    private BackfillParquetTestData() {
    }

    public static void write(Connection connection, Path parquetFile, List<SourceRow> rows)
            throws SQLException, IOException {
        Schema schema = sourceSchema(connection);
        Files.createDirectories(parquetFile.getParent());
        Files.deleteIfExists(parquetFile);

        Configuration configuration = new Configuration(false);
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(parquetFile.toUri());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                        HadoopOutputFile.fromPath(outputPath, configuration))
                .withSchema(schema)
                .withConf(configuration)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (SourceRow row : rows) {
                writer.write(record(schema, row));
            }
        }
    }

    private static Schema sourceSchema(Connection connection) throws SQLException {
        List<Schema.Field> fields = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("DESC " + TARGET_TABLE)) {
            while (result.next()) {
                String starRocksName = result.getString("Field");
                String sourceName = starRocksName.toUpperCase(Locale.ROOT);
                fields.add(new Schema.Field(sourceName, avroType(result.getString("Type")), null, (Object) null));
            }
        }
        if (fields.isEmpty()) {
            throw new SQLException("StarRocks table schema returned no columns");
        }

        Schema schema = Schema.createRecord("BackfillSourceRecord", null, "com.nimbo.backfill.test", false);
        schema.setFields(fields);
        return schema;
    }

    private static Schema avroType(String starRocksType) {
        String baseType = starRocksType.toLowerCase(Locale.ROOT).replace(" unsigned", "");
        int parameters = baseType.indexOf('(');
        if (parameters >= 0) {
            baseType = baseType.substring(0, parameters);
        }

        return switch (baseType) {
            case "char", "varchar", "string" -> Schema.create(Schema.Type.STRING);
            case "tinyint", "smallint", "int", "integer" -> Schema.create(Schema.Type.INT);
            case "bigint", "largeint" -> Schema.create(Schema.Type.LONG);
            case "float" -> Schema.create(Schema.Type.FLOAT);
            case "double", "decimal", "decimal32", "decimal64", "decimal128", "decimal256" ->
                Schema.create(Schema.Type.DOUBLE);
            default -> throw new IllegalArgumentException("Unsupported StarRocks test type: " + starRocksType);
        };
    }

    private static GenericRecord record(Schema schema, SourceRow row) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = switch (field.schema().getType()) {
                case STRING -> "value";
                case INT -> 1;
                case LONG -> 1L;
                case FLOAT -> 1.0f;
                case DOUBLE -> 1.0d;
                default -> throw new IllegalArgumentException("Unsupported test field: " + field.name());
            };
            record.put(field.name(), value);
        }

        record.put("IPV4_SRC_ADDR", row.sourceIp());
        record.put("IPV4_DST_ADDR", "198.51.100.20");
        record.put("KAFKA_TOPIC", "netflow-source");
        record.put("KAFKA_PARTITION", 3);
        record.put("KAFKA_OFFSET", row.kafkaOffset());
        record.put("IN_BYTES", row.inBytes());
        record.put("OUT_BYTES", row.outBytes());
        record.put("IN_PKTS", row.inPackets());
        record.put("OUT_PKTS", row.outPackets());
        record.put(PROCESS_TIME, row.timestampMillis());
        return record;
    }

    public record SourceRow(String sourceIp, long timestampMillis, long kafkaOffset, long inBytes, long outBytes,
            long inPackets, long outPackets) {
    }
}
