package com.nimbo.storage.service.parquet;

import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.exception.ParquetWriteException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

@Component
public final class ParquetFileWriter {

    private final Configuration hadoopConfiguration;
    private final ParquetProperties parquetProperties;

    public ParquetFileWriter(FileSystem fileSystem, ParquetProperties parquetProperties) {
        Objects.requireNonNull(fileSystem, "fileSystem must not be null");
        this.parquetProperties = Objects.requireNonNull(parquetProperties, "parquetProperties must not be null");
        this.hadoopConfiguration = fileSystem.getConf();
    }


    public ParquetWriter<GenericRecord> createWriter(Path targetPath, Schema schema)
            throws ParquetWriteException {
        Objects.requireNonNull(targetPath, "targetPath must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        CompressionCodecName codec = CompressionCodecName.valueOf(
                parquetProperties.getCompressionCodec().toUpperCase()
        );

        return buildWriter(targetPath, schema, codec);
    }

    public void write(ParquetWriter<GenericRecord> writer, GenericRecord record)
            throws ParquetWriteException {
        Objects.requireNonNull(writer, "writer must not be null");
        Objects.requireNonNull(record, "record must not be null");

        try {
            writer.write(record);
        } catch (IOException e) {
            throw new ParquetWriteException("Failed writing record to Parquet writer", e);
        }
    }

    public void closeWriter(ParquetWriter<GenericRecord> writer)
            throws ParquetWriteException {
        Objects.requireNonNull(writer, "writer must not be null");

        try {
            writer.close();
        } catch (IOException e) {
            throw new ParquetWriteException("Failed closing Parquet writer", e);
        }
    }


    private ParquetWriter<GenericRecord> buildWriter(
            Path targetPath,
            Schema schema,
            CompressionCodecName codec
    ) throws ParquetWriteException {
        try {
            return AvroParquetWriter
                    .<GenericRecord>builder(targetPath)
                    .withSchema(schema)
                    .withConf(hadoopConfiguration)
                    .withCompressionCodec(codec)
                    .withRowGroupSize(parquetProperties.getBlockSizeBytes())
                    .withPageSize(parquetProperties.getPageSizeBytes())
                    .withDictionaryEncoding(parquetProperties.isEnableDictionary())
                    .build();
        } catch (IOException e) {
            throw new ParquetWriteException("Failed creating Parquet writer for path: " + targetPath, e);
        }
    }
}
