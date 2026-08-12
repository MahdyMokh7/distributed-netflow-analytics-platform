package com.nimbo.storage.service.parquet;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.exception.ParquetWriteException;
import com.nimbo.storage.service.batching.RecordBatch;
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

    public void writeToPath(RecordBatch batch, Path targetPath) throws ParquetWriteException {
        Objects.requireNonNull(batch, "batch must not be null");
        Objects.requireNonNull(targetPath, "targetPath must not be null");

        Schema schema = extractAndValidateSchema(batch);
        CompressionCodecName codec = CompressionCodecName.valueOf(
                parquetProperties.getCompressionCodec().toUpperCase()
        );

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(targetPath)
                .withSchema(schema)
                .withConf(hadoopConfiguration)
                .withCompressionCodec(codec)
                .withRowGroupSize(parquetProperties.getBlockSizeBytes())
                .withPageSize(parquetProperties.getPageSizeBytes())
                .withDictionaryEncoding(parquetProperties.isEnableDictionary())
                .build()) {

            for (IngestedRecord ingestedRecord : batch.getRecords()) {
                writer.write(ingestedRecord.record());
            }
        } catch (IOException e) {
            throw new ParquetWriteException("Failed writing Parquet payload to HDFS path: " + targetPath, e);
        }
    }

    private Schema extractAndValidateSchema(RecordBatch batch) {
        Schema schema = batch.getRecords().get(0).record().getSchema();

        for (IngestedRecord ingestedRecord : batch.getRecords()) {
            Schema currentSchema = ingestedRecord.record().getSchema();
            if (schema != currentSchema && !schema.equals(currentSchema)) {
                throw new IllegalArgumentException("Records in the same batch have inconsistent Avro schemas");
            }
        }
        return schema;
    }
}