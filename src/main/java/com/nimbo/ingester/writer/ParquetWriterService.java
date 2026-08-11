package com.nimbo.ingester.writer;

import com.nimbo.ingester.model.IngestedRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetWriterService implements AutoCloseable {

    private final Configuration hadoopConfiguration;
    private final Path outputPath;

    private ParquetWriter<GenericRecord> writer;

    public ParquetWriterService(
            String hdfsUri,
            String outputPath
    ) {

        this.hadoopConfiguration =
                new Configuration();

        this.hadoopConfiguration.set(
                "fs.defaultFS",
                hdfsUri
        );

        this.outputPath =
                new Path(outputPath);
    }

    public void write(

            IngestedRecord ingestedRecord
    ) throws Exception {
        GenericRecord record =
                ingestedRecord.record();

        System.out.println(
                "Written: topic="
                        + ingestedRecord.topic()
                        + ", partition="
                        + ingestedRecord.partition()
                        + ", offset="
                        + ingestedRecord.offset()
        );
    }

    @Override
    public void close() throws Exception {

        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
