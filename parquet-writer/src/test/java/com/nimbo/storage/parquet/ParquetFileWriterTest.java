package com.nimbo.storage.parquet;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbo.config.ParquetProperties;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetFileWriterTest {

  @TempDir java.nio.file.Path tempDir;

  private ParquetProperties createProperties() {

    ParquetProperties properties = new ParquetProperties();
    properties.setCompressionCodec("SNAPPY");
    properties.setBlockSizeBytes(1024 * 1024);
    properties.setPageSizeBytes(1024 * 1024);
    properties.setEnableDictionary(true);

    return properties;
  }

  private Schema createSchema() {

    return new Schema.Parser()
        .parse(
            """
            {
              "type":"record",
              "name":"Test",
              "fields":[
                {
                  "name":"value",
                  "type":"string"
                }
              ]
            }
            """);
  }

  private FileSystem createFileSystem() throws Exception {

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    return FileSystem.newInstance(URI.create("file:///"), conf);
  }

  @Test
  void shouldCreateWriteAndCloseParquetWriter() throws Exception {

    FileSystem fs = createFileSystem();
    ParquetFileWriter writerService = new ParquetFileWriter(fs, createProperties());
    Path path = new Path(tempDir.resolve("test.parquet").toString());
    Schema schema = createSchema();
    ParquetWriter<GenericRecord> writer = writerService.createWriter(path, schema);
    GenericRecord testRecord = new GenericData.Record(schema);
    testRecord.put("value", "hello");
    writerService.write(writer, testRecord);
    writerService.closeWriter(writer);

    assertTrue(fs.exists(path));
  }

  @Test
  void shouldRejectNullWriterDependencies() throws Exception {

    FileSystem fs = createFileSystem();
    ParquetProperties properties = createProperties();

    assertThrows(NullPointerException.class, () -> new ParquetFileWriter(null, properties));
    assertThrows(NullPointerException.class, () -> new ParquetFileWriter(fs, null));
  }

  @Test
  void shouldRejectNullCreateWriterArguments() throws Exception {

    FileSystem fs = createFileSystem();
    ParquetProperties properties = createProperties();
    ParquetFileWriter writerService = new ParquetFileWriter(fs, properties);
    Path validPath = new Path("/tmp/test.parquet");
    Schema validSchema = createSchema();

    assertThrows(NullPointerException.class, () -> writerService.createWriter(null, validSchema));
    assertThrows(NullPointerException.class, () -> writerService.createWriter(validPath, null));
  }

  @Test
  void shouldRejectNullWriteArguments() throws Exception {

    FileSystem fs = createFileSystem();
    ParquetFileWriter writerService = new ParquetFileWriter(fs, createProperties());
    assertThrows(NullPointerException.class, () -> writerService.write(null, null));
  }
}
