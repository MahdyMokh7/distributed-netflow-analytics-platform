package com.nimbo.storage.hdfs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HdfsFileStorageTest {

  @TempDir java.nio.file.Path temp;

  private FileSystem fileSystem() throws Exception {

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");

    return FileSystem.newInstance(URI.create("file:///"), conf);
  }

  @Test
  void shouldCreateDirectory() throws Exception {

    FileSystem fs = fileSystem();
    HdfsFileStorage storage = new HdfsFileStorage(fs);
    Path path = new Path(temp.resolve("data").toString());
    storage.ensureDirectoryExists(path);

    assertTrue(fs.exists(path));
  }

  @Test
  void shouldCommitAtomicFile() throws Exception {

    FileSystem fs = fileSystem();
    HdfsFileStorage storage = new HdfsFileStorage(fs);
    Path tempFile = new Path(temp.resolve("a.tmp").toString());
    Path finalFile = new Path(temp.resolve("a.parquet").toString());
    fs.createNewFile(tempFile);
    storage.commitAtomic(tempFile, finalFile);

    assertTrue(fs.exists(finalFile));
    assertFalse(fs.exists(tempFile));
  }

  @Test
  void shouldCleanupTemporaryFile() throws Exception {

    FileSystem fs = fileSystem();
    HdfsFileStorage storage = new HdfsFileStorage(fs);
    Path tempFile = new Path(temp.resolve("delete.tmp").toString());
    fs.createNewFile(tempFile);
    storage.cleanupTempFile(tempFile);

    assertFalse(fs.exists(tempFile));
  }

  @Test
  void shouldRejectNullPath() throws Exception {

    HdfsFileStorage storage = new HdfsFileStorage(fileSystem());

    assertThrows(NullPointerException.class, () -> storage.ensureDirectoryExists(null));
  }
}
