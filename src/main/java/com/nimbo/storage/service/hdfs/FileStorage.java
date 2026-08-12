package com.nimbo.storage.service.hdfs;

import org.apache.hadoop.fs.Path;
import java.io.IOException;

public interface FileStorage {
    void ensureDirectoryExists(Path path) throws IOException;
    void commitAtomic(Path tempPath, Path finalPath) throws IOException;
    void cleanupTempFile(Path tempPath);
}