package com.nimbo.storage.service.hdfs;

import org.apache.hadoop.fs.Path;

public interface FileStorage {
    void ensureDirectoryExists(Path path);
    void commitAtomic(Path tempPath, Path finalPath);
    void cleanupTempFile(Path tempPath);
}