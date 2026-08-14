package com.nimbo.storage.service.hdfs;

import com.nimbo.storage.exception.HdfsStorageException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

@Component
public final class HdfsFileStorage implements FileStorage {

    private static final Logger logger = LoggerFactory.getLogger(HdfsFileStorage.class);

    private final FileSystem fileSystem;

    public HdfsFileStorage(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null");
    }

    @Override
    public void ensureDirectoryExists(Path path) throws IOException {
        if (!fileSystem.exists(path)) {
            boolean created = fileSystem.mkdirs(path);
            if (!created && !fileSystem.exists(path)) {
                throw new HdfsStorageException("Failed to create HDFS directory: " + path);
            }
            logger.info("Created HDFS directory: {}", path);
        }

    }

    @Override
    public void commitAtomic(Path tempPath, Path finalPath) throws IOException {
        if (fileSystem.exists(finalPath)) {
            throw new HdfsStorageException("Target Parquet file already exists: " + finalPath);
        }

        boolean renamed = fileSystem.rename(tempPath, finalPath);
        if (!renamed) {
            throw new HdfsStorageException("Failed to rename temporary Parquet file: " + tempPath + " -> " + finalPath);
        }
    }

    @Override
    public void cleanupTempFile(Path tempPath) {
        try {
            if (fileSystem.exists(tempPath)) {
                boolean deleted = fileSystem.delete(tempPath, false);
                if (!deleted) {
                    logger.warn("Failed to delete temporary HDFS file: {}", tempPath);
                }
            }
        } catch (IOException e) {
            logger.warn("Failed during temporary file cleanup: {}", tempPath, e);
        }
    }
}