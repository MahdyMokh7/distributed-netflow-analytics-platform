package com.nimbo.storage.service.hdfs;

import com.nimbo.storage.exception.HdfsStorageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public final class HdfsFileStorage implements FileStorage {

    private final FileSystem fileSystem;

    public HdfsFileStorage(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null");
    }

    @Override
    public void ensureDirectoryExists(Path path) {
        Objects.requireNonNull(path, "path must not be null");

        try {
            if (fileSystem.exists(path)) {
                return;
            }
            boolean created = fileSystem.mkdirs(path);
            if (created) {
                log.info("Created HDFS directory: {}", path);
                return;
            }
            if (!fileSystem.exists(path)) {
                throw new HdfsStorageException(
                        "Failed to create HDFS directory: " + path
                );
            }
            log.debug("HDFS directory already created by another consumer: {}", path);

        } catch (IOException e) {
            throw new HdfsStorageException(
                    "Failed accessing HDFS directory: " + path,
                    e
            );
        }
    }

    @Override
    public void commitAtomic(Path tempPath, Path finalPath) {
        Objects.requireNonNull(tempPath, "tempPath must not be null");
        Objects.requireNonNull(finalPath, "finalPath must not be null");

        try {
            if (fileSystem.exists(finalPath)) {
                throw new HdfsStorageException(
                        "Target Parquet file already exists: " + finalPath
                );
            }
            if (!fileSystem.rename(tempPath, finalPath)) {
                throw new HdfsStorageException(
                        "Failed to rename temporary Parquet file: "
                                + tempPath + " -> " + finalPath
                );
            }
        } catch (IOException e) {
            throw new HdfsStorageException(
                    "Failed committing temporary Parquet file: "
                            + tempPath + " -> " + finalPath, e
            );
        }
    }

    @Override
    public void cleanupTempFile(Path tempPath) {
        Objects.requireNonNull(tempPath, "tempPath must not be null");

        try {
            if (fileSystem.exists(tempPath)) {
                boolean deleted = fileSystem.delete(tempPath, false);
                if (!deleted) {
                    log.warn("Failed to delete temporary HDFS file: {}", tempPath);
                }
            }
        } catch (IOException e) {
            log.warn("Failed during temporary file cleanup: {}", tempPath, e);
        }
    }
}