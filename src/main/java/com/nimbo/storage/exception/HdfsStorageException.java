package com.nimbo.storage.exception;

public class HdfsStorageException extends StorageException {

    public HdfsStorageException(String message) {
        super(message);
    }

    public HdfsStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}