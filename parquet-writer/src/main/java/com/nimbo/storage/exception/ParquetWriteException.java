package com.nimbo.storage.exception;

public class ParquetWriteException extends StorageException {

  public ParquetWriteException(String message) {
    super(message);
  }

  public ParquetWriteException(String message, Throwable cause) {
    super(message, cause);
  }
}
