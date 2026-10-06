package com.nimbo.cubemaker.exception;

import java.util.UUID;

public class BatchProcessingException extends RuntimeException {
  private final UUID batchId;

  public BatchProcessingException(UUID batchId, String message, Throwable cause) {
    super(message, cause);
    this.batchId = batchId;
  }

  public UUID getBatchId() {
    return batchId;
  }
}
