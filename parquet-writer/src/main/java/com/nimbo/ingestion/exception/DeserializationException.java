package com.nimbo.ingestion.exception;

public class DeserializationException extends RuntimeException {

  public DeserializationException(String message) {
    super(message);
  }

  public DeserializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
