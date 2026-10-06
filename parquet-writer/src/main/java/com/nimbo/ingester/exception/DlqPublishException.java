package com.nimbo.ingester.exception;

public class DlqPublishException extends RuntimeException {

  public DlqPublishException(String message, Throwable cause) {
    super(message, cause);
  }
}
