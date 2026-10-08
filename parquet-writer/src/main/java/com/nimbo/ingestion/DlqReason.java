package com.nimbo.ingestion;

public enum DlqReason {
  VALIDATION_ERROR,
  DESERIALIZATION_ERROR,
  UNSUPPORTED_SCHEMA,
  LATE_RECORD
}
