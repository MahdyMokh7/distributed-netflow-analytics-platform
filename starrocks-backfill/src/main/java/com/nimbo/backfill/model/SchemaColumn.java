package com.nimbo.backfill.model;

import java.util.Objects;

public record SchemaColumn(String name, String type, boolean nullable) {

  public SchemaColumn {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(type, "type must not be null");
    if (name.isBlank() || type.isBlank()) {
      throw new IllegalArgumentException("Schema column name and type must not be blank");
    }
  }
}
