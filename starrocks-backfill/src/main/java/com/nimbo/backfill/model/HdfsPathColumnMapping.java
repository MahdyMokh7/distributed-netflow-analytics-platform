package com.nimbo.backfill.model;

import java.util.List;

public record HdfsPathColumnMapping(String path, List<String> expressions, String mappingSummary) {

  public HdfsPathColumnMapping {
    expressions = List.copyOf(expressions);
    if (path.isBlank() || expressions.isEmpty()) {
      throw new IllegalArgumentException("HDFS mapping path and expressions must not be empty");
    }
  }
}
