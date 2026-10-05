package com.nimbo.backfill.model;

import java.util.List;

public record HdfsSchema(String path, List<SchemaColumn> columns) {

    public HdfsSchema {
        columns = List.copyOf(columns);
        if (path.isBlank() || columns.isEmpty()) {
            throw new IllegalArgumentException("HDFS path and columns must not be empty");
        }
    }
}
