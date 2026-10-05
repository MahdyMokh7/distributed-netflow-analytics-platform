package com.nimbo.backfill.model;

import java.util.List;

public record BackfillProjection(List<String> tableColumns, List<HdfsPathProjection> pathProjections,
        List<String> skippedPaths) {

    public BackfillProjection {
        tableColumns = List.copyOf(tableColumns);
        pathProjections = List.copyOf(pathProjections);
        skippedPaths = List.copyOf(skippedPaths);
        if (tableColumns.isEmpty() || pathProjections.isEmpty()) {
            throw new IllegalArgumentException("Backfill projection must contain columns and HDFS path projections");
        }
    }

    public record HdfsPathProjection(String path, List<String> expressions, String summary) {

        public HdfsPathProjection {
            expressions = List.copyOf(expressions);
            if (path.isBlank() || expressions.isEmpty()) {
                throw new IllegalArgumentException("HDFS projection path and expressions must not be empty");
            }
        }
    }
}
