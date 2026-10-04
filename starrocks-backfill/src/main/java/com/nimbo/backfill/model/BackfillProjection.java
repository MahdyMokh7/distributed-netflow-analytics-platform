package com.nimbo.backfill.model;

import java.util.List;

public record BackfillProjection(List<String> starRocksColumnNames,
        List<HdfsPathColumnMapping> hdfsColumnMappings, List<String> skippedPaths) {

    public BackfillProjection {
        starRocksColumnNames = List.copyOf(starRocksColumnNames);
        hdfsColumnMappings = List.copyOf(hdfsColumnMappings);
        skippedPaths = List.copyOf(skippedPaths);
        if (starRocksColumnNames.isEmpty() || hdfsColumnMappings.isEmpty()) {
            throw new IllegalArgumentException("Backfill projection must contain columns and HDFS column mappings");
        }
    }
}
