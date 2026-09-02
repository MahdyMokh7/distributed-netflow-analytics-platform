package com.nimbo.batch.writer;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

@FunctionalInterface
public interface RowMapper<T> extends Serializable {

    T map(Row row, UUID batchId, Timestamp ingestedAt);
}