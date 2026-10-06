package com.nimbo.cubemaker.writer;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.spark.sql.Row;

@FunctionalInterface
public interface RowMapper<T> extends Serializable {

  T map(Row row, UUID batchId, Timestamp ingestedAt);
}
