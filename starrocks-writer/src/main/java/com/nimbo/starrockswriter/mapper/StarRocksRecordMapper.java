package com.nimbo.starrockswriter.mapper;

import com.nimbo.starrockswriter.model.StarRocksRow;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StarRocksRecordMapper {

  private final SchemaMapping schemaMapping;

  public StarRocksRow map(GenericRecord genericRecord, String topic, int partition, long offset, long processTime) {
    return new StarRocksRow(topic, partition, offset, processTime, schemaMapping.extractValues(genericRecord));
  }
}
