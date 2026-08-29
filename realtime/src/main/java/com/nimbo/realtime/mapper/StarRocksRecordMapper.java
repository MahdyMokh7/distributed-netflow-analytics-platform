package com.nimbo.realtime.mapper;

import com.nimbo.realtime.model.StarRocksRow;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StarRocksRecordMapper {

    private final SchemaMapping schemaMapping;

    public StarRocksRow map(
            GenericRecord genericRecord,
            String topic,
            int partition,
            long offset,
            long kafkaTimestamp
    ) {

        return new StarRocksRow(
                topic,
                partition,
                offset,
                kafkaTimestamp,
                schemaMapping.extractValues(genericRecord)
        );
    }
}
