package com.nimbo.realtime.mapper;

import com.nimbo.realtime.model.StarRocksRow;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StarRocksRecordMapperTest {

    @Test
    void mapsKafkaMetadataAndNetflowValues() {
        SchemaMapping schemaMapping = mock(SchemaMapping.class);
        GenericRecord record = mock(GenericRecord.class);
        when(schemaMapping.extractValues(record)).thenReturn(List.of("10.0.0.1", 443));

        StarRocksRecordMapper mapper = new StarRocksRecordMapper(schemaMapping);
        StarRocksRow row = mapper.map(record, "netflow", 2, 99L, 123456L);

        assertEquals("netflow", row.kafkaTopic());
        assertEquals(2, row.kafkaPartition());
        assertEquals(99L, row.kafkaOffset());
        assertEquals(123456L, row.kafkaTimestamp());
        assertEquals(List.of("10.0.0.1", 443), row.netFlowValues());
    }
}
