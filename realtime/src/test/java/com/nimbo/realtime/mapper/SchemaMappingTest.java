package com.nimbo.realtime.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SchemaMappingTest {

    private final Schema schema = new Schema.Parser().parse("""
            {
              "type":"record",
              "name":"TestRecord",
              "fields":[
                {"name":"SRC_ADDR","type":"string"},
                {"name":"PORT","type":"int"},
                {"name":"BYTES","type":"long"},
                {"name":"RATIO","type":"double"},
                {"name":"OPTIONAL","type":["null","string"],"default":null}
              ]
            }
            """);

    @Test
    void buildsKafkaAndAvroColumnList() {
        SchemaMapping mapping = new SchemaMapping(schema);

        assertEquals(
                "kafka_topic,kafka_partition,kafka_offset,kafka_timestamp,src_addr,port,bytes,ratio,optional",
                mapping.starRocksColumns()
        );
    }

    @Test
    void extractsAndNormalizesValuesInSchemaOrder() {
        SchemaMapping mapping = new SchemaMapping(schema);
        GenericRecord record = new GenericData.Record(schema);
        record.put("SRC_ADDR", "10.0.0.1");
        record.put("PORT", 443);
        record.put("BYTES", 1234L);
        record.put("RATIO", 1.5d);
        record.put("OPTIONAL", null);

        List<Object> values = mapping.extractValues(record);

        assertEquals("10.0.0.1", values.get(0));
        assertEquals(443, values.get(1));
        assertEquals(1234L, values.get(2));
        assertEquals(1.5d, values.get(3));
        assertNull(values.get(4));
    }
}
