package com.nimbo.realtime.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaMappingTest {

    private Schema schema() {
        return new Schema.Parser().parse("""
                {
                  "type":"record",
                  "name":"TestRecord",
                  "fields":[
                    {
                      "name":"SRC_IP",
                      "type":"string"
                    },
                    {
                      "name":"PACKETS",
                      "type":"long"
                    },
                    {
                      "name":"RATIO",
                      "type":"double"
                    },
                    {
                      "name":"COUNT",
                      "type":"int"
                    }
                  ]
                }
                """);
    }

    @Test
    void shouldGenerateStarRocksColumns() {
        SchemaMapping mapping = new SchemaMapping(schema());

        assertThat(mapping.starRocksColumns())
                .isEqualTo("kafka_topic,kafka_partition,kafka_offset,kafka_timestamp,src_ip,packets,ratio,count");
    }

    @Test
    void shouldExtractAndNormalizeValues() {
        SchemaMapping mapping = new SchemaMapping(schema());
        GenericRecord record = new GenericData.Record(schema());

        record.put("SRC_IP", "10.0.0.1");
        record.put("PACKETS", 100);
        record.put("RATIO", 1);
        record.put("COUNT", 5);

        List<Object> values = mapping.extractValues(record);

        assertThat(values).containsExactly("10.0.0.1", 100L, 1.0d, 5);
    }

    @Test
    void shouldKeepNullValues() {
        SchemaMapping mapping = new SchemaMapping(schema());
        GenericRecord record = new GenericData.Record(schema());

        record.put("SRC_IP", null);
        record.put("PACKETS", null);
        record.put("RATIO", null);
        record.put("COUNT", null);

        assertThat(mapping.extractValues(record)).containsExactly(null, null, null, null);
    }
}
