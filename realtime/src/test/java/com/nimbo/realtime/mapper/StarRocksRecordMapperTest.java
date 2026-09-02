package com.nimbo.realtime.mapper;

import com.nimbo.realtime.model.StarRocksRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StarRocksRecordMapperTest {

    private Schema schema() {
        return new Schema.Parser().parse("""
                {
                  "type":"record",
                  "name":"Netflow",
                  "fields":[
                    {
                      "name":"SRC_IP",
                      "type":["null","string"],
                      "default":null
                    },
                    {
                      "name":"BYTES",
                      "type":["null","long"],
                      "default":null
                    }
                  ]
                }
                """);
    }

    private GenericRecord record() {
        GenericRecord record = new GenericData.Record(schema());
        record.put("SRC_IP", "10.0.0.1");
        record.put("BYTES", 100L);
        return record;
    }

    @Test
    void shouldMapKafkaMetadataAndAvroValues() {
        SchemaMapping mapping = new SchemaMapping(schema());
        StarRocksRecordMapper mapper = new StarRocksRecordMapper(mapping);

        StarRocksRow row = mapper.map(
                record(),
                "netflow-topic",
                2,
                500L,
                123456L
        );

        assertThat(row.kafkaTopic()).isEqualTo("netflow-topic");
        assertThat(row.kafkaPartition()).isEqualTo(2);
        assertThat(row.kafkaOffset()).isEqualTo(500L);
        assertThat(row.kafkaTimestamp()).isEqualTo(123456L);
        assertThat(row.netFlowValues()).containsExactly("10.0.0.1", 100L);
    }

    @Test
    void shouldPreserveNullAvroValues() {
        GenericRecord record = new GenericData.Record(schema());
        record.put("SRC_IP", null);
        record.put("BYTES", null);

        StarRocksRecordMapper mapper = new StarRocksRecordMapper(new SchemaMapping(schema()));

        StarRocksRow row = mapper.map(record, "topic", 0, 1L, 2L);

        assertThat(row.netFlowValues()).containsExactly(null, null);
    }

    @Test
    void shouldReturnStarRocksRowWithCorrectValueOrder() {
        StarRocksRecordMapper mapper = new StarRocksRecordMapper(new SchemaMapping(schema()));

        StarRocksRow row = mapper.map(record(), "topic", 1, 2L, 3L);

        assertThat(row.values()).containsExactly("topic", 1, 2L, 3L, "10.0.0.1", 100L);
    }

    @Test
    void shouldCreateDifferentRowsForDifferentRecords() {
        StarRocksRecordMapper mapper = new StarRocksRecordMapper(new SchemaMapping(schema()));

        StarRocksRow first = mapper.map(record(), "topic", 0, 1L, 2L);

        GenericRecord secondRecord = new GenericData.Record(schema());
        secondRecord.put("SRC_IP", "192.168.1.1");
        secondRecord.put("BYTES", 200L);

        StarRocksRow second = mapper.map(secondRecord, "topic", 0, 2L, 3L);

        assertThat(first.netFlowValues()).isNotEqualTo(second.netFlowValues());
    }
}
