package com.nimbo.starrockswriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.starrockswriter.model.StarRocksRow;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class StarRocksRecordMapperTest {

  private Schema schema() {
    return new Schema.Parser()
        .parse(
            """
            {
              "type":"record",
              "name":"Netflow",
              "fields":[
                {"name":"SRC_IP","type":["null","string"],"default":null},
                {"name":"BYTES","type":["null","long"],"default":null}
              ]
            }
            """);
  }

  @Test
  void shouldMapKafkaMetadataAndAvroValuesByName() {
    GenericRecord record = new GenericData.Record(schema());
    record.put("SRC_IP", "10.0.0.1");
    record.put("BYTES", 100L);

    StarRocksRecordMapper mapper = new StarRocksRecordMapper(new SchemaMapping());
    StarRocksRow row = mapper.map(record, "netflow-topic", 2, 500L, 123456L);

    assertThat(row.kafkaTopic()).isEqualTo("netflow-topic");
    assertThat(row.processTime()).isEqualTo(123456L);
    assertThat(row.netFlowValues())
        .containsEntry("src_ip", "10.0.0.1")
        .containsEntry("bytes", 100L);
    assertThat(row.values(List.of("src_ip", "bytes", "future_field")))
        .containsExactly("netflow-topic", 2, 500L, 123456L, "10.0.0.1", 100L, null);
  }
}
