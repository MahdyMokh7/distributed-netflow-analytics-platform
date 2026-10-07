package com.nimbo.starrockswriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class SchemaMappingTest {

  private Schema schema() {
    return new Schema.Parser()
        .parse(
            """
                {
                 "type":"record",
                 "name":"TestRecord",
                 "fields":[
                  {"name":"SRC_IP","type":"string"},
                  {"name":"PACKETS","type":"long"},
                  {"name":"RATIO","type":"double"},
                  {"name":"COUNT","type":"int"}
                 ]
                }
                """);
  }

  @Test
  void shouldGenerateStarRocksColumns() {
    SchemaMapping mapping = new SchemaMapping();

    assertThat(mapping.starRocksColumns(List.of("src_ip", "packets", "ratio", "count")))
        .isEqualTo(
            "kafka_topic,kafka_partition,kafka_offset,PROCESS_TIME,src_ip,packets,ratio,count");
  }

  @Test
  void shouldExtractAndNormalizeValuesByColumnName() {
    SchemaMapping mapping = new SchemaMapping();
    GenericRecord record = new GenericData.Record(schema());

    record.put("SRC_IP", "10.0.0.1");
    record.put("PACKETS", 100L);
    record.put("RATIO", 1.0d);
    record.put("COUNT", 5);

    Map<String, Object> values = mapping.extractValues(record);

    assertThat(values)
        .containsEntry("src_ip", "10.0.0.1")
        .containsEntry("packets", 100L)
        .containsEntry("ratio", 1.0d)
        .containsEntry("count", 5);
  }

  @Test
  void shouldRequestSafeIntToBigintWidening() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertThat(new SchemaMapping().typeEvolution("INT", longSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.WIDEN);
  }

  @Test
  void shouldTreatStarRocksIntDisplayWidthAsInt() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertThat(new SchemaMapping().typeEvolution("int(11)", intSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.COMPATIBLE);
  }

  @Test
  void shouldWidenStarRocksIntDisplayWidthToBigint() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertThat(new SchemaMapping().typeEvolution("int(11)", longSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.WIDEN);
  }

  @Test
  void shouldTreatStarRocksBigintDisplayWidthAsBigint() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertThat(new SchemaMapping().typeEvolution("bigint(20)", longSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.COMPATIBLE);
  }

  @Test
  void shouldAcceptExistingBigintForIntWriterSchema() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertThat(new SchemaMapping().typeEvolution("BIGINT", intSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.COMPATIBLE);
  }

  @Test
  void shouldRejectUnsafeTypeChange() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertThat(new SchemaMapping().typeEvolution("DOUBLE", longSchema))
        .isEqualTo(SchemaMapping.TypeEvolution.INCOMPATIBLE);
  }

  @Test
  void shouldMapNullableUnionToStarRocksType() {
    Schema nullableLong = new Schema.Parser().parse("[\"null\",\"long\"]");
    assertThat(new SchemaMapping().starRocksType(nullableLong)).isEqualTo("BIGINT");
  }
}
