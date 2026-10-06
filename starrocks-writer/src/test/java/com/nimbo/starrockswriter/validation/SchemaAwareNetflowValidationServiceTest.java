package com.nimbo.starrockswriter.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.common.validation.ValidationResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class SchemaAwareNetflowValidationServiceTest {

  private final SchemaAwareNetflowValidationService validator =
      new SchemaAwareNetflowValidationService();

  @Test
  void shouldAllowFieldsThatAreAbsentFromOlderSchema() {
    Schema schema =
        new Schema.Parser()
            .parse(
                """
                {
                  "type":"record",
                  "name":"OldNetflow",
                  "fields":[
                    {"name":"IPV4_SRC_ADDR","type":"string"},
                    {"name":"IPV4_DST_ADDR","type":"string"}
                  ]
                }
                """);

    GenericRecord record = new GenericData.Record(schema);
    record.put("IPV4_SRC_ADDR", "10.0.0.1");
    record.put("IPV4_DST_ADDR", "10.0.0.2");

    ValidationResult result = validator.validate(record);
    assertThat(result.valid()).isTrue();
  }

  @Test
  void shouldStillValidateFieldsThatExist() {
    Schema schema =
        new Schema.Parser()
            .parse(
                """
                {
                  "type":"record",
                  "name":"NetflowWithPort",
                  "fields":[
                    {"name":"L4_SRC_PORT","type":"int"}
                  ]
                }
                """);

    GenericRecord record = new GenericData.Record(schema);
    record.put("L4_SRC_PORT", 70000);

    ValidationResult result = validator.validate(record);
    assertThat(result.valid()).isFalse();
    assertThat(result.reason()).contains("L4_SRC_PORT out of range");
  }
}
