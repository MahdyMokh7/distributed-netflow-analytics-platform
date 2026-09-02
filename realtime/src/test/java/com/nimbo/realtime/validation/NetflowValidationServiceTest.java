package com.nimbo.realtime.validation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NetflowValidationServiceTest {

    private NetflowValidationService service;
    private Schema schema;

    @BeforeEach
    void setUp() {
        service = new NetflowValidationService();

        schema = new Schema.Parser().parse("""
                {
                  "type":"record",
                  "name":"NetFlowV2Record",
                  "fields":[
                    {"name":"IPV4_SRC_ADDR","type":"string"},
                    {"name":"IPV4_DST_ADDR","type":"string"},
                    {"name":"L4_SRC_PORT","type":"int"},
                    {"name":"L4_DST_PORT","type":"int"},
                    {"name":"PROTOCOL","type":"int"},
                    {"name":"MIN_TTL","type":"int"},
                    {"name":"MAX_TTL","type":"int"},
                    {"name":"MIN_IP_PKT_LEN","type":"int"},
                    {"name":"MAX_IP_PKT_LEN","type":"int"},
                    {"name":"ICMP_TYPE","type":"int"},
                    {"name":"ICMP_IPV4_TYPE","type":"int"},
                    {"name":"DNS_QUERY_ID","type":"int"},
                    {"name":"DNS_QUERY_TYPE","type":"int"},
                    {"name":"IN_BYTES","type":"long"},
                    {"name":"OUT_BYTES","type":"long"},
                    {"name":"IN_PKTS","type":"long"},
                    {"name":"OUT_PKTS","type":"long"},
                    {"name":"FLOW_DURATION_MILLISECONDS","type":"long"},
                    {"name":"DURATION_IN","type":"long"},
                    {"name":"DURATION_OUT","type":"long"},
                    {"name":"LONGEST_FLOW_PKT","type":"long"},
                    {"name":"SHORTEST_FLOW_PKT","type":"long"},
                    {"name":"L7_PROTO","type":"double"}
                  ]
                }
                """);
    }

    private GenericRecord validRecord() {
        GenericRecord record = new GenericData.Record(schema);

        record.put("IPV4_SRC_ADDR", "192.168.1.10");
        record.put("IPV4_DST_ADDR", "8.8.8.8");
        record.put("L4_SRC_PORT", 1000);
        record.put("L4_DST_PORT", 443);
        record.put("PROTOCOL", 6);
        record.put("MIN_TTL", 20);
        record.put("MAX_TTL", 30);
        record.put("MIN_IP_PKT_LEN", 64);
        record.put("MAX_IP_PKT_LEN", 512);
        record.put("ICMP_TYPE", 0);
        record.put("ICMP_IPV4_TYPE", 0);
        record.put("DNS_QUERY_ID", 1);
        record.put("DNS_QUERY_TYPE", 1);
        record.put("IN_BYTES", 100);
        record.put("OUT_BYTES", 200);
        record.put("IN_PKTS", 10);
        record.put("OUT_PKTS", 20);
        record.put("FLOW_DURATION_MILLISECONDS", 1000);
        record.put("DURATION_IN", 100);
        record.put("DURATION_OUT", 200);
        record.put("LONGEST_FLOW_PKT", 512);
        record.put("SHORTEST_FLOW_PKT", 64);
        record.put("L7_PROTO", 7.5);

        return record;
    }

    @Test
    void shouldAcceptValidRecord() {
        ValidationResult result = service.validate(validRecord());

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isNull();
    }

    @Test
    void shouldRejectNullRecord() {
        ValidationResult result = service.validate(null);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).contains("Record is null");
    }

    @Test
    void shouldRejectInvalidSourceIpv4() {
        GenericRecord record = validRecord();
        record.put("IPV4_SRC_ADDR", "999.1.1.1");

        ValidationResult result = service.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).contains("IPV4_SRC_ADDR invalid IPv4");
    }

    @Test
    void shouldRejectPortOutsideRange() {
        GenericRecord record = validRecord();
        record.put("L4_SRC_PORT", 70000);

        ValidationResult result = service.validate(record);

        assertThat(result.reason()).contains("L4_SRC_PORT out of range");
    }

    @Test
    void shouldRejectNegativeCounters() {
        GenericRecord record = validRecord();
        record.put("IN_BYTES", -1L);

        ValidationResult result = service.validate(record);

        assertThat(result.reason()).contains("IN_BYTES is negative");
    }

    @Test
    void shouldRejectMinGreaterThanMax() {
        GenericRecord record = validRecord();
        record.put("MIN_TTL", 100);
        record.put("MAX_TTL", 10);

        ValidationResult result = service.validate(record);

        assertThat(result.reason()).contains("MIN_TTL > MAX_TTL");
    }

    @Test
    void shouldRejectNegativeL7Protocol() {
        GenericRecord record = validRecord();
        record.put("L7_PROTO", -1.0);

        ValidationResult result = service.validate(record);

        assertThat(result.reason()).contains("L7_PROTO is negative");
    }

    @Test
    void shouldRejectInfiniteL7Protocol() {
        GenericRecord record = validRecord();
        record.put("L7_PROTO", Double.POSITIVE_INFINITY);

        ValidationResult result = service.validate(record);

        assertThat(result.reason()).contains("L7_PROTO is not finite");
    }
}
