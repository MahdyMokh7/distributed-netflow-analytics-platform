package com.nimbo.common.validation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class NetflowValidationServiceTest {

    private NetflowValidationService validationService;
    private Schema schema;

    @BeforeEach
    void setUp() {
        validationService = new NetflowValidationService();
        schema = new Schema.Parser().parse("""
                {
                  "type":"record",
                  "name":"NetFlowV2Record",
                  "fields":[
                    {"name":"IPV4_SRC_ADDR","type":["null","string"],"default":null},
                    {"name":"IPV4_DST_ADDR","type":["null","string"],"default":null},
                    {"name":"L4_SRC_PORT","type":["null","int"],"default":null},
                    {"name":"L4_DST_PORT","type":["null","int"],"default":null},
                    {"name":"PROTOCOL","type":["null","int"],"default":null},
                    {"name":"MIN_TTL","type":["null","int"],"default":null},
                    {"name":"MAX_TTL","type":["null","int"],"default":null},
                    {"name":"MIN_IP_PKT_LEN","type":["null","int"],"default":null},
                    {"name":"MAX_IP_PKT_LEN","type":["null","int"],"default":null},
                    {"name":"ICMP_TYPE","type":["null","int"],"default":null},
                    {"name":"ICMP_IPV4_TYPE","type":["null","int"],"default":null},
                    {"name":"DNS_QUERY_ID","type":["null","int"],"default":null},
                    {"name":"DNS_QUERY_TYPE","type":["null","int"],"default":null},
                    {"name":"IN_BYTES","type":["null","long"],"default":null},
                    {"name":"OUT_BYTES","type":["null","long"],"default":null},
                    {"name":"IN_PKTS","type":["null","long"],"default":null},
                    {"name":"OUT_PKTS","type":["null","long"],"default":null},
                    {"name":"FLOW_DURATION_MILLISECONDS","type":["null","long"],"default":null},
                    {"name":"DURATION_IN","type":["null","long"],"default":null},
                    {"name":"DURATION_OUT","type":["null","long"],"default":null},
                    {"name":"LONGEST_FLOW_PKT","type":["null","long"],"default":null},
                    {"name":"SHORTEST_FLOW_PKT","type":["null","long"],"default":null},
                    {"name":"L7_PROTO","type":["null","double"],"default":null}
                  ]
                }
                """);
    }

    @Test
    void givenCompleteNetFlowAtSupportedBoundaries_whenValidated_thenRecordIsAccepted() {
        GenericRecord record = validRecord();
        record.put("L4_SRC_PORT", 0);
        record.put("L4_DST_PORT", 65_535);
        record.put("MIN_TTL", 0);
        record.put("MAX_TTL", 255);
        record.put("IN_BYTES", 0L);
        record.put("OUT_PKTS", 0L);

        ValidationResult result = validationService.validate(record);

        assertThat(result).isEqualTo(ValidationResult.success());
    }

    @ParameterizedTest
    @CsvSource({
            "IPV4_SRC_ADDR, '192.168.1', 'IPV4_SRC_ADDR invalid IPv4: 192.168.1'",
            "IPV4_DST_ADDR, '10.0..1', 'IPV4_DST_ADDR invalid IPv4: 10.0..1'",
            "IPV4_SRC_ADDR, '10.0.a.1', 'IPV4_SRC_ADDR invalid IPv4: 10.0.a.1'",
            "IPV4_SRC_ADDR, '10.0.-1.1', 'IPV4_SRC_ADDR invalid IPv4: 10.0.-1.1'",
            "IPV4_DST_ADDR, '10.0.0.256', 'IPV4_DST_ADDR invalid IPv4: 10.0.0.256'"
    })
    void givenMalformedIpv4_whenValidated_thenSpecificAddressIsRejected(
            String field, String value, String expectedError) {
        GenericRecord record = validRecord();
        record.put(field, value);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo(expectedError);
    }

    @ParameterizedTest
    @CsvSource({
            "L4_SRC_PORT, -1, 'L4_SRC_PORT out of range'",
            "L4_DST_PORT, 65536, 'L4_DST_PORT out of range'"
    })
    void givenPortOutsideUnsignedRange_whenValidated_thenPortIsRejected(
            String field, int value, String expectedError) {
        GenericRecord record = validRecord();
        record.put(field, value);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).contains(expectedError).contains(String.valueOf(value));
    }

    @ParameterizedTest
    @CsvSource({
            "MIN_TTL, -1, 'MIN_TTL out of range'",
            "MAX_TTL, 256, 'MAX_TTL out of range'"
    })
    void givenTtlOutsideUnsignedByteRange_whenValidated_thenTtlIsRejected(
            String field, int value, String expectedError) {
        GenericRecord record = validRecord();
        record.put(field, value);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).contains(expectedError).contains(String.valueOf(value));
    }

    @ParameterizedTest
    @CsvSource({
            "IN_BYTES, 'IN_BYTES is negative: -1'",
            "OUT_BYTES, 'OUT_BYTES is negative: -1'",
            "IN_PKTS, 'IN_PKTS is negative: -1'",
            "OUT_PKTS, 'OUT_PKTS is negative: -1'"
    })
    void givenNegativeTrafficCounter_whenValidated_thenCounterIsRejected(String field, String expectedError) {
        GenericRecord record = validRecord();
        record.put(field, -1L);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo(expectedError);
    }

    @Test
    void givenNegativePacketLengths_whenValidated_thenEveryPacketLengthIsRejected() {
        GenericRecord record = validRecord();
        record.put("LONGEST_FLOW_PKT", -1);
        record.put("SHORTEST_FLOW_PKT", -1);
        record.put("MIN_IP_PKT_LEN", -1);
        record.put("MAX_IP_PKT_LEN", -1);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason())
                .contains("LONGEST_FLOW_PKT")
                .contains("SHORTEST_FLOW_PKT")
                .contains("MIN_IP_PKT_LEN")
                .contains("MAX_IP_PKT_LEN");
    }

    @Test
    void givenInvertedMinimumAndMaximumValues_whenValidated_thenAllOrderingErrorsAreReported() {
        GenericRecord record = validRecord();
        record.put("MIN_TTL", 100);
        record.put("MAX_TTL", 50);
        record.put("SHORTEST_FLOW_PKT", 1_500);
        record.put("LONGEST_FLOW_PKT", 64);
        record.put("MIN_IP_PKT_LEN", 1_500);
        record.put("MAX_IP_PKT_LEN", 64);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason())
                .isEqualTo("MIN_TTL > MAX_TTL (100 > 50)"
                        + " | SHORTEST_FLOW_PKT > LONGEST_FLOW_PKT (1500 > 64)"
                        + " | MIN_IP_PKT_LEN > MAX_IP_PKT_LEN (1500 > 64)");
    }

    @Test
    void givenSeveralMissingRequiredValues_whenValidated_thenEveryMissingFieldIsReportedOnce() {
        GenericRecord record = validRecord();
        record.put("IPV4_SRC_ADDR", null);
        record.put("L4_DST_PORT", null);
        record.put("IN_BYTES", null);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason())
                .isEqualTo("IPV4_SRC_ADDR is null | L4_DST_PORT is null | IN_BYTES is null");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "IPV4_SRC_ADDR",
            "IPV4_DST_ADDR",
            "L4_SRC_PORT",
            "L4_DST_PORT",
            "IN_BYTES",
            "OUT_BYTES",
            "IN_PKTS",
            "OUT_PKTS",
            "MIN_TTL",
            "MAX_TTL",
            "LONGEST_FLOW_PKT",
            "SHORTEST_FLOW_PKT",
            "MIN_IP_PKT_LEN",
            "MAX_IP_PKT_LEN"
    })
    void givenMissingValidatedField_whenValidated_thenExactFieldIsRejected(String field) {
        GenericRecord record = validRecord();
        record.put(field, null);

        ValidationResult result = validationService.validate(record);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo(field + " is null");
    }

    @Test
    void givenNullRecord_whenValidated_thenRecordIsRejected() {
        ValidationResult result = validationService.validate(null);

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("Record is null");
    }

    @Test
    void givenNegativeL7Protocol_whenValidated_thenProtocolIsRejected() {
        GenericRecord record = validRecord();
        record.put("L7_PROTO", -1.0);

        ValidationResult result = validationService.validate(record);

        assertThat(result.reason()).isEqualTo("L7_PROTO is negative: -1.0");
    }

    @Test
    void givenInfiniteL7Protocol_whenValidated_thenProtocolIsRejected() {
        GenericRecord record = validRecord();
        record.put("L7_PROTO", Double.POSITIVE_INFINITY);

        ValidationResult result = validationService.validate(record);

        assertThat(result.reason()).isEqualTo("L7_PROTO is not finite: Infinity");
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
        record.put("IN_BYTES", 100L);
        record.put("OUT_BYTES", 200L);
        record.put("IN_PKTS", 10L);
        record.put("OUT_PKTS", 20L);
        record.put("FLOW_DURATION_MILLISECONDS", 1000L);
        record.put("DURATION_IN", 100L);
        record.put("DURATION_OUT", 200L);
        record.put("LONGEST_FLOW_PKT", 512L);
        record.put("SHORTEST_FLOW_PKT", 64L);
        record.put("L7_PROTO", 7.5);

        return record;
    }
}
