package com.nimbo.ingester.service;

import com.nimbo.ingester.model.ValidationResult;
import com.nimbo.support.NetFlowTestData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class NetflowValidationServiceTest {

    private NetFlowTestData testData;
    private NetflowValidationService validationService;

    @BeforeEach
    void setUp() {
        testData = new NetFlowTestData();
        validationService = new NetflowValidationService();
    }

    @Test
    void givenCompleteNetFlowAtSupportedBoundaries_whenValidated_thenRecordIsAccepted() {
        // Given: a complete flow using valid lower and upper boundaries
        GenericRecord record = testData.validRecord();
        record.put("L4_SRC_PORT", 0);
        record.put("L4_DST_PORT", 65_535);
        record.put("MIN_TTL", 0);
        record.put("MAX_TTL", 255);
        record.put("IN_BYTES", 0L);
        record.put("OUT_PKTS", 0L);

        // When: domain validation checks every constrained field
        ValidationResult result = validationService.validate(record);

        // Then: inclusive boundaries and zero counters remain valid
        assertThat(result).isEqualTo(ValidationResult.ok());
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
        // Given: a complete flow containing one malformed IPv4 address
        GenericRecord record = testData.validRecord();
        record.put(field, value);

        // When: domain validation evaluates the address
        ValidationResult result = validationService.validate(record);

        // Then: validation identifies the exact invalid address
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(expectedError);
    }

    @ParameterizedTest
    @CsvSource({
            "L4_SRC_PORT, -1, 'L4_SRC_PORT out of range: -1'",
            "L4_DST_PORT, 65536, 'L4_DST_PORT out of range: 65536'"
    })
    void givenPortOutsideUnsignedRange_whenValidated_thenPortIsRejected(
            String field, int value, String expectedError) {
        // Given: a complete flow containing one out-of-range port
        GenericRecord record = testData.validRecord();
        record.put(field, value);

        // When: domain validation checks the port range
        ValidationResult result = validationService.validate(record);

        // Then: validation reports the exact port violation
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(expectedError);
    }

    @ParameterizedTest
    @CsvSource({
            "MIN_TTL, -1, 'MIN_TTL out of range: -1'",
            "MAX_TTL, 256, 'MAX_TTL out of range: 256'"
    })
    void givenTtlOutsideUnsignedByteRange_whenValidated_thenTtlIsRejected(
            String field, int value, String expectedError) {
        // Given: a complete flow containing one out-of-range TTL
        GenericRecord record = testData.validRecord();
        record.put(field, value);

        // When: domain validation checks the TTL range
        ValidationResult result = validationService.validate(record);

        // Then: validation reports the exact TTL violation
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(expectedError);
    }

    @ParameterizedTest
    @CsvSource({
            "IN_BYTES, 'IN_BYTES is negative: -1'",
            "OUT_BYTES, 'OUT_BYTES is negative: -1'",
            "IN_PKTS, 'IN_PKTS is negative: -1'",
            "OUT_PKTS, 'OUT_PKTS is negative: -1'"
    })
    void givenNegativeTrafficCounter_whenValidated_thenCounterIsRejected(String field, String expectedError) {
        // Given: a complete flow containing one negative traffic counter
        GenericRecord record = testData.validRecord();
        record.put(field, -1L);

        // When: domain validation checks nonnegative counters
        ValidationResult result = validationService.validate(record);

        // Then: validation reports the exact negative counter
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(expectedError);
    }

    @Test
    void givenNegativePacketLengths_whenValidated_thenEveryPacketLengthIsRejected() {
        // Given: a complete flow containing negative packet-length boundaries
        GenericRecord record = testData.validRecord();
        record.put("LONGEST_FLOW_PKT", -1);
        record.put("SHORTEST_FLOW_PKT", -1);
        record.put("MIN_IP_PKT_LEN", -1);
        record.put("MAX_IP_PKT_LEN", -1);

        // When: domain validation checks nonnegative packet lengths
        ValidationResult result = validationService.validate(record);

        // Then: every negative packet-length field is reported once
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage())
                .isEqualTo("LONGEST_FLOW_PKT is negative: -1"
                        + " | SHORTEST_FLOW_PKT is negative: -1"
                        + " | MIN_IP_PKT_LEN is negative: -1"
                        + " | MAX_IP_PKT_LEN is negative: -1");
    }

    @Test
    void givenInvertedMinimumAndMaximumValues_whenValidated_thenAllOrderingErrorsAreReported() {
        // Given: a flow with inverted TTL and packet-length boundaries
        GenericRecord record = testData.validRecord();
        record.put("MIN_TTL", 100);
        record.put("MAX_TTL", 50);
        record.put("SHORTEST_FLOW_PKT", 1_500);
        record.put("LONGEST_FLOW_PKT", 64);
        record.put("MIN_IP_PKT_LEN", 1_500);
        record.put("MAX_IP_PKT_LEN", 64);

        // When: domain validation checks related minimum and maximum fields
        ValidationResult result = validationService.validate(record);

        // Then: all ordering failures are returned in deterministic validation order
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage())
                .isEqualTo("MIN_TTL > MAX_TTL (100 > 50)"
                        + " | SHORTEST_FLOW_PKT > LONGEST_FLOW_PKT (1500 > 64)"
                        + " | MIN_IP_PKT_LEN > MAX_IP_PKT_LEN (1500 > 64)");
    }

    @Test
    void givenSeveralMissingRequiredValues_whenValidated_thenEveryMissingFieldIsReportedOnce() {
        // Given: a flow with deterministic missing identity and traffic values
        GenericRecord record = testData.validRecord();
        record.put("IPV4_SRC_ADDR", null);
        record.put("L4_DST_PORT", null);
        record.put("IN_BYTES", null);

        // When: domain validation evaluates the incomplete flow
        ValidationResult result = validationService.validate(record);

        // Then: validation returns every missing field in rule order
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage())
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
        // Given: a complete flow with one validated field missing
        GenericRecord record = testData.validRecord();
        record.put(field, null);

        // When: domain validation checks the incomplete flow
        ValidationResult result = validationService.validate(record);

        // Then: validation identifies only the missing field
        assertThat(result.valid()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(field + " is null");
    }
}
