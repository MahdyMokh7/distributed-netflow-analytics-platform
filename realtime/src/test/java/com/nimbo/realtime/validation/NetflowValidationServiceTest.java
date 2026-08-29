package com.nimbo.realtime.validation;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NetflowValidationServiceTest {

    private final NetflowValidationService service = new NetflowValidationService();

    @Test
    void acceptsValidRecord() {
        ValidationResult result = service.validate(record(validValues()));

        assertTrue(result.valid());
    }

    @Test
    void rejectsNullRecord() {
        ValidationResult result = service.validate(null);

        assertFalse(result.valid());
        assertTrue(result.reason().contains("Record is null"));
    }

    @Test
    void reportsInvalidIpv4AndOutOfRangePort() {
        Map<String, Object> values = validValues();
        values.put("IPV4_SRC_ADDR", "999.1.1.1");
        values.put("L4_DST_PORT", 70_000);

        ValidationResult result = service.validate(record(values));

        assertFalse(result.valid());
        assertTrue(result.reason().contains("IPV4_SRC_ADDR invalid IPv4"));
        assertTrue(result.reason().contains("L4_DST_PORT out of range"));
    }

    @Test
    void reportsNegativeCountersAndInvalidMinMaxPairs() {
        Map<String, Object> values = validValues();
        values.put("IN_BYTES", -1L);
        values.put("MIN_TTL", 200);
        values.put("MAX_TTL", 100);

        ValidationResult result = service.validate(record(values));

        assertFalse(result.valid());
        assertTrue(result.reason().contains("IN_BYTES is negative"));
        assertTrue(result.reason().contains("MIN_TTL > MAX_TTL"));
    }

    @Test
    void rejectsNonFiniteOrNegativeL7Protocol() {
        Map<String, Object> values = validValues();
        values.put("L7_PROTO", Double.NaN);

        ValidationResult nanResult = service.validate(record(values));
        assertFalse(nanResult.valid());
        assertTrue(nanResult.reason().contains("L7_PROTO is not finite"));

        values.put("L7_PROTO", -0.5d);
        ValidationResult negativeResult = service.validate(record(values));
        assertFalse(negativeResult.valid());
        assertTrue(negativeResult.reason().contains("L7_PROTO is negative"));
    }

    private GenericRecord record(Map<String, Object> values) {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> values.get(invocation.getArgument(0, String.class)));
        return record;
    }

    private Map<String, Object> validValues() {
        Map<String, Object> values = new HashMap<>();
        values.put("IPV4_SRC_ADDR", "10.0.0.1");
        values.put("IPV4_DST_ADDR", "10.0.0.2");
        values.put("L4_SRC_PORT", 12345);
        values.put("L4_DST_PORT", 443);
        values.put("PROTOCOL", 6);
        values.put("MIN_TTL", 32);
        values.put("MAX_TTL", 64);
        values.put("MIN_IP_PKT_LEN", 64);
        values.put("MAX_IP_PKT_LEN", 1500);
        values.put("ICMP_TYPE", 0);
        values.put("ICMP_IPV4_TYPE", 0);
        values.put("DNS_QUERY_ID", 1);
        values.put("DNS_QUERY_TYPE", 1);
        values.put("IN_BYTES", 1000L);
        values.put("OUT_BYTES", 2000L);
        values.put("IN_PKTS", 10L);
        values.put("OUT_PKTS", 20L);
        values.put("FLOW_DURATION_MILLISECONDS", 1000L);
        values.put("DURATION_IN", 500L);
        values.put("DURATION_OUT", 700L);
        values.put("LONGEST_FLOW_PKT", 1500L);
        values.put("SHORTEST_FLOW_PKT", 64L);
        values.put("L7_PROTO", 7.0d);
        return values;
    }
}
