package com.nimbo.realtime.validation;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NetflowValidationService {

    private static final List<String> REQUIRED_NUMERIC_FIELDS =
            List.of(
                    "L4_SRC_PORT",
                    "L4_DST_PORT",
                    "PROTOCOL",
                    "L7_PROTO",
                    "IN_BYTES",
                    "OUT_BYTES",
                    "IN_PKTS",
                    "OUT_PKTS",
                    "FLOW_DURATION_MILLISECONDS",
                    "TCP_FLAGS",
                    "CLIENT_TCP_FLAGS",
                    "SERVER_TCP_FLAGS",
                    "DURATION_IN",
                    "DURATION_OUT",
                    "MIN_TTL",
                    "MAX_TTL",
                    "LONGEST_FLOW_PKT",
                    "SHORTEST_FLOW_PKT",
                    "MIN_IP_PKT_LEN",
                    "MAX_IP_PKT_LEN",
                    "SRC_TO_DST_SECOND_BYTES",
                    "DST_TO_SRC_SECOND_BYTES",
                    "RETRANSMITTED_IN_BYTES",
                    "RETRANSMITTED_IN_PKTS",
                    "RETRANSMITTED_OUT_BYTES",
                    "RETRANSMITTED_OUT_PKTS",
                    "SRC_TO_DST_AVG_THROUGHPUT",
                    "DST_TO_SRC_AVG_THROUGHPUT",
                    "NUM_PKTS_UP_TO_128_BYTES",
                    "NUM_PKTS_128_TO_256_BYTES",
                    "NUM_PKTS_256_TO_512_BYTES",
                    "NUM_PKTS_512_TO_1024_BYTES",
                    "NUM_PKTS_1024_TO_1514_BYTES",
                    "TCP_WIN_MAX_IN",
                    "TCP_WIN_MAX_OUT",
                    "ICMP_TYPE",
                    "ICMP_IPV4_TYPE",
                    "DNS_QUERY_ID",
                    "DNS_QUERY_TYPE",
                    "DNS_TTL_ANSWER",
                    "FTP_COMMAND_RET_CODE"
            );

    public ValidationResult validate(GenericRecord record) {

        if (record == null) {
            return ValidationResult.failure(
                    "NetFlow record is null"
            );
        }

        for (String field : REQUIRED_NUMERIC_FIELDS) {

            Object value = record.get(field);

            if (value == null) {
                return ValidationResult.failure(
                        "Missing required field: " + field
                );
            }

            if (!(value instanceof Number)) {
                return ValidationResult.failure(
                        "Field is not numeric: " + field
                );
            }
        }

        return ValidationResult.success();
    }
}