package com.nimbo.realtime.mapper;

import com.nimbo.realtime.model.StarRocksRow;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
public class StarRocksRecordMapper {

    public StarRocksRow map(
            GenericRecord record,
            String topic,
            int partition,
            long offset,
            long kafkaTimestamp
    ) {

        return new StarRocksRow(

                // Kafka metadata
                topic,
                partition,
                offset,
                kafkaTimestamp,

                // NetFlow
                asString(record, "IPV4_SRC_ADDR"),
                asString(record, "IPV4_DST_ADDR"),

                asInt(record, "L4_SRC_PORT"),
                asInt(record, "L4_DST_PORT"),

                asInt(record, "PROTOCOL"),
                asDouble(record, "L7_PROTO"),

                asLong(record, "IN_BYTES"),
                asLong(record, "OUT_BYTES"),

                asLong(record, "IN_PKTS"),
                asLong(record, "OUT_PKTS"),

                asLong(record, "FLOW_DURATION_MILLISECONDS"),

                asInt(record, "TCP_FLAGS"),
                asInt(record, "CLIENT_TCP_FLAGS"),
                asInt(record, "SERVER_TCP_FLAGS"),

                asLong(record, "DURATION_IN"),
                asLong(record, "DURATION_OUT"),

                asInt(record, "MIN_TTL"),
                asInt(record, "MAX_TTL"),

                asInt(record, "LONGEST_FLOW_PKT"),
                asInt(record, "SHORTEST_FLOW_PKT"),

                asInt(record, "MIN_IP_PKT_LEN"),
                asInt(record, "MAX_IP_PKT_LEN"),

                asLong(record, "SRC_TO_DST_SECOND_BYTES"),
                asLong(record, "DST_TO_SRC_SECOND_BYTES"),

                asLong(record, "RETRANSMITTED_IN_BYTES"),
                asLong(record, "RETRANSMITTED_IN_PKTS"),

                asLong(record, "RETRANSMITTED_OUT_BYTES"),
                asLong(record, "RETRANSMITTED_OUT_PKTS"),

                asLong(record, "SRC_TO_DST_AVG_THROUGHPUT"),
                asLong(record, "DST_TO_SRC_AVG_THROUGHPUT"),

                asLong(record, "NUM_PKTS_UP_TO_128_BYTES"),
                asLong(record, "NUM_PKTS_128_TO_256_BYTES"),
                asLong(record, "NUM_PKTS_256_TO_512_BYTES"),
                asLong(record, "NUM_PKTS_512_TO_1024_BYTES"),
                asLong(record, "NUM_PKTS_1024_TO_1514_BYTES"),

                asInt(record, "TCP_WIN_MAX_IN"),
                asInt(record, "TCP_WIN_MAX_OUT"),

                asInt(record, "ICMP_TYPE"),
                asInt(record, "ICMP_IPV4_TYPE"),

                asInt(record, "DNS_QUERY_ID"),
                asInt(record, "DNS_QUERY_TYPE"),

                asLong(record, "DNS_TTL_ANSWER"),

                asInt(record, "FTP_COMMAND_RET_CODE")
        );
    }

    private String asString(
            GenericRecord record,
            String field
    ) {
        Object value = record.get(field);

        return value == null
                ? null
                : value.toString();
    }

    private int asInt(
            GenericRecord record,
            String field
    ) {
        return ((Number) record.get(field))
                .intValue();
    }

    private long asLong(
            GenericRecord record,
            String field
    ) {
        return ((Number) record.get(field))
                .longValue();
    }

    private double asDouble(
            GenericRecord record,
            String field
    ) {
        return ((Number) record.get(field))
                .doubleValue();
    }
}
