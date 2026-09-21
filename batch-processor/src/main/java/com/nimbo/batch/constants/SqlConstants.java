package com.nimbo.batch.constants;

public final class SqlConstants {
    private SqlConstants() {}

    // ============ Traffic Aggregates ============
    public static final String TRAFFIC_AGGREGATES_INSERT =
            "INSERT INTO traffic_aggregates (src_ip, window_start, window_end, " +
                    "in_bytes, out_bytes, in_packets, out_packets, record_count, " +
                    "batch_id, ingested_at) " +
                    "VALUES (CAST(:srcIp AS inet), :windowStart, :windowEnd, :inBytes, :outBytes, " +
                    ":inPackets, :outPackets, :recordCount, :batchId, :ingestedAt)";

    public static final String TRAFFIC_AGGREGATES_CONFLICT =
            "ON CONFLICT (src_ip, window_start, batch_id) DO UPDATE SET " +
                    "window_end = EXCLUDED.window_end, " +
                    "in_bytes = EXCLUDED.in_bytes, " +
                    "out_bytes = EXCLUDED.out_bytes, " +
                    "in_packets = EXCLUDED.in_packets, " +
                    "out_packets = EXCLUDED.out_packets, " +
                    "record_count = EXCLUDED.record_count, " +
                    "batch_id = EXCLUDED.batch_id, " +
                    "ingested_at = EXCLUDED.ingested_at";

    public static final String TRAFFIC_AGGREGATES_UPSERT =
            TRAFFIC_AGGREGATES_INSERT + " " + TRAFFIC_AGGREGATES_CONFLICT;

    // ============ Connection Aggregates ============
    public static final String CONNECTION_AGGREGATES_INSERT =
            "INSERT INTO connection_aggregates (id, src_ip, dst_ip, window_start, window_end, " +
                    "in_bytes, out_bytes, in_packets, out_packets, " +
                    "batch_id, ingested_at) " +
                    "VALUES (:id, CAST(:srcIp AS inet), CAST(:dstIp AS inet), :windowStart, :windowEnd, :inBytes, :outBytes, " +
                    ":inPackets, :outPackets, :batchId, :ingestedAt)";

    public static final String CONNECTION_AGGREGATES_CONFLICT =
            "ON CONFLICT (id, window_start, batch_id) DO UPDATE SET " +
                    "src_ip = EXCLUDED.src_ip, " +
                    "dst_ip = EXCLUDED.dst_ip, " +
                    "window_start = EXCLUDED.window_start, " +
                    "window_end = EXCLUDED.window_end, " +
                    "in_bytes = EXCLUDED.in_bytes, " +
                    "out_bytes = EXCLUDED.out_bytes, " +
                    "in_packets = EXCLUDED.in_packets, " +
                    "out_packets = EXCLUDED.out_packets, " +
                    "batch_id = EXCLUDED.batch_id, " +
                    "ingested_at = EXCLUDED.ingested_at";

    public static final String CONNECTION_AGGREGATES_UPSERT =
            CONNECTION_AGGREGATES_INSERT + " " + CONNECTION_AGGREGATES_CONFLICT;

    // ============ Protocol Usage Aggregates ============
    public static final String PROTOCOL_USAGE_AGGREGATES_INSERT =
            "INSERT INTO protocol_usage_aggregates (l7_protocol, window_start, window_end, " +
                    "in_bytes, out_bytes, in_packets, out_packets, connection_count, " +
                    "batch_id, ingested_at) " +
                    "VALUES (:l7Protocol, :windowStart, :windowEnd, :inBytes, :outBytes, " +
                    ":inPackets, :outPackets, :connectionCount, :batchId, :ingestedAt)";

    public static final String PROTOCOL_USAGE_AGGREGATES_CONFLICT =
            "ON CONFLICT (l7_protocol, window_start, batch_id) DO UPDATE SET " +
                    "window_end = EXCLUDED.window_end, " +
                    "in_bytes = EXCLUDED.in_bytes, " +
                    "out_bytes = EXCLUDED.out_bytes, " +
                    "in_packets = EXCLUDED.in_packets, " +
                    "out_packets = EXCLUDED.out_packets, " +
                    "connection_count = EXCLUDED.connection_count, " +
                    "batch_id = EXCLUDED.batch_id, " +
                    "ingested_at = EXCLUDED.ingested_at";

    public static final String PROTOCOL_USAGE_AGGREGATES_UPSERT =
            PROTOCOL_USAGE_AGGREGATES_INSERT + " " + PROTOCOL_USAGE_AGGREGATES_CONFLICT;

    // ============ IP Bandwidth Aggregates ============
    public static final String IP_BANDWIDTH_AGGREGATES_INSERT =
            "INSERT INTO ip_bandwidth_aggregates (ip, window_start, window_end, " +
                    "inbound_bytes, outbound_bytes, inbound_packets, outbound_packets, " +
                    "batch_id, ingested_at) " +
                    "VALUES (CAST(:ip AS inet), :windowStart, :windowEnd, :inboundBytes, :outboundBytes, " +
                    ":inboundPackets, :outboundPackets, :batchId, :ingestedAt)";

    public static final String IP_BANDWIDTH_AGGREGATES_CONFLICT =
            "ON CONFLICT (ip, window_start, batch_id) DO UPDATE SET " +
                    "window_end = EXCLUDED.window_end, " +
                    "inbound_bytes = EXCLUDED.inbound_bytes, " +
                    "outbound_bytes = EXCLUDED.outbound_bytes, " +
                    "inbound_packets = EXCLUDED.inbound_packets, " +
                    "outbound_packets = EXCLUDED.outbound_packets, " +
                    "batch_id = EXCLUDED.batch_id, " +
                    "ingested_at = EXCLUDED.ingested_at";

    public static final String IP_BANDWIDTH_AGGREGATES_UPSERT =
            IP_BANDWIDTH_AGGREGATES_INSERT + " " + IP_BANDWIDTH_AGGREGATES_CONFLICT;
}
