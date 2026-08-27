package com.nimbo.batch.constants;

public final class ColumnConstants {
    private ColumnConstants() {}

    // ============ Database Column Names ============
    // Common
    public static final String COL_WINDOW_START = "window_start";
    public static final String COL_WINDOW_END = "window_end";
    public static final String COL_BATCH_ID = "batch_id";
    public static final String COL_INGESTED_AT = "ingested_at";
    public static final String COL_IN_BYTES = "in_bytes";
    public static final String COL_OUT_BYTES = "out_bytes";
    public static final String COL_IN_PACKETS = "in_packets";
    public static final String COL_OUT_PACKETS = "out_packets";

    // Traffic Aggregates
    public static final String COL_SRC_IP = "src_ip";
    public static final String COL_RECORD_COUNT = "record_count";

    // Connection Aggregates
    public static final String COL_ID = "id";
    public static final String COL_DST_IP = "dst_ip";
    public static final String COL_SRC_PORT = "src_port";
    public static final String COL_DST_PORT = "dst_port";
    public static final String COL_L4_PROTOCOL = "l4_protocol";

    // Protocol Aggregates
    public static final String COL_L7_PROTOCOL = "l7_protocol";
    public static final String COL_CONNECTION_COUNT = "connection_count";

    // IP Bandwidth Aggregates
    public static final String COL_IP = "ip";
    public static final String COL_INBOUND_BYTES = "inbound_bytes";
    public static final String COL_OUTBOUND_BYTES = "outbound_bytes";
    public static final String COL_INBOUND_PACKETS = "inbound_packets";
    public static final String COL_OUTBOUND_PACKETS = "outbound_packets";

    // ============ Spark Source Column Names (NetFlow Schema) ============
    public static final String NETFLOW_SRC_IP = "IPV4_SRC_ADDR";
    public static final String NETFLOW_DST_IP = "IPV4_DST_ADDR";
    public static final String NETFLOW_SRC_PORT = "L4_SRC_PORT";
    public static final String NETFLOW_DST_PORT = "L4_DST_PORT";
    public static final String NETFLOW_PROTOCOL = "PROTOCOL";
    public static final String NETFLOW_L7_PROTO = "L7_PROTO";
    public static final String NETFLOW_IN_BYTES = "IN_BYTES";
    public static final String NETFLOW_OUT_BYTES = "OUT_BYTES";
    public static final String NETFLOW_IN_PKTS = "IN_PKTS";
    public static final String NETFLOW_OUT_PKTS = "OUT_PKTS";
    public static final String NETFLOW_PROCESSING_TIME = "processing_time";

    // ============ Spark Alias Constants ============
    public static final String SPARK_SRC_IP = "src_ip";
    public static final String SPARK_DST_IP = "dst_ip";
    public static final String SPARK_SRC_PORT = "src_port";
    public static final String SPARK_DST_PORT = "dst_port";
    public static final String SPARK_L4_PROTOCOL = "l4_protocol";
    public static final String SPARK_L7_PROTOCOL = "l7_protocol";
    public static final String SPARK_IN_BYTES = "in_bytes";
    public static final String SPARK_OUT_BYTES = "out_bytes";
    public static final String SPARK_IN_PACKETS = "in_packets";
    public static final String SPARK_OUT_PACKETS = "out_packets";
    public static final String SPARK_RECORD_COUNT = "record_count";
    public static final String SPARK_CONNECTION_COUNT = "connection_count";
    public static final String SPARK_IP = "ip";
    public static final String SPARK_OUTBOUND_BYTES = "outbound_bytes";
    public static final String SPARK_INBOUND_BYTES = "inbound_bytes";
    public static final String SPARK_OUTBOUND_PACKETS = "outbound_packets";
    public static final String SPARK_INBOUND_PACKETS = "inbound_packets";
    public static final String SPARK_ID = "id";
    public static final String SPARK_WINDOW_START = "window_start";
    public static final String SPARK_WINDOW_END = "window_end";
}