package com.nimbo.batch.writer;

public final class ColumnTypeConstants {
    private ColumnTypeConstants() {}

    public static final String STRING = "string";
    public static final String TIMESTAMP = "timestamp";
    public static final String BIGINT = "bigint";
    public static final String INTEGER = "integer";
    public static final String DOUBLE = "double";
    public static final String UUID = "uuid";

    public static final String COL_WINDOW_START = "window_start";
    public static final String COL_WINDOW_END = "window_end";
    public static final String COL_BATCH_ID = "batch_id";
    public static final String COL_INGESTED_AT = "ingested_at";
    public static final String COL_IN_BYTES = "in_bytes";
    public static final String COL_OUT_BYTES = "out_bytes";
    public static final String COL_IN_PACKETS = "in_packets";
    public static final String COL_OUT_PACKETS = "out_packets";

    public static final String COL_RECORD_COUNT = "record_count";
    public static final String COL_ID = "id";
    public static final String COL_SRC_IP = "src_ip";
    public static final String COL_DST_IP = "dst_ip";
    public static final String COL_SRC_PORT = "src_port";
    public static final String COL_DST_PORT = "dst_port";
    public static final String COL_L4_PROTOCOL = "l4_protocol";
    public static final String COL_L7_PROTOCOL = "l7_protocol";

    public static final String COL_CONNECTION_COUNT = "connection_count";

    public static final String COL_IP = "ip";
    public static final String COL_INBOUND_BYTES = "inbound_bytes";
    public static final String COL_OUTBOUND_BYTES = "outbound_bytes";
    public static final String COL_INBOUND_PACKETS = "inbound_packets";
    public static final String COL_OUTBOUND_PACKETS = "outbound_packets";

}