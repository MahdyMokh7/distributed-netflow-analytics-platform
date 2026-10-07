CREATE TABLE IF NOT EXISTS traffic_aggregates (
    src_ip INET NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    record_count BIGINT NOT NULL CHECK (record_count >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (src_ip, window_start, batch_id)
);

CREATE TABLE IF NOT EXISTS connection_aggregates (
    id BYTEA NOT NULL,
    src_ip INET NOT NULL,
    dst_ip INET NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, window_start, batch_id)
);

CREATE TABLE IF NOT EXISTS protocol_usage_aggregates (
    l7_protocol DOUBLE PRECISION NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    connection_count BIGINT NOT NULL CHECK (connection_count >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (l7_protocol, window_start, batch_id)
);

CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates (
    ip INET NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    inbound_bytes BIGINT NOT NULL CHECK (inbound_bytes >= 0),
    outbound_bytes BIGINT NOT NULL CHECK (outbound_bytes >= 0),
    inbound_packets BIGINT NOT NULL CHECK (inbound_packets >= 0),
    outbound_packets BIGINT NOT NULL CHECK (outbound_packets >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ip, window_start, batch_id)
);
