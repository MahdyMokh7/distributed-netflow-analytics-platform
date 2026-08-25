-- 1. Protocol Dimension Table (Static Reference Data)
-- =====================================================================
CREATE TABLE IF NOT EXISTS protocol_dim (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

COMMENT ON TABLE protocol_dim IS 'Static reference table for network protocols (L4 and L7)';
COMMENT ON COLUMN protocol_dim.id IS 'Protocol code (e.g., 6=TCP, 17=UDP)';
COMMENT ON COLUMN protocol_dim.name IS 'Protocol name (e.g., TCP, UDP)';
COMMENT ON COLUMN protocol_dim.description IS 'Full protocol description';

-- 2. Traffic Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS traffic_aggregates (
    id BIGSERIAL,
    src_ip VARCHAR(45) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    record_count BIGINT NOT NULL CHECK (record_count >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(src_ip, window_start),
    PRIMARY KEY (id, window_start)
    ) PARTITION BY RANGE (window_start);

COMMENT ON TABLE traffic_aggregates IS 'IP bandwidth consumption by source IP per time window';
COMMENT ON COLUMN traffic_aggregates.src_ip IS 'Source IP address (IPv4 or IPv6)';
COMMENT ON COLUMN traffic_aggregates.window_start IS 'Start of the aggregation window';
COMMENT ON COLUMN traffic_aggregates.window_end IS 'End of the aggregation window';
COMMENT ON COLUMN traffic_aggregates.in_bytes IS 'Inbound bytes from this source IP';
COMMENT ON COLUMN traffic_aggregates.out_bytes IS 'Outbound bytes from this source IP';
COMMENT ON COLUMN traffic_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_traffic_src_ip_window ON traffic_aggregates(src_ip, window_start);
CREATE INDEX idx_traffic_window_start ON traffic_aggregates(window_start);
CREATE INDEX idx_traffic_batch_id ON traffic_aggregates(batch_id);

-- 3. Connection Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS connection_aggregates (
    id VARCHAR(64),
    src_ip VARCHAR(45) NOT NULL,
    dst_ip VARCHAR(45) NOT NULL,
    src_port INTEGER NOT NULL,
    dst_port INTEGER NOT NULL,
    l4_protocol INTEGER NOT NULL REFERENCES protocol_dim(id),
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, window_start)
    ) PARTITION BY RANGE (window_start);

COMMENT ON TABLE connection_aggregates IS 'IP connection details (5-tuple) per time window';
COMMENT ON COLUMN connection_aggregates.id IS 'SHA-256 hash of (src_ip|dst_ip|src_port|dst_port|l4_protocol|window_start)';
COMMENT ON COLUMN connection_aggregates.l4_protocol IS 'Layer 4 protocol code (references protocol_dim)';
COMMENT ON COLUMN connection_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_connections_src_ip_window ON connection_aggregates(src_ip, window_start);
CREATE INDEX idx_connections_dst_ip_window ON connection_aggregates(dst_ip, window_start);
CREATE INDEX idx_connections_window_start ON connection_aggregates(window_start);
CREATE INDEX idx_connections_batch_id ON connection_aggregates(batch_id);

-- 4. Protocol Usage Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS protocol_usage_aggregates (
    id BIGSERIAL,
    l7_protocol DOUBLE PRECISION NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
    out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
    in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
    out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
    connection_count BIGINT NOT NULL CHECK (connection_count >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(l7_protocol, window_start),
    PRIMARY KEY (id, window_start)
    ) PARTITION BY RANGE (window_start);

COMMENT ON TABLE protocol_usage_aggregates IS 'Protocol usage statistics (L7 application layer) per time window';
COMMENT ON COLUMN protocol_usage_aggregates.l7_protocol IS 'Layer 7 protocol identifier (e.g., 6.0=TCP, 17.0=UDP, 53.0=DNS)';
COMMENT ON COLUMN protocol_usage_aggregates.connection_count IS 'Number of connections using this protocol';
COMMENT ON COLUMN protocol_usage_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_protocol_l7_window ON protocol_usage_aggregates(l7_protocol, window_start);
CREATE INDEX idx_protocol_window_start ON protocol_usage_aggregates(window_start);
CREATE INDEX idx_protocol_batch_id ON protocol_usage_aggregates(batch_id);

-- 5. IP Bandwidth Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates (
    id BIGSERIAL,
    ip VARCHAR(45) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    inbound_bytes BIGINT NOT NULL CHECK (inbound_bytes >= 0),
    outbound_bytes BIGINT NOT NULL CHECK (outbound_bytes >= 0),
    inbound_packets BIGINT NOT NULL CHECK (inbound_packets >= 0),
    outbound_packets BIGINT NOT NULL CHECK (outbound_packets >= 0),
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ip, window_start),
    PRIMARY KEY (id, window_start)
    ) PARTITION BY RANGE (window_start);

COMMENT ON TABLE ip_bandwidth_aggregates IS 'Top IPs by bandwidth consumption (unified source+dst perspective)';
COMMENT ON COLUMN ip_bandwidth_aggregates.ip IS 'IP address (appears as either source or destination)';
COMMENT ON COLUMN ip_bandwidth_aggregates.inbound_bytes IS 'Bytes received by this IP';
COMMENT ON COLUMN ip_bandwidth_aggregates.outbound_bytes IS 'Bytes sent by this IP';
COMMENT ON COLUMN ip_bandwidth_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_bandwidth_ip_window ON ip_bandwidth_aggregates(ip, window_start);
CREATE INDEX idx_bandwidth_window_start ON ip_bandwidth_aggregates(window_start);
CREATE INDEX idx_bandwidth_batch_id ON ip_bandwidth_aggregates(batch_id);