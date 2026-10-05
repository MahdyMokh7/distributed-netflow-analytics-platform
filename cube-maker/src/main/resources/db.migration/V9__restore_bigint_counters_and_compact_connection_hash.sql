-- Aggregate counters are sums over potentially large batches and can exceed INTEGER.
ALTER TABLE traffic_aggregates
    ALTER COLUMN in_packets TYPE BIGINT USING in_packets::BIGINT,
    ALTER COLUMN out_packets TYPE BIGINT USING out_packets::BIGINT,
    ALTER COLUMN record_count TYPE BIGINT USING record_count::BIGINT;

ALTER TABLE connection_aggregates
    ALTER COLUMN in_packets TYPE BIGINT USING in_packets::BIGINT,
    ALTER COLUMN out_packets TYPE BIGINT USING out_packets::BIGINT;

ALTER TABLE protocol_usage_aggregates
    ALTER COLUMN in_packets TYPE BIGINT USING in_packets::BIGINT,
    ALTER COLUMN out_packets TYPE BIGINT USING out_packets::BIGINT,
    ALTER COLUMN connection_count TYPE BIGINT USING connection_count::BIGINT;

ALTER TABLE ip_bandwidth_aggregates
    ALTER COLUMN inbound_packets TYPE BIGINT USING inbound_packets::BIGINT,
    ALTER COLUMN outbound_packets TYPE BIGINT USING outbound_packets::BIGINT;

-- Preserve SHA-256 while storing its digest as 32 bytes instead of 64 hex characters.
ALTER TABLE connection_aggregates
    ALTER COLUMN id TYPE BYTEA USING decode(id, 'hex');

COMMENT ON COLUMN connection_aggregates.id IS
    '32-byte SHA-256 digest of (src_ip|dst_ip|window_start)';

ANALYZE traffic_aggregates;
ANALYZE connection_aggregates;
ANALYZE protocol_usage_aggregates;
ANALYZE ip_bandwidth_aggregates;
