CREATE UNLOGGED TABLE traffic_aggregates_staging (
    src_ip TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL,
    in_packets BIGINT NOT NULL,
    out_packets BIGINT NOT NULL,
    record_count BIGINT NOT NULL,
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    spark_partition_id INTEGER NOT NULL,
    task_attempt_id INTEGER NOT NULL
);

CREATE INDEX idx_traffic_staging_batch_partition
    ON traffic_aggregates_staging(batch_id, spark_partition_id);

CREATE UNLOGGED TABLE connection_aggregates_staging (
    id BYTEA NOT NULL,
    src_ip TEXT NOT NULL,
    dst_ip TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL,
    in_packets BIGINT NOT NULL,
    out_packets BIGINT NOT NULL,
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    spark_partition_id INTEGER NOT NULL,
    task_attempt_id INTEGER NOT NULL
);

CREATE INDEX idx_connection_staging_batch_partition
    ON connection_aggregates_staging(batch_id, spark_partition_id);

CREATE UNLOGGED TABLE ip_bandwidth_aggregates_staging (
    ip TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    inbound_bytes BIGINT NOT NULL,
    outbound_bytes BIGINT NOT NULL,
    inbound_packets BIGINT NOT NULL,
    outbound_packets BIGINT NOT NULL,
    batch_id UUID NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    spark_partition_id INTEGER NOT NULL,
    task_attempt_id INTEGER NOT NULL
);

CREATE INDEX idx_bandwidth_staging_batch_partition
    ON ip_bandwidth_aggregates_staging(batch_id, spark_partition_id);