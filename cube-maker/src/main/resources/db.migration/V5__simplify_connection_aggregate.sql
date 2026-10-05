ALTER TABLE connection_aggregates
    DROP COLUMN src_port,
    DROP COLUMN dst_port,
    DROP COLUMN l4_protocol;
