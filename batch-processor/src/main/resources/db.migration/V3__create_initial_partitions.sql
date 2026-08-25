DO $$
DECLARE
start_date DATE := date_trunc('month', CURRENT_DATE);
    end_date DATE := date_trunc('month', CURRENT_DATE + INTERVAL '1 month');
    month_label TEXT;
BEGIN
    month_label := to_char(start_date, 'YYYY_MM');

    -- Traffic Aggregates
EXECUTE format('
        CREATE TABLE IF NOT EXISTS traffic_aggregates_%s
        PARTITION OF traffic_aggregates
        FOR VALUES FROM (%L) TO (%L)',
               month_label, start_date, end_date
        );

-- Connection Aggregates
EXECUTE format('
        CREATE TABLE IF NOT EXISTS connection_aggregates_%s
        PARTITION OF connection_aggregates
        FOR VALUES FROM (%L) TO (%L)',
               month_label, start_date, end_date
        );

-- Protocol Usage Aggregates
EXECUTE format('
        CREATE TABLE IF NOT EXISTS protocol_usage_aggregates_%s
        PARTITION OF protocol_usage_aggregates
        FOR VALUES FROM (%L) TO (%L)',
               month_label, start_date, end_date
        );

-- IP Bandwidth Aggregates
EXECUTE format('
        CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates_%s
        PARTITION OF ip_bandwidth_aggregates
        FOR VALUES FROM (%L) TO (%L)',
               month_label, start_date, end_date
        );

RAISE NOTICE 'Created partitions for month: %', month_label;
END;
$$;

-- Verify partitions exist
SELECT
    c.relname AS partition_name,
    pg_get_expr(c.relpartbound, c.oid) AS partition_range,
    c.reltuples::bigint AS estimated_rows
FROM
    pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
WHERE
    p.relname = 'traffic_aggregates'
   OR p.relname = 'connection_aggregates'
   OR p.relname = 'protocol_usage_aggregates'
   OR p.relname = 'ip_bandwidth_aggregates'
ORDER BY
    p.relname, c.relname;