DO $$
DECLARE
current_week DATE;
    end_week DATE;
    table_name TEXT;
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
    week_label TEXT;
BEGIN
    -- Start from 4 weeks ago (1 month back)
    current_week := DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '4 weeks';
    end_week := current_week + INTERVAL '12 weeks';

    WHILE current_week < end_week LOOP
        week_label := TO_CHAR(current_week, 'YYYY_MM_DD');
        start_date := TO_CHAR(current_week, 'YYYY-MM-DD');
        end_date := TO_CHAR(current_week + INTERVAL '1 week', 'YYYY-MM-DD');

-- Traffic Aggregates
EXECUTE format('
            CREATE TABLE IF NOT EXISTS traffic_aggregates_%s
            PARTITION OF traffic_aggregates
            FOR VALUES FROM (%L) TO (%L)',
               week_label, start_date, end_date
        );

-- Connection Aggregates
EXECUTE format('
            CREATE TABLE IF NOT EXISTS connection_aggregates_%s
            PARTITION OF connection_aggregates
            FOR VALUES FROM (%L) TO (%L)',
               week_label, start_date, end_date
        );

-- Protocol Usage Aggregates
EXECUTE format('
            CREATE TABLE IF NOT EXISTS protocol_usage_aggregates_%s
            PARTITION OF protocol_usage_aggregates
            FOR VALUES FROM (%L) TO (%L)',
               week_label, start_date, end_date
        );

-- IP Bandwidth Aggregates
EXECUTE format('
            CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates_%s
            PARTITION OF ip_bandwidth_aggregates
            FOR VALUES FROM (%L) TO (%L)',
               week_label, start_date, end_date
        );

RAISE NOTICE 'Created partitions for week starting: %', start_date;

        current_week := current_week + INTERVAL '1 week';
END LOOP;
END $$;

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