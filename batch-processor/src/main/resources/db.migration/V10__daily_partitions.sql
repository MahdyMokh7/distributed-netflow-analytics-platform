-- Convert the empty bootstrap partition set from weekly to daily partitions.
-- This migration intentionally refuses to discard data on an existing installation.
DO $$
DECLARE
    parent_name TEXT;
    child_name TEXT;
    has_rows BOOLEAN;
BEGIN
    FOREACH parent_name IN ARRAY ARRAY[
        'traffic_aggregates',
        'connection_aggregates',
        'protocol_usage_aggregates',
        'ip_bandwidth_aggregates'
    ] LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I LIMIT 1)', parent_name)
            INTO STRICT has_rows;
        IF has_rows THEN
            RAISE EXCEPTION
                'Cannot replace non-empty weekly partitions for %. Drain or migrate the data first.',
                parent_name;
        END IF;

        FOR child_name IN
            SELECT child.relname
            FROM pg_inherits inheritance
            JOIN pg_class parent ON parent.oid = inheritance.inhparent
            JOIN pg_class child ON child.oid = inheritance.inhrelid
            WHERE parent.relname = parent_name
        LOOP
            EXECUTE format('DROP TABLE %I', child_name);
        END LOOP;
    END LOOP;
END $$;
