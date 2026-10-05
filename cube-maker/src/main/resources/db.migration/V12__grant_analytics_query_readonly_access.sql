-- =====================================================================
-- Grant minimum read-only access for analytics-query generated SQL
-- =====================================================================

DO $$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO analytics_query_reader',
        current_database());
END
$$;

GRANT USAGE ON SCHEMA public
    TO analytics_query_reader;

GRANT SELECT ON TABLE
    public.traffic_aggregates,
    public.connection_aggregates,
    public.protocol_usage_aggregates,
    public.ip_bandwidth_aggregates
    TO analytics_query_reader;
