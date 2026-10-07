-- The PostgreSQL bootstrap creates the login/group roles and grants database
-- CONNECT and schema USAGE. Flyway runs as the table owner, so it grants only
-- the table privilege that it is authorized to grant.

GRANT SELECT ON TABLE
    public.traffic_aggregates,
    public.connection_aggregates,
    public.protocol_usage_aggregates,
    public.ip_bandwidth_aggregates
    TO analytics_query_reader;
