-- =====================================================================
-- V8: Storage optimization for aggregate tables
-- =====================================================================
-- Changes:
-- 1. Convert IP columns VARCHAR(45) -> INET
-- 2. Reduce packet/count columns BIGINT -> INTEGER
-- 3. Remove unused surrogate BIGINT id columns
--
-- These changes reduce row size and index size while preserving
-- logical keys used for exactly-once UPSERT semantics.
-- =====================================================================


-- =====================================================================
-- 1. Convert IP columns VARCHAR(45) -> INET
-- =====================================================================

ALTER TABLE traffic_aggregates
ALTER COLUMN src_ip TYPE INET
    USING src_ip::INET;


ALTER TABLE connection_aggregates
ALTER COLUMN src_ip TYPE INET
    USING src_ip::INET;

ALTER TABLE connection_aggregates
ALTER COLUMN dst_ip TYPE INET
    USING dst_ip::INET;


ALTER TABLE ip_bandwidth_aggregates
ALTER COLUMN ip TYPE INET
    USING ip::INET;



-- =====================================================================
-- 2. Reduce packet/count columns BIGINT -> INTEGER
-- =====================================================================

ALTER TABLE traffic_aggregates
ALTER COLUMN in_packets TYPE INTEGER
    USING in_packets::INTEGER,
    ALTER COLUMN out_packets TYPE INTEGER
    USING out_packets::INTEGER,
    ALTER COLUMN record_count TYPE INTEGER
    USING record_count::INTEGER;


ALTER TABLE connection_aggregates
ALTER COLUMN in_packets TYPE INTEGER
    USING in_packets::INTEGER,
    ALTER COLUMN out_packets TYPE INTEGER
    USING out_packets::INTEGER;


ALTER TABLE protocol_usage_aggregates
ALTER COLUMN in_packets TYPE INTEGER
    USING in_packets::INTEGER,
    ALTER COLUMN out_packets TYPE INTEGER
    USING out_packets::INTEGER,
    ALTER COLUMN connection_count TYPE INTEGER
    USING connection_count::INTEGER;


ALTER TABLE ip_bandwidth_aggregates
ALTER COLUMN inbound_packets TYPE INTEGER
    USING inbound_packets::INTEGER,
    ALTER COLUMN outbound_packets TYPE INTEGER
    USING outbound_packets::INTEGER;



-- =====================================================================
-- 3. Remove unused surrogate id columns
-- =====================================================================
-- Logical identity is already represented by the composite primary keys.
-- These generated IDs are not used by REST API or UPSERT logic.


ALTER TABLE traffic_aggregates
DROP COLUMN id;


ALTER TABLE protocol_usage_aggregates
DROP COLUMN id;


ALTER TABLE ip_bandwidth_aggregates
DROP COLUMN id;



-- Refresh planner statistics
-- =====================================================================

ANALYZE traffic_aggregates;
ANALYZE connection_aggregates;
ANALYZE protocol_usage_aggregates;
ANALYZE ip_bandwidth_aggregates;