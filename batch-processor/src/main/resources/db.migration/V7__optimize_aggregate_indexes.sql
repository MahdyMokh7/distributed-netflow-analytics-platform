-- =====================================================================
-- V7: Optimize aggregate table indexes
-- =====================================================================
-- Remove redundant indexes covered by primary keys or not required by
-- REST API query patterns.
-- =====================================================================


-- =====================================================================
-- Traffic Aggregates
-- PK(src_ip, window_start, batch_id) already supports IP + time queries.
-- =====================================================================

DROP INDEX IF EXISTS idx_traffic_src_ip_window;
DROP INDEX IF EXISTS idx_traffic_batch_id;


-- =====================================================================
-- IP Bandwidth Aggregates
-- PK(ip, window_start, batch_id) already supports IP + time queries.
-- =====================================================================

DROP INDEX IF EXISTS idx_bandwidth_ip_window;
DROP INDEX IF EXISTS idx_bandwidth_batch_id;


-- =====================================================================
-- Protocol Usage Aggregates
-- PK(l7_protocol, window_start, batch_id) already supports protocol queries.
-- =====================================================================

DROP INDEX IF EXISTS idx_protocol_l7_window;
DROP INDEX IF EXISTS idx_protocol_window_start;
DROP INDEX IF EXISTS idx_protocol_batch_id;


-- =====================================================================
-- Connection Aggregates
-- Keep source/destination IP indexes because REST API queries by IP.
-- =====================================================================

DROP INDEX IF EXISTS idx_connections_window_start;
DROP INDEX IF EXISTS idx_connections_batch_id;


-- Refresh planner statistics after index changes.
-- =====================================================================

ANALYZE traffic_aggregates;
ANALYZE connection_aggregates;
ANALYZE protocol_usage_aggregates;
ANALYZE ip_bandwidth_aggregates;