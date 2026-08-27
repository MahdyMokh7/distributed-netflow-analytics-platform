## Run Batch-Processor  [Complete Flow: Build → Transfer → Run on Node3]

### 1. Build JAR (Local Machine)

```bash
cd ~/Desktop/nimbo/Nimbyte/batch-processor

# Clean and package with tests skipped
mvn clean package -DskipTests
```

JAR location: `target/batch-processor-1.0.0-SNAPSHOT.jar`

---

### 2. Transfer JAR to Node3 (Local Machine)

```bash
scp target/batch-processor.jar team1@10.10.61.53:~/
```

---

### 3. Run on Node3 (Server)

```bash
ssh team1@10.10.61.53

# Run with prod profile
java -jar batch-processor.jar --spring.profiles.active=prod

java -jar batch-processor-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=prod \
  --batch.schedule="0 */1 * * * ?"
  
  
kubectl exec -it -n team1 spark-spark-master-7b9bb87b78-ntjn4 -- \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.nimbo.batch.BatchProcessorApplication \
  /tmp/batch-processor.jar \
  --spring.profiles.active=dev
  
  
docker run -it --rm \
  --network host \
  -v /home/team1/batch-processor-1.0.0-SNAPSHOT.jar:/app/batch-processor.jar \
  zoraq/spark:4.1.1 \
  spark-submit \
  --master spark://10.10.61.71:7077 \
  --deploy-mode client \
  --class com.nimbo.batch.BatchProcessorApplication \
  /app/batch-processor.jar \
  --spring.profiles.active=prod
  
  
mvn clean spring-boot:run -Dspring-boot.run.profiles=dev


docker run -d   --name nimbo-postgres   -e POSTGRES_USER=nimbo_user   -e POSTGRES_PASSWORD=123456   -e POSTGRES_DB=nimbo_db   -p 5432:5432   postgres:16.2
docker exec -it nimbo-postgres psql -U nimbo_user -d nimbo_db

```

## Monitor

- **Spark UI:** `http://10.10.61.71/`
- **Spark History UI:** `http://10.10.61.72/`
- **Logs:** `tail -f batch-processor.log`

spark-ui ip : http://10.10.61.71/
spark-ui history ip : http://10.10.61.72/



## How to Edit on PostgreSQL DB

### Clean path from your local system to `nimbo_db` as `nimbo_batch`

Use **Node1** because your `kubectl` access works there:

```bash
# 1. From your local system
ssh team1@10.10.61.51

# 2. Switch to the Kubernetes user
sudo su kubernetes

# 3. Go to the kubernetes user's home (optional)
cd /home/kubernetes

# 4. Enter the PostgreSQL master pod
kubectl exec -it -n team1 postgresql-postgresql-0 -- \
  psql -U postgres -d nimbo_db
```

You should then see:

```text
nimbo_db=>
```

Then, inside `psql`, you can create tables:

```sql
CREATE TABLE example (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

\dt
```

Also, **`psql -h ...` is a Linux command**, so don't type it inside the `nimbo_db=#` prompt. Inside `psql`, use SQL commands such as `CREATE TABLE`, or `\dt`, `\q`, etc.

## PostgreSQL / `psql` Quick Guide

### Connect

```bash
psql -h HOST -p 5432 -U USER -d DATABASE
```

Example:

```bash
psql -h 10.10.61.70 -p 5432 -U nimbo_user -d nimbo_db
```

### Tables

```sql
\dt                         -- list tables
\d table_name               -- describe table
\d+ table_name              -- detailed description
```

### Create table

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Insert

```sql
INSERT INTO users (name)
VALUES ('Mahdi');
```

### Select

```sql
SELECT * FROM users;
SELECT id, name FROM users;
SELECT * FROM users WHERE id = 1;
```

### Update

```sql
UPDATE users
SET name = 'Ali'
WHERE id = 1;
```

**Always use `WHERE` unless you intentionally want to update every row.**

### Delete

```sql
DELETE FROM users
WHERE id = 1;
```

### Alter table

```sql
ALTER TABLE users ADD COLUMN email VARCHAR(255);

ALTER TABLE users DROP COLUMN email;

ALTER TABLE users RENAME COLUMN name TO full_name;

ALTER TABLE users RENAME TO customers;
```

### Drop table

```sql
DROP TABLE users;
```

Safer:

```sql
DROP TABLE IF EXISTS users;
```

### Databases

```sql
\l                          -- list databases
\c database_name            -- connect to another database
```

### Users / roles

```sql
\du                         -- list users/roles
```

Create:

```sql
CREATE USER myuser WITH PASSWORD 'password';
```

Change password:

```sql
ALTER USER myuser WITH PASSWORD 'new_password';
```

### Useful information

```sql
SELECT current_user;        -- current user
SELECT current_database();  -- current database
SELECT version();           -- PostgreSQL version
```

### Quit PostgreSQL

```sql
\q
```

### Clear terminal

```sql
\! clear
```

### Transactions — very useful

```sql
BEGIN;

UPDATE users SET name = 'Test' WHERE id = 1;

ROLLBACK;       -- undo changes
```

Or:

```sql
BEGIN;

UPDATE users SET name = 'Test' WHERE id = 1;

COMMIT;         -- permanently apply
```

### Important distinction

Commands starting with `\` are **psql commands**:

```text
\dt
\du
\l
\q
```

SQL commands are things like:

```sql
SELECT ...
CREATE TABLE ...
INSERT ...
UPDATE ...
DELETE ...
ALTER TABLE ...
```

And **don't type `psql ...` while already inside `psql`**. `psql` is the Linux command used to enter PostgreSQL in the first place.




## V_MIGRATION

-- 1. Protocol Dimension Table (Static Reference Data)
-- =====================================================================
CREATE TABLE IF NOT EXISTS protocol_dim (
id INTEGER PRIMARY KEY,
name VARCHAR(50) NOT NULL,
description VARCHAR(200),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE protocol_dim IS 'Static reference table for network protocols (L4 and L7)';
COMMENT ON COLUMN protocol_dim.id IS 'Protocol code (e.g., 6=TCP, 17=UDP)';
COMMENT ON COLUMN protocol_dim.name IS 'Protocol name (e.g., TCP, UDP)';
COMMENT ON COLUMN protocol_dim.description IS 'Full protocol description';

-- 2. Traffic Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS traffic_aggregates (
id BIGSERIAL,
src_ip VARCHAR(45) NOT NULL,
window_start TIMESTAMP NOT NULL,
window_end TIMESTAMP NOT NULL,
in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
record_count BIGINT NOT NULL CHECK (record_count >= 0),
batch_id UUID NOT NULL,
ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (src_ip, window_start, batch_id)
) PARTITION BY RANGE (window_start);

COMMENT ON TABLE traffic_aggregates IS 'IP bandwidth consumption by source IP per time window';
COMMENT ON COLUMN traffic_aggregates.src_ip IS 'Source IP address (IPv4 or IPv6)';
COMMENT ON COLUMN traffic_aggregates.window_start IS 'Start of the aggregation window';
COMMENT ON COLUMN traffic_aggregates.window_end IS 'End of the aggregation window';
COMMENT ON COLUMN traffic_aggregates.in_bytes IS 'Inbound bytes from this source IP';
COMMENT ON COLUMN traffic_aggregates.out_bytes IS 'Outbound bytes from this source IP';
COMMENT ON COLUMN traffic_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_traffic_src_ip_window ON traffic_aggregates(src_ip, window_start);
CREATE INDEX idx_traffic_window_start ON traffic_aggregates(window_start);
CREATE INDEX idx_traffic_batch_id ON traffic_aggregates(batch_id);

-- 3. Connection Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS connection_aggregates (
id VARCHAR(64),
src_ip VARCHAR(45) NOT NULL,
dst_ip VARCHAR(45) NOT NULL,
src_port INTEGER NOT NULL,
dst_port INTEGER NOT NULL,
l4_protocol INTEGER NOT NULL REFERENCES protocol_dim(id),
window_start TIMESTAMP NOT NULL,
window_end TIMESTAMP NOT NULL,
in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
batch_id UUID NOT NULL,
ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (id, window_start, batch_id)
) PARTITION BY RANGE (window_start);

COMMENT ON TABLE connection_aggregates IS 'IP connection details (5-tuple) per time window';
COMMENT ON COLUMN connection_aggregates.id IS 'SHA-256 hash of (src_ip|dst_ip|src_port|dst_port|l4_protocol|window_start)';
COMMENT ON COLUMN connection_aggregates.l4_protocol IS 'Layer 4 protocol code (references protocol_dim)';
COMMENT ON COLUMN connection_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_connections_src_ip_window ON connection_aggregates(src_ip, window_start);
CREATE INDEX idx_connections_dst_ip_window ON connection_aggregates(dst_ip, window_start);
CREATE INDEX idx_connections_window_start ON connection_aggregates(window_start);
CREATE INDEX idx_connections_batch_id ON connection_aggregates(batch_id);

-- 4. Protocol Usage Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS protocol_usage_aggregates (
id BIGSERIAL,
l7_protocol DOUBLE PRECISION NOT NULL,
window_start TIMESTAMP NOT NULL,
window_end TIMESTAMP NOT NULL,
in_bytes BIGINT NOT NULL CHECK (in_bytes >= 0),
out_bytes BIGINT NOT NULL CHECK (out_bytes >= 0),
in_packets BIGINT NOT NULL CHECK (in_packets >= 0),
out_packets BIGINT NOT NULL CHECK (out_packets >= 0),
connection_count BIGINT NOT NULL CHECK (connection_count >= 0),
batch_id UUID NOT NULL,
ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (l7_protocol, window_start, batch_id)
) PARTITION BY RANGE (window_start);

COMMENT ON TABLE protocol_usage_aggregates IS 'Protocol usage statistics (L7 application layer) per time window';
COMMENT ON COLUMN protocol_usage_aggregates.l7_protocol IS 'Layer 7 protocol identifier (e.g., 6.0=TCP, 17.0=UDP, 53.0=DNS)';
COMMENT ON COLUMN protocol_usage_aggregates.connection_count IS 'Number of connections using this protocol';
COMMENT ON COLUMN protocol_usage_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_protocol_l7_window ON protocol_usage_aggregates(l7_protocol, window_start);
CREATE INDEX idx_protocol_window_start ON protocol_usage_aggregates(window_start);
CREATE INDEX idx_protocol_batch_id ON protocol_usage_aggregates(batch_id);

-- 5. IP Bandwidth Aggregates Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS ip_bandwidth_aggregates (
id BIGSERIAL,
ip VARCHAR(45) NOT NULL,
window_start TIMESTAMP NOT NULL,
window_end TIMESTAMP NOT NULL,
inbound_bytes BIGINT NOT NULL CHECK (inbound_bytes >= 0),
outbound_bytes BIGINT NOT NULL CHECK (outbound_bytes >= 0),
inbound_packets BIGINT NOT NULL CHECK (inbound_packets >= 0),
outbound_packets BIGINT NOT NULL CHECK (outbound_packets >= 0),
batch_id UUID NOT NULL,
ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
UNIQUE(ip, window_start),
PRIMARY KEY (ip, window_start, batch_id)
) PARTITION BY RANGE (window_start);

COMMENT ON TABLE ip_bandwidth_aggregates IS 'Top IPs by bandwidth consumption (unified source+dst perspective)';
COMMENT ON COLUMN ip_bandwidth_aggregates.ip IS 'IP address (appears as either source or destination)';
COMMENT ON COLUMN ip_bandwidth_aggregates.inbound_bytes IS 'Bytes received by this IP';
COMMENT ON COLUMN ip_bandwidth_aggregates.outbound_bytes IS 'Bytes sent by this IP';
COMMENT ON COLUMN ip_bandwidth_aggregates.batch_id IS 'UUID identifying the batch run that produced this row';

CREATE INDEX idx_bandwidth_ip_window ON ip_bandwidth_aggregates(ip, window_start);
CREATE INDEX idx_bandwidth_window_start ON ip_bandwidth_aggregates(window_start);
CREATE INDEX idx_bandwidth_batch_id ON ip_bandwidth_aggregates(batch_id);

-- =====================================================================
-- Nimbo Batch Processor - Seed Protocol Dimension Data (V2)
-- =====================================================================

INSERT INTO protocol_dim (id, name, description) VALUES
(1, 'ICMP', 'Internet Control Message Protocol'),
(2, 'IGMP', 'Internet Group Management Protocol'),
(6, 'TCP', 'Transmission Control Protocol'),
(17, 'UDP', 'User Datagram Protocol'),
(50, 'ESP', 'Encapsulating Security Payload'),
(51, 'AH', 'Authentication Header'),
(89, 'OSPF', 'Open Shortest Path First'),
(132, 'SCTP', 'Stream Control Transmission Protocol')
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE protocol_dim IS 'Static reference table for network protocols (L4 and L7)';

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

-- Batch Processor State Table
-- Tracks which Parquet files have been successfully processed for incremental reading
CREATE TABLE IF NOT EXISTS batch_processor_state (
id BIGSERIAL PRIMARY KEY,
file_name VARCHAR(255) NOT NULL UNIQUE,
file_timestamp TIMESTAMP NOT NULL,
batch_id UUID NOT NULL,
processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE batch_processor_state IS 'Tracks processed Parquet files for incremental reading (Topic 4)';
COMMENT ON COLUMN batch_processor_state.file_name IS 'Name of the Parquet file processed (e.g., netflow-p0-2026-08-27T10-30-00Z-abc.parquet)';
COMMENT ON COLUMN batch_processor_state.file_timestamp IS 'Timestamp extracted from filename (UTC)';
COMMENT ON COLUMN batch_processor_state.batch_id IS 'UUID of the batch run that processed this file';
COMMENT ON COLUMN batch_processor_state.processed_at IS 'When this file was marked as processed';

CREATE INDEX idx_file_timestamp ON batch_processor_state(file_timestamp);
CREATE INDEX idx_batch_id ON batch_processor_state(batch_id);
CREATE INDEX idx_file_name ON batch_processor_state(file_name);