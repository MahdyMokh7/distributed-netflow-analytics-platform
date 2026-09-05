# Week 3 Implementation Plan: Cube Maker - PostgreSQL Integration (Updated)

## 1. Database Design & Schema

### 1.1 Table Schema Definition
- **1.1.1** `traffic_aggregates` - IP bandwidth consumption by source IP per window
  - Columns: id (BIGSERIAL PK), src_ip (VARCHAR(45) NOT NULL), window_start (TIMESTAMP NOT NULL), window_end (TIMESTAMP NOT NULL), in_bytes (BIGINT NOT NULL CHECK >=0), out_bytes (BIGINT NOT NULL CHECK >=0), in_packets (BIGINT NOT NULL CHECK >=0), out_packets (BIGINT NOT NULL CHECK >=0), record_count (BIGINT NOT NULL CHECK >=0), batch_id (UUID NOT NULL), ingested_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Indexes: (src_ip, window_start) UNIQUE, (window_start) for time-based queries, (batch_id) for run tracking
  - Partitioning: RANGE (window_start) for time-series data management

- **1.1.2** `connection_aggregates` - IP connection details per window
  - Columns: id (VARCHAR(64) PRIMARY KEY - SHA-256 hash), src_ip (VARCHAR(45) NOT NULL), dst_ip (VARCHAR(45) NOT NULL), src_port (INT NOT NULL), dst_port (INT NOT NULL), l4_protocol (INT NOT NULL), window_start (TIMESTAMP NOT NULL), window_end (TIMESTAMP NOT NULL), in_bytes (BIGINT NOT NULL CHECK >=0), out_bytes (BIGINT NOT NULL CHECK >=0), in_packets (BIGINT NOT NULL CHECK >=0), out_packets (BIGINT NOT NULL CHECK >=0), batch_id (UUID NOT NULL), ingested_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Indexes: (src_ip, window_start), (dst_ip, window_start), (window_start), (batch_id)
  - Partitioning: RANGE (window_start)
  - Foreign Key: l4_protocol → protocol_dim(id)

- **1.1.3** `protocol_usage_aggregates` - Protocol usage statistics per window
  - Columns: id (BIGSERIAL PK), l7_protocol (DOUBLE PRECISION NOT NULL), window_start (TIMESTAMP NOT NULL), window_end (TIMESTAMP NOT NULL), in_bytes (BIGINT NOT NULL CHECK >=0), out_bytes (BIGINT NOT NULL CHECK >=0), in_packets (BIGINT NOT NULL CHECK >=0), out_packets (BIGINT NOT NULL CHECK >=0), connection_count (BIGINT NOT NULL CHECK >=0), batch_id (UUID NOT NULL), ingested_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Indexes: (l7_protocol, window_start) UNIQUE, (window_start), (batch_id)
  - Partitioning: RANGE (window_start)

- **1.1.4** `ip_bandwidth_aggregates` - Top IPs by bandwidth consumption (bidirectional)
  - Columns: id (BIGSERIAL PK), ip (VARCHAR(45) NOT NULL), window_start (TIMESTAMP NOT NULL), window_end (TIMESTAMP NOT NULL), inbound_bytes (BIGINT NOT NULL CHECK >=0), outbound_bytes (BIGINT NOT NULL CHECK >=0), inbound_packets (BIGINT NOT NULL CHECK >=0), outbound_packets (BIGINT NOT NULL CHECK >=0), batch_id (UUID NOT NULL), ingested_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Indexes: (ip, window_start) UNIQUE, (window_start), (batch_id)
  - Partitioning: RANGE (window_start)

- **1.1.5** `protocol_dim` - Static reference/lookup table for protocols
  - Columns: id (INT PRIMARY KEY), name (VARCHAR(50) NOT NULL), description (VARCHAR(200)), created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Seed data: (6, 'TCP', 'Transmission Control Protocol'), (17, 'UDP', 'User Datagram Protocol'), (1, 'ICMP', 'Internet Control Message Protocol'), etc.

### 1.2 Database Migration Strategy
- **1.2.1** Flyway for schema versioning (production standard)
- **1.2.2** Initial migration scripts for all 5 tables (4 fact + 1 dimension)
- **1.2.3** Seed migration for `protocol_dim` reference data
- **1.2.4** Idempotent migration scripts (IF NOT EXISTS, CREATE IF NOT EXISTS)
- **1.2.5** Partition maintenance strategy (pg_partman or scheduled job)

## 2. PostgreSQL Writer Component

### 2.1 Architecture & Design
- **2.1.1** `PostgresWriter` component with JDBC batch writing capabilities
- **2.1.2** Use HikariCP connection pool (already configured in PostgresConfig)
- **2.1.3** Write strategy: append-only (INSERT) with batch size optimization
- **2.1.4** Parallel writes using Spark's `foreachPartition`
- **2.1.5** Batch ID propagation: generated once per `runBatchJob()` execution, shared across all 4 processors

### 2.2 Implementation Components
- **2.2.1** `PostgresWriter` - Core JDBC batch writer
  - Batch size: configurable (default 1000)
  - PreparedStatement for each table
  - Transaction per batch
  - Connection management via HikariCP

- **2.2.2** `TableMapper` - Dataset Row to PreparedStatement mapping
  - Column order mapping per table
  - Type conversion (Timestamp, Long, String, Integer, UUID)
  - Null handling with proper defaults
  - SHA-256 ID generation for connection_aggregates

- **2.2.3** Error Handling Strategy
  - Fail-fast on connection errors
  - Batch-level rollback on individual batch failure
  - Detailed logging for failed batches with batch_id correlation
  - Circuit breaker pattern for repeated failures

### 2.3 Configuration
- **2.3.1** Batch size configuration in `application-*.yml`
- **2.3.2** Retry configuration (3 attempts, exponential backoff)
- **2.3.3** Connection pool tuning (already in PostgresConfig)
- **2.3.4** Write mode: console vs database (existing OutputConfig)

## 3. Processor Modifications

### 3.1 Base Processor Enhancements
- **3.1.1** Add `PostgresWriter` dependency injection
- **3.1.2** Modify `process()` signature to accept `String batchId` parameter
- **3.1.3** Add `writeToDatabase(Dataset<Row>, String batchId)` method
- **3.1.4** Add `batch_id` and `ingested_at` columns to final aggregation
- **3.1.5** Unified error handling for write operations
- **3.1.6** Performance metrics logging (records written, time taken)

### 3.2 TrafficAggregationProcessor Updates
- **3.2.1** Keep console output for development
- **3.2.2** Add `writeToPostgres()` method
- **3.2.3** Map aggregated columns to `traffic_aggregates` schema
- **3.2.4** Add `batch_id` column with lit value
- **3.2.5** Log write statistics with batch_id correlation

### 3.3 ConnectionProcessor Updates
- **3.3.1** Keep console output for development
- **3.3.2** Add `writeToPostgres()` method
- **3.3.3** Map aggregated columns to `connection_aggregates` schema
- **3.3.4** Ensure SHA-256 ID generation remains
- **3.3.5** Add `batch_id` column with lit value

### 3.4 ProtocolProcessor Updates
- **3.4.1** Keep console output for development
- **3.4.2** Add `writeToPostgres()` method
- **3.4.3** Map aggregated columns to `protocol_usage_aggregates` schema
- **3.4.4** Add `batch_id` column with lit value

### 3.5 TopIpProcessor Updates
- **3.5.1** Keep console output for development
- **3.5.2** Rename to `IpBandwidthProcessor` (optional - aligns with table name)
- **3.5.3** Add `writeToPostgres()` method
- **3.5.4** Map aggregated columns to `ip_bandwidth_aggregates` schema
- **3.5.5** Add `batch_id` column with lit value

### 3.6 BatchJobRunner Updates
- **3.6.1** Generate single `batchId` (UUID) per execution
- **3.6.2** Pass `batchId` to each processor's `process(String batchId)` method
- **3.6.3** Log batch_id at start and end of job execution
- **3.6.4** Include batch_id in all log messages for traceability

## 4. Testing Strategy

### 4.1 Unit Tests
- **4.1.1** `PostgresWriter` unit tests with mock JDBC
- **4.1.2** TableMapper tests for each table schema
- **4.1.3** Configuration validation tests
- **4.1.4** Error handling tests
- **4.1.5** Batch ID propagation tests

### 4.2 Integration Tests with Testcontainers
- **4.2.1** Base test class with PostgreSQL container + Flyway migrations
- **4.2.2** Test actual writes to PostgreSQL with real connections
- **4.2.3** Verify data integrity after write (NOT NULL, CHECK constraints)
- **4.2.4** Test duplicate prevention (UNIQUE constraints)
- **4.2.5** Verify protocol_dim reference data seeded
- **4.2.6** Performance tests with sample data

### 4.3 Processor Integration Tests
- **4.3.1** Test each processor with HDFS data + PostgreSQL write
- **4.3.2** Verify records exist in database with correct batch_id
- **4.3.3** Test duplicate write prevention (idempotency via UNIQUE)
- **4.3.4** Test with empty datasets (graceful handling)
- **4.3.5** Verify all 4 processors share same batch_id in a run

### 4.4 End-to-End Tests
- **4.4.1** Full batch job execution with batch_id generation
- **4.4.2** Verify all 4 tables populated with same batch_id
- **4.4.3** Test with scheduled execution
- **4.4.4** Validate output mode switching (console vs database)
- **4.4.5** Verify protocol_dim FK constraints work


## 5. Topic-4-times

### 5.1 change TablleMapper
- 5.1.1 Add JDBI dependency to pom.xml
- 5.1.2 Create domain model classes (TrafficAggregate, etc.)
- 5.1.3 Create JDBI DAO interfaces with @SqlBatch
- 5.1.4 Rewrite PostgresWriter to use JDBI
- 5.1.5 Keep retry and backoff logic
- 5.1.6 Delete TableMapper
- 5.1.7 Update tests

### 5.2 change ACK


### 5.3 Processing time change place


### 5.4 Incremental read + guarantee at-least-once with no data loss


## 6. Production Readiness

### 6.1 Error Handling & Resilience
- **5.1.1** Connection failure handling with retry
- **5.1.2** Batch failure partial rollback strategy
- **5.1.3** Health checks for PostgreSQL connectivity
- **5.1.4** Graceful degradation (fallback to console output if database unavailable)
- **5.1.5** Batch_id correlation for troubleshooting failed runs

### 6.2 Monitoring & Observability
- **5.2.1** Micrometer metrics for PostgreSQL writes
  - Records written per processor per batch_id
  - Write latency per table
  - Error count per table
- **5.2.2** Structured logging with batch_id correlation
- **5.2.3** Actuator endpoints for health checks
- **5.2.4** Batch run tracking via batch_id in database

### 6.3 Performance Optimization
- **5.3.1** Tune batch size based on data volume (configurable)
- **5.3.2** Parallel writes across partitions via foreachPartition
- **5.3.3** Connection pooling optimization (HikariCP)
- **5.3.4** Consider `COPY` for large inserts (PostgreSQL-specific)
- **5.3.5** Partition pruning via window_start range partitioning

### 6.4 Configuration Profiles
- **5.4.1** `dev` profile: console output + PostgreSQL (lightweight)
- **5.4.2** `prod` profile: PostgreSQL only (production)
- **5.4.3** `test` profile: Testcontainers for integration tests

## 7. Code Quality & Documentation

### 6.1 Clean Code Principles
- **6.1.1** Single Responsibility (SRP): processors only aggregate, writers only persist
- **6.1.2** Open/Closed: extendable for new table types
- **6.1.3** Dependency Injection for all components
- **6.1.4** Meaningful method and class names matching domain

### 6.2 Documentation
- **6.2.1** Javadoc for public APIs
- **6.2.2** README updates with new features and table definitions
- **6.2.3** Database schema documentation with ER diagram
- **6.2.4** Configuration documentation
- **6.2.5** Batch_id tracing documentation

### 6.3 Code Review Readiness
- **6.3.1** Self-review checklist
- **6.3.2** Performance considerations documented
- **6.3.3** Security considerations (SQL injection prevention with PreparedStatement)
- **6.3.4** Trade-offs documented (partitioning, indexing strategy)

## 7. Deliverables

### 7.1 Code Artifacts
- **7.1.1** `PostgresWriter.java` - Core writer component
- **7.1.2** Updated processor classes with batch_id-aware write methods
- **7.1.3** Updated `BatchJobRunner` with batch_id generation
- **7.1.4** Updated model classes with `batchId` and `ingestedAt` fields
- **7.1.5** Flyway migration scripts (V1__initial_schema.sql, V2__seed_protocol_dim.sql)
- **7.1.6** Test classes with Testcontainers
- **7.1.7** Updated configuration files

### 7.2 Testing Artifacts
- **7.2.1** Unit tests for all new components
- **7.2.2** Integration tests for database writes
- **7.2.3** Testcontainers configuration
- **7.2.4** Performance test results (or documentation)
- **7.2.5** Test coverage report (target: >80%)

### 7.3 Documentation
- **7.3.1** Updated README with PostgreSQL integration
- **7.3.2** Database schema diagram (ER diagram)
- **7.3.3** Architecture decision records (ADRs) for key decisions
- **7.3.4** Configuration guide with all options
- **7.3.5** Batch_id tracing guide for troubleshooting

### 7.4 TPM Review Preparation
- **7.4.1** Architecture decisions and rationale (partitioning, constraints, batch_id)
- **7.4.2** Performance considerations (indexes, partitions, batch sizes)
- **7.4.3** Error handling strategy (retry, fallback, batch_id correlation)
- **7.4.4** Production readiness assessment
- **7.4.5** Data lineage and auditability via batch_id

## 8. Risk Mitigation

### 8.1 Technical Risks
- **8.1.1** PostgreSQL performance under load → Batch size tuning, connection pooling, partition pruning
- **8.1.2** Data consistency on retry → Idempotent writes via UNIQUE constraints, batch_id tracking
- **8.1.3** Memory issues with large datasets → Streaming writes via foreachPartition
- **8.1.4** Network latency to PostgreSQL → Connection pooling, keep-alive, retry logic
- **8.1.5** Schema evolution → Flyway migrations with versioning

### 8.2 Project Risks
- **8.2.1** Time constraints → Prioritize core functionality (writes), defer optimizations
- **8.2.2** Integration complexity → Start with simple append with batch_id, iterate
- **8.2.3** Testing complexity → Testcontainers simplifies integration testing
- **8.2.4** Data volume growth → Partitioning strategy addresses this proactively

