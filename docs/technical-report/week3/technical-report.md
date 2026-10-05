# Nimbo Project — Week 3 Technical Report

## Cube Maker Implementation & Production Hardening

---

### 1. Executive Summary

Week 3 focused on completing the **batch-processor** module (Cube Maker) by implementing PostgreSQL integration for aggregate storage and production-hardening the entire batch pipeline. The work included:

- Replacing the fragile manual `TableMapper` with a production-grade JDBI DAO layer
- Implementing proper Avro schema separation for Kafka deserialization vs Parquet writing
- Adding `PROCESSING_TIME` as a per-record ingestion timestamp
- Building incremental file reading with at-least-once semantics
- Fixing critical bugs including ANSI overflow, timestamp parsing, and file discovery ordering

**Status:** All 4 aggregations are working. Tests are passing. The batch-processor module is production-ready.

---

### 2. System Architecture Review

#### 2.1 Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              NIMBO DATA PIPELINE                                   │
│                                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │  Kafka   │───▶│   Ingester   │───▶│    HDFS     │───▶│   Spark     │         │
│  │ (Avro)   │    │ (Consumer)   │    │  (Parquet)  │    │  Processor  │         │
│  └──────────┘    └──────────────┘    └──────────────┘    └──────┬───────┘         │
│         ▲               ▲                      ▲                 │                 │
│         │               │                      │                 ▼                 │
│    Netflow          Avro Schema          PROCESSING_TIME    ┌──────────────┐      │
│    Records          + Storage Schema     column added      │ PostgreSQL   │      │
│    (43 fields)        (44 fields)        at ingestion      │ 4 Aggregate  │      │
│                       ▲                                    │ Tables       │      │
│                       │                                    └──────────────┘      │
│                   Built programmatically                                        │
│                   (wire schema + PROCESSING_TIME)                               │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### 2.2 Module Status

| Module | Purpose | Status |
|--------|---------|--------|
| `parquet-writer` | Kafka → HDFS Parquet | ✅ Complete |
| `batch-processor` | Spark → PostgreSQL | ✅ Complete |
| `realtime` | Kafka → StarRocks | ⚠️ Not covered |

---

### 3. Key Technical Implementations

#### 3.1 JDBI DAO Layer (Replacing TableMapper)

**Problem:** The original `TableMapper` used 500+ lines of manual type-switching code to convert Spark `Row` to JDBC `PreparedStatement`. This was:
- Fragile (hardcoded column names)
- Error-prone (manual type conversion)
- Hard to maintain (every new table required changes)

**Solution:** Implemented JDBI DAO pattern with declarative SQL.

**Implementation Details:**

| Component | Purpose |
|-----------|---------|
| `@RegisterBeanMapper` | Maps database columns to Java object fields |
| `@SqlBatch` | Executes batch inserts with `ON CONFLICT DO UPDATE` |
| `@BindBean` | Automatically binds Java object fields to SQL parameters |
| `JdbiConfig` | Creates Jdbi instance and registers all DAO beans |

**Key Classes Created:**

| File | Purpose |
|------|---------|
| `JdbiConfig.java` | JDBI configuration with explicit DAO beans |
| `TrafficAggregateDao.java` | UPSERT operations for traffic_aggregates |
| `ConnectionAggregateDao.java` | UPSERT operations for connection_aggregates |
| `ProtocolAggregateDao.java` | UPSERT operations for protocol_usage_aggregates |
| `IpBandwidthAggregateDao.java` | UPSERT operations for ip_bandwidth_aggregates |
| `BatchStateDao.java` | CRUD operations for batch_processor_state |

**Result:** Code reduced from 500+ lines to ~50 lines of clean, type-safe DAO interfaces.

---

#### 3.2 Avro Schema Separation (Wire vs Storage)

**Problem:** Adding `PROCESSING_TIME` to the same Avro schema used for Kafka deserialization caused `EOFException` — Avro binary encoding is positional, and Kafka messages don't contain the extra field.

**Solution:** Two schemas, one source of truth.

| Schema | Purpose | Fields | Source |
|--------|---------|--------|--------|
| **Wire Schema** | Kafka deserialization | 43 fields | `NetFlowV2Record.avsc` file |
| **Storage Schema** | Parquet writing | 43 + PROCESSING_TIME | Built programmatically |

**Implementation:**

```java
@Bean
public Schema avroSchema() {
    // Loads 43 fields from .avsc file — used only for Kafka
}

@Bean
public Schema storageSchema(Schema avroSchema) {
    // Copies all 43 fields, adds PROCESSING_TIME — used only for Parquet
    // Each field is copied as a NEW instance to avoid "Field already used" error
}
```

**Critical Detail:** Each field must be a **new instance** when copying — reusing field objects triggers `AvroRuntimeException: Field already used`.

---

#### 3.3 Processing Time Implementation

**Decision:** Add `PROCESSING_TIME` as a per-record column in the Parquet file at ingestion time (parquet-writer), not at processing time (batch-processor).

**Why This Matters:**

| Location | Issue |
|----------|-------|
| **In Spark processor** | `current_timestamp()` changes based on when you process, not when data arrived |
| **In parquet-writer** | Captures actual ingestion time, consistent across reprocessing |

**Implementation Details:**

| Aspect | Implementation |
|--------|----------------|
| **Value source** | `Instant.now()` per record |
| **Format** | Epoch microseconds (matches Spark's `TimestampType` precision) |
| **Schema** | `{"type": "long", "logicalType": "timestamp-micros"}` |
| **Spark usage** | `window(col("PROCESSING_TIME").cast("timestamp"), windowDuration)` |

**Bug Fixed:** Double `Instant.now()` call — rare chance of crossing second boundary. Fixed by capturing `Instant` once and reusing.

```java
// Before (could be corrupted)
long processingTimeMicros = Instant.now().getEpochSecond() * 1_000_000 + Instant.now().getNano() / 1000;

// After (safe)
Instant now = Instant.now();
long processingTimeMicros = now.getEpochSecond() * 1_000_000 + now.getNano() / 1000;
```

---

#### 3.4 Incremental File Reading (At-Least-Once)

**Problem:** Reading all Parquet files every run causes memory issues and reprocesses old data.

**Solution:** File tracking with at-least-once semantics.

**Key Decisions:**

| Decision | Rationale |
|----------|-----------|
| **Track individual filenames** | Handles late-arriving files — no file is ever skipped or starved |
| **PostgreSQL as source of truth** | Multi-instance deployments need shared state |
| **In-memory fallback** | At-least-once semantics preserved even during DB outages |
| **Files marked AFTER all processors succeed** | Partial failure → files reprocessed next run |

**File Discovery Flow:**

```
1. List all files in HDFS
2. Query PostgreSQL for already-processed filenames
3. Filter out processed files
4. Sort ALL remaining files by timestamp (oldest first)
5. Apply maxFilesPerBatch cap AFTER sorting
6. Return limited list
```

**Why Sort-Before-Cap Matters:**

`FileSystem.listStatus()` order isn't guaranteed to be chronological. Without sorting first, the cap would be applied to an arbitrary subset, causing old files to starve.

---

#### 3.5 Decimal Type for Aggregations (ANSI Overflow Fix)

**Problem:** Network traffic sums can exceed `Long.MAX_VALUE` (9.22 × 10¹⁸). Spark's ANSI mode (enabled by default) throws `ArithmeticException` on overflow.

**Solution:** Cast all `sum()` operations to `Decimal(38,0)`.

```java
// Before (overflow possible)
sum(col(IN_BYTES)).alias(IN_BYTES)

// After (safe up to 10^38)
sum(col(IN_BYTES).cast("decimal(38,0)")).alias(IN_BYTES)
```

**Why Not Disable ANSI:**

| Approach | Why We Rejected It |
|----------|-------------------|
| Disable ANSI | Hides data corruption issues — not production-grade |
| Cast to Decimal | Safe, no side effects, keeps ANSI enabled for other checks |

**Database Schema Consideration:** If using `BIGINT` for byte columns, consider changing to `NUMERIC(38,0)` to prevent overflow on insertion.

---

#### 3.6 HDFS FileSystem Bean Configuration

**Problem:** `FileDiscoveryService` required a `FileSystem` bean but Spring didn't have one.

**Solution:** Created `HdfsFileSystemConfig` to provide the bean.

```java
@Configuration
public class HdfsFileSystemConfig {
    @Bean
    public FileSystem fileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsConfig.getUri());
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        return FileSystem.get(URI.create(hdfsConfig.getUri()), conf);
    }
}
```

**Note:** The import conflict between `org.apache.hadoop.conf.Configuration` and `org.springframework.context.annotation.Configuration` was resolved by using full qualified names.

---

#### 3.7 Filename Format Standardization

**Problem:** Original `FileNameGenerator` used `ISO_INSTANT` which includes variable-length fractional seconds. The parser couldn't reliably extract the timestamp.

**Solution:** Fixed-width format with millisecond precision.

```
Original: netflow-p0-2026-08-27T10-30-00.123456789Z-abc.parquet
          ↑ variable length (hard to parse)

New:      netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet
          ↑ 2-digit partition  ↑ fixed 24 chars  ↑ easy parsing
```

**Key Changes:**

| Component | Before | After |
|-----------|--------|-------|
| Partition | `p0`, `p10` (variable) | `p01`, `p10` (fixed 2 digits) |
| Timestamp | `ISO_INSTANT` (variable fraction) | `yyyy-MM-dd'T'HH-mm-ss-SSS'Z'` (fixed 24 chars) |
| Parser | `indexOf` scanning | Fixed-position substring |
| Pattern | Complex regex optional | Simple `substring(12, 36)` |

---

### 4. Critical Bugs Fixed

| Bug | Description | Root Cause | Fix |
|-----|-------------|------------|-----|
| **#1: Schema Pollution** | Kafka deserialization failed with `EOFException` | `PROCESSING_TIME` added to wire schema | Two schemas (wire + storage) |
| **#2: Writer Schema Mismatch** | `PROCESSING_TIME` silently dropped from Parquet | `openWriter()` used wire schema | Use `storageSchema` for Parquet writer |
| **#3: Filename Parsing** | All files skipped by `FileDiscoveryService` | `extractTimestamp()` found wrong dash | Fixed-width format + regex |
| **#4: File Discovery Ordering** | Old files starved with backlog | Cap applied before sorting | Sort ALL files, THEN cap |
| **#5: Double Instant.now()** | Rare timestamp corruption | Two separate `Instant.now()` calls | One `Instant`, reused |
| **#6: ANSI Overflow** | `ArithmeticException` in aggregations | `sum(Long)` exceeded `Long.MAX_VALUE` | Cast to `Decimal(38,0)` |
| **#7: Processing Time Type** | `window()` expected `Timestamp`, got `BIGINT` | Spark read as `LongType` | `.cast("timestamp")` |

---

### 5. Testing Strategy

#### 5.1 Unit Tests

| Test Class | Coverage |
|------------|----------|
| `TrafficAggregationProcessorTest` | ✅ All processor edge cases |
| `ConnectionProcessorTest` | ✅ Aggregation logic, empty dataset handling |
| `ProtocolProcessorTest` | ✅ Protocol grouping and sums |
| `TopIpProcessorTest` | ✅ Bidirectional IP aggregation |
| `BatchJobRunnerTest` | ✅ File discovery integration, failure recovery |
| `PostgresWriterTest` | ✅ Batch writes, retry, ACK verification |
| `FileDiscoveryServiceTest` | ✅ File listing, filtering, sort-before-cap, fallback |

**Results:** 96 tests passing (0 failures, 0 errors).

#### 5.2 Integration Tests

| Test | Purpose | Status |
|------|---------|--------|
| `StorageIT` | End-to-end HDFS Parquet write/read | ✅ Passing |
| Postgres container tests | Full pipeline validation | ✅ Passing |

#### 5.3 Test Data Generation

The `TestDataGenerator` was updated to include `PROCESSING_TIME` column, ensuring test data matches the production schema.

---

### 6. Configuration Changes

#### 6.1 New Configuration Properties

| Property | Purpose | Default |
|----------|---------|---------|
| `batch.max-files-per-batch` | Limit files per batch run | 10 (dev), 20 (prod) |
| `postgres.retry.max-attempts` | Max retry attempts | 3 (dev), 5 (prod) |
| `postgres.retry.initial-delay-ms` | Initial retry delay | 1000ms (dev), 2000ms (prod) |
| `output.fallback-to-console-on-db-failure` | Console fallback when DB fails | `false` |

#### 6.2 Updated Profiles

| Profile | Purpose |
|---------|---------|
| `dev` | Console output, local PostgreSQL, 10 files/batch |
| `prod` | Database output, remote PostgreSQL, 20 files/batch |

---

### 7. Code Quality Improvements

| Principle | How It's Applied |
|-----------|------------------|
| **SRP** | Processors only aggregate; Writers only persist |
| **DRY** | DAO pattern eliminates duplicate JDBC code |
| **KISS** | Simple, explicit configurations over complex auto-discovery |
| **Separation of Concerns** | Wire schema ≠ Storage schema |
| **Fail Fast** | ACK verification on batch writes |
| **Idempotency** | UPSERT with `ON CONFLICT DO UPDATE` |

---

### 8. Files Modified/Created

#### 8.1 New Files (batch-processor)

| File | Purpose |
|------|---------|
| `HdfsFileSystemConfig.java` | HDFS FileSystem bean |
| `JdbiConfig.java` | JDBI configuration + DAO beans |
| `BatchStateDao.java` | State table DAO |
| `BatchStateRecord.java` | State table model |
| `FileDiscoveryService.java` | Incremental file reading |

#### 8.2 Modified Files (batch-processor)

| File | Changes |
|------|---------|
| `PostgresWriter.java` | JDBI integration |
| `TableRowMapper.java` | Clean mapping (no type-switching) |
| `All 4 processors` | `.cast("decimal(38,0)")` + `.cast("timestamp")`, removed `orderBy` |
| `BatchJobRunner.java` | File discovery integration |
| `BaseProcessor.java` | `process(List<Path> files, UUID batchId)` |
| `ColumnConstants.java` | Added `NETFLOW_EVENT_TIME` constant |
| `application.yml` | Added `max-files-per-batch` |
| `application-dev.yml` | Updated for dev environment |
| `application-prod.yml` | Updated for prod environment |
| `BatchConfig.java` | Added `maxFilesPerBatch` field |
| `pom.xml` | Added JDBI dependencies |

#### 8.3 Modified Files (parquet-writer)

| File | Changes |
|------|---------|
| `NetFlowV2Record.avsc` | **Reverted** — removed `PROCESSING_TIME` (wire schema stays clean) |
| `IngesterConfiguration.java` | Added `storageSchema()` bean |
| `NetFlowStorage.java` | Uses `storageSchema` for Parquet, adds `PROCESSING_TIME` per record |
| `FileNameGenerator.java` | Fixed-width format, 2-digit partition, millisecond precision |
| `ConsumerManager.java` | Injected `storageSchema` parameter |

---

### 9. Current Architecture (End of Week 3)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BATCH-PROCESSOR ARCHITECTURE                            │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     FileDiscoveryService                            │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │  1. List HDFS files                                           │ │   │
│  │  │  2. Filter: already processed (PostgreSQL + in-memory)        │ │   │
│  │  │  3. Sort by timestamp (oldest first)                          │ │   │
│  │  │  4. Limit: maxFilesPerBatch                                   │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        4 Processors                                 │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐   │   │
│  │  │ Traffic    │  │ Connection │  │ Protocol   │  │ Top IP     │   │   │
│  │  │ Aggregation│  │ Aggregation│  │ Aggregation│  │ Aggregation│   │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘   │   │
│  │    window:      window:         window:        window:             │   │
│  │    1 hour       5 minutes       1 hour         1 hour              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      PostgresWriter                                 │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │  JDBI DAOs → @SqlBatch → INSERT ... ON CONFLICT DO UPDATE    │ │   │
│  │  │  ACK: verifyAllRowsAffected() → retry with exponential backoff│ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                       PostgreSQL                                    │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │   │
│  │  │ traffic_aggregates│  │connection_       │  │protocol_usage_   │  │   │
│  │  │ (src_ip, window) │  │aggregates        │  │aggregates        │  │   │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │   │
│  │  ┌──────────────────┐  ┌──────────────────┐                       │   │
│  │  │ ip_bandwidth_    │  │ batch_processor_ │                       │   │
│  │  │ aggregates       │  │ state            │                       │   │
│  │  │ (ip, window)     │  │ (processed files)│                       │   │
│  │  └──────────────────┘  └──────────────────┘                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 10. What Was Deferred

| Feature | Reason | Priority |
|---------|--------|----------|
| Fallback to console on DB failure | Retry logic already handles transient failures | Low |
| PostgreSQL health check | Can be added later with actuator | Low |
| Parallel writes via `foreachPartition` | Aggregated data is small (thousands of rows) | Low |
| `COPY` for large inserts | Not needed for current data volume | Low |

---

### 11. Summary of Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **JDBI over manual JDBC** | Type safety, 90% less code, maintainable |
| **Two Avro schemas** | Prevent EOF on Kafka deserialization |
| **Processing time in ingester** | Consistent across reprocessing, correct for windowing |
| **Per-record processing time** | Correct for Spark windowing (files can span multiple windows) |
| **Fixed-width filename** | Simple timestamp extraction, no parsing ambiguity |
| **Sort-before-cap** | Prevents file starvation with backlog |
| **Decimal(38,0) for sums** | No overflow, keeps ANSI enabled |
| **At-least-once semantics** | No data loss, acceptable trade-off |
| **batch_id in UPSERT key** | Idempotency + auditability |
| **Explicit DAO beans** | Clear, debuggable, no magic |
| **Keep ANSI enabled** | Production-grade — don't hide data corruption |

---

### 12. Conclusion

Week 3 successfully completed the batch-processor module with all 4 aggregations writing to PostgreSQL. The system is now:

- **Production-ready** — ACK verification, retry with backoff, at-least-once semantics
- **Maintainable** — JDBI DAO pattern replaces fragile 500+ line TableMapper
- **Scalable** — Incremental file reading, batch limits, configurable threading
- **Auditable** — batch_id traceability across all tables and logs
- **Correct** — Decimal type prevents overflow, two schemas prevent deserialization errors, fixed filename parsing

**The Cube Maker is complete and ready for integration with the REST API module.** 🚀