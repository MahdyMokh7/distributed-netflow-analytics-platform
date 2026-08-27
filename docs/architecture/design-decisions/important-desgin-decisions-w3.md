# Nimbo Batch Processor — Key Technical Decisions (Week 3)

---

## 1. JDBI Over Manual JDBC

### Decision
Replace the manual `TableMapper` (500+ lines of fragile type-switching code) with JDBI DAO interfaces.

### Why
| Aspect | Manual JDBC | JDBI |
|--------|-------------|------|
| **Code volume** | 500+ lines | ~50 lines per DAO |
| **Type safety** | ❌ Hardcoded strings | ✅ `@BindBean` compile-time safety |
| **Maintainability** | ❌ Changes require multiple files | ✅ One DAO per table |
| **Batch support** | Manual `addBatch()`/`executeBatch()` | `@SqlBatch` handles it |

### Implementation
```java
@RegisterBeanMapper(TrafficAggregate.class)
public interface TrafficAggregateDao {
    @SqlBatch(TRAFFIC_AGGREGATES_UPSERT)
    int[] upsertBatch(@BindBean List<TrafficAggregate> aggregates);
}
```

---

## 2. Two Schemas for Avro (Wire vs Storage)

### Decision
Maintain two separate Avro schemas:
- **Wire schema** — original 43 fields, used only for Kafka deserialization
- **Storage schema** — original + `PROCESSING_TIME`, used only for Parquet writing

### Why
Avro binary encoding is **positional** — field names aren't sent over the wire. Adding `PROCESSING_TIME` to the reader schema but not the writer schema would cause `EOFException` during deserialization.

### Implementation
```java
@Bean
public Schema avroSchema() { /* 43 fields, from .avsc file */ }

@Bean
public Schema storageSchema(Schema avroSchema) {
    // Programmatically add PROCESSING_TIME to avroSchema
    // Used only for Parquet writing
}
```

---

## 3. Processing Time Placement

### Decision
Add `PROCESSING_TIME` in `batch-ingester` (when writing Parquet), not in Spark processor.

### Why
| Location | Issue |
|----------|-------|
| **In Spark processor** | `current_timestamp()` changes based on when you process, not when data arrived |
| **In batch-ingester** | Captures actual ingestion time, consistent across reprocessing |

### Implementation
```java
private GenericRecord enrichWithProcessingTime(GenericRecord original) {
    Instant now = Instant.now();  // Single call, reused
    long processingTimeMicros = now.getEpochSecond() * 1_000_000 + now.getNano() / 1000;
    // ... add to record
}
```

---

## 4. Per-Record Processing Time

### Decision
Each record gets its own `processing_time` value (`Instant.now()` per record), not one timestamp per file.

### Why
- `batch_id` is generated per scheduler run (not per file)
- UPSERT key = `(window_start, dimension, batch_id)` — prevents overwrites
- A single Parquet file can legitimately contain records from multiple windows — `window()` handles this correctly

### Trade-off
- Files can span multiple hours → correctly processed by Spark's `window()` function

---

## 5. Filename Format (Fixed Width)

### Decision
Use fixed-width filename format: `netflow-p{partition:02d}-{timestamp}-{uuid}.parquet`

### Format
```
netflow-p01-2026-08-28T10-30-00-123Z-abc-def.parquet
         ↑                        ↑
    fixed 2 digits         24 chars (fixed)
```

### Why
| Before (Variable) | After (Fixed) |
|-------------------|---------------|
| `p0`, `p10` (different start positions) | `p01`, `p10` (timestamp always at index 12) |
| Complex `indexOf()` scanning | Simple `substring(start, start + 24)` |
| Parsing failures with fractional seconds | Fixed format: `yyyy-MM-dd'T'HH-mm-ss-SSS'Z'` |

---

## 6. File Discovery: Sort Before Cap

### Decision
Collect ALL unprocessed files, sort by timestamp (oldest first), THEN apply `maxFilesPerBatch` cap.

### Before (Broken)
```java
for (Path file : allFiles) {  // arbitrary HDFS order
    // ...
    newFiles.add(...);
    if (newFiles.size() >= maxFiles) break;  // ❌ Cap BEFORE sorting
}
newFiles.sort(Comparator.comparing(f -> f.timestamp));  // Sorts arbitrary subset
```

### After (Fixed)
```java
// Collect ALL files first
for (Path file : allFiles) {
    allNewFiles.add(...);
}
// Sort ALL by timestamp (oldest first)
allNewFiles.sort(Comparator.comparing(f -> f.timestamp));
// Apply cap AFTER sorting
allNewFiles.stream().limit(maxFiles).collect(...);
```

### Why
`FileSystem.listStatus()` order isn't guaranteed. Without sorting first, old files could starve indefinitely.

---

## 7. Dual State Storage (PostgreSQL + In-Memory)

### Decision
Track processed files in both PostgreSQL and in-memory cache.

### Priority
1. **PostgreSQL first** (source of truth across instances)
2. **In-memory fallback** (if DB is down)

### Implementation
```java
private Set<String> getProcessedFileNames() {
    try {
        Set<String> dbFiles = stateDao.getProcessedFileNames();
        inMemoryProcessedFiles.clear();
        inMemoryProcessedFiles.addAll(dbFiles);
        return dbFiles;
    } catch (Exception e) {
        // Fallback to in-memory
        return inMemoryProcessedFiles;
    }
}
```

### Why
- Multi-instance: DB is the only reliable source across instances
- Resilience: if DB is down, memory preserves state (at-least-once)
- Recovery: when DB comes back, memory syncs with it

---

## 8. Batch ID Per Scheduler Run

### Decision
`batch_id` (UUID) is generated once per `BatchJobRunner.runBatchJob()` execution, shared across all 4 processors.

### Why
| Concern | How It's Solved |
|---------|-----------------|
| **Idempotency** | UPSERT key includes `batch_id` → re-running same data doesn't overwrite |
| **Auditability** | Every row in PostgreSQL has `batch_id` → trace data to a specific run |
| **No data loss** | If a batch fails, no `batch_id` is committed → reprocess next run (at-least-once) |

---

## 9. ACK Verification on Batch Writes

### Decision
Verify every statement in a JDBC batch affected exactly 1 row.

### Implementation
```java
private void verifyAllRowsAffected(int[] results) {
    for (int result : results) {
        if (result != 1) {
            throw new RuntimeException("expected 1 row affected, got " + result);
        }
    }
}
```

### Why
- `statement.executeBatch()` returns `int[]` where each int is rows affected
- If any row is `0` (no insert/update) or `>1` (shouldn't happen with unique constraints), the batch is invalid
- Ensures zero data loss — no commit without full ACK

---

## 10. Retry with Exponential Backoff

### Decision
Retry failed batch operations with exponential backoff.

### Implementation
```java
private long calculateRetryDelay(int attempt, long initialDelayMs) {
    return initialDelayMs * (long) Math.pow(2.0, (double) attempt - 1);
}
// Attempt 1: 1000ms, Attempt 2: 2000ms, Attempt 3: 4000ms
```

### Why
- Prevents database overload during recovery
- Gives DB time to recover from transient failures
- Configurable: `maxAttempts`, `initialDelayMs`

---

## 11. At-Least-Once Semantics

### Decision
Files are marked as processed **ONLY after ALL processors succeed**.

### Implementation
```java
try {
    // Process all files through all 4 processors
    for (BaseProcessor processor : processors) {
        processor.process(files, batchId);
    }
    // ONLY after ALL succeed
    fileDiscoveryService.markFilesAsProcessed(files, batchId);
} catch (Exception e) {
    // DO NOT mark files as processed → reprocess next run
    throw e;
}
```

### Why
- If any processor fails, files aren't marked → next run reprocesses them
- This gives at-least-once semantics (acceptable trade-off for no data loss)
- `ON CONFLICT DO UPDATE` prevents duplicates on reprocessing

---

## 12. Test Data Includes PROCESSING_TIME

### Decision
Test data generation includes `PROCESSING_TIME` column.

### Why
- Processor tests need `PROCESSING_TIME` column to exist
- Generated Parquet files must match the actual schema used in production

### Implementation
```java
DataTypes.createStructField(
    "PROCESSING_TIME",
    DataTypes.TimestampType,
    true
)
```

---

## Summary

| Decision | Key Reason |
|----------|------------|
| **JDBI over JDBC** | Type safety, 90% less code |
| **Two Avro schemas** | Prevent EOF on deserialization |
| **Processing time in ingester** | Consistent across reprocessing |
| **Per-record processing time** | Correct for Spark windowing |
| **Fixed-width filename** | Simple timestamp extraction |
| **Sort before cap** | Prevents file starvation |
| **Dual state storage** | Resilience + multi-instance support |
| **Batch ID per run** | Idempotency + auditability |
| **ACK verification** | Zero data loss guarantee |
| **Exponential backoff** | Prevent DB overload on recovery |
| **At-least-once** | No data loss |
| **Test data with PROCESSING_TIME** | Tests match production schema |