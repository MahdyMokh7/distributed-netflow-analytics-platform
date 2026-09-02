# Technical Report: Batch-Processor Writer Evolution

## From Driver-Centric to Executor-Centric Database Writing

---

### 1. Executive Summary

This report documents the architectural evolution of the Nimbo batch-processor's database writer component. The system transitioned from a driver-centric `toLocalIterator()` approach to a distributed `foreachPartition()` model where each Spark executor independently writes aggregated data to PostgreSQL. This transformation addressed critical scalability, memory, and serialization challenges while maintaining exactly-once processing semantics.

**Key Achievements:**
- ✅ Eliminated driver memory bottlenecks by distributing write operations
- ✅ Achieved true parallel writes across Spark partitions
- ✅ Resolved complex serialization issues with Spark closures
- ✅ Maintained idempotency via `batch_id` in UPSERT operations
- ✅ Implemented retry with exponential backoff and ACK verification

---

### 2. Problem Statement

#### 2.1 Original Architecture: Driver-Centric Writes

The initial implementation used `Dataset.toLocalIterator()` to collect all aggregated data on the Spark driver before writing to PostgreSQL:

```java
// Original approach
Iterator<Row> rows = dataset.toLocalIterator();
while (rows.hasNext()) {
    Row row = rows.next();
    // Convert and write to database
}
```

**Architecture Diagram:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ORIGINAL ARCHITECTURE                                   │
│                                                                             │
│  Spark Executors                                                           │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                           │
│  │ Partition  │  │ Partition  │  │ Partition  │                           │
│  │    1       │  │    2       │  │    3       │                           │
│  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘                           │
│         │               │               │                                  │
│         └───────────────┼───────────────┘                                  │
│                         ▼                                                  │
│               ┌─────────────────────┐                                      │
│               │   DRIVER JVM        │                                      │
│               │  toLocalIterator()  │  ← ALL DATA COLLECTED HERE           │
│               │  Collect all rows   │  ← Memory bottleneck                 │
│               └──────────┬──────────┘                                      │
│                          ▼                                                  │
│                  ┌──────────────┐                                          │
│                  │  PostgreSQL  │                                          │
│                  └──────────────┘                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 2.2 Limitations

| Limitation | Impact |
|------------|--------|
| **Driver Memory Pressure** | All aggregation results collected on driver JVM; large datasets cause OOM |
| **Single Point of Failure** | Driver failure = entire job failure |
| **No Parallelism** | Sequential writes; cannot leverage Spark's distributed nature |
| **Scalability Ceiling** | Limited by driver heap size, not by cluster resources |

---

### 3. Solution Design: Executor-Centric Writes

#### 3.1 New Architecture

The solution moves write operations to Spark executors using `Dataset.foreachPartition()`:

```java
// New approach
dataset.foreachPartition((Iterator<Row> partition) -> {
    // Each executor writes its own partition
    D dao = daoSupplier.get();
    List<T> batch = new ArrayList<>(batchSize);
    while (partition.hasNext()) {
        Row row = partition.next();
        batch.add(rowMapper.map(row, batchId, ingestedAt));
        if (batch.size() >= batchSize) {
            flushBatch(dao, batch);  // Write to PostgreSQL
        }
    }
});
```

**Architecture Diagram:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    NEW ARCHITECTURE                                         │
│                                                                             │
│  Spark Executors (Parallel Writes)                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Executor 1               │  Executor 2              │  Executor 3  │   │
│  │  ┌─────────────────────┐  │  ┌─────────────────────┐  │  ┌──────────┐│   │
│  │  │ Partition 1         │  │  │ Partition 2         │  │  │Partition ││   │
│  │  │ foreachPartition()  │  │  │ foreachPartition()  │  │  │    3     ││   │
│  │  │ ┌─────────────────┐ │  │  │ ┌─────────────────┐ │  │  │foreach...││   │
│  │  │ │ JDBI DAO        │ │  │  │ │ JDBI DAO        │ │  │  └──────────┘│   │
│  │  │ │ Batch 1 → DB    │ │  │  │ │ Batch 1 → DB    │ │  │     │        │   │
│  │  │ │ Batch 2 → DB    │ │  │  │ │ Batch 2 → DB    │ │  │     │        │   │
│  │  │ └─────────────────┘ │  │  │ └─────────────────┘ │  │     │        │   │
│  │  └─────────────────────┘  │  └─────────────────────┘  │     │        │   │
│  └───────────────────────────┼───────────────────────────┼─────┘        │   │
│                              │                           │               │   │
│                              ▼                           ▼               │   │
│                         ┌─────────────────────────────────────┐         │   │
│                         │           PostgreSQL               │         │   │
│                         │  (UPSERT with batch_id)            │         │   │
│                         └─────────────────────────────────────┘         │   │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 3.2 Benefits

| Aspect | Improvement |
|--------|-------------|
| **Scalability** | Horizontal scaling across all partitions |
| **Memory** | Each executor handles its own partition only |
| **Parallelism** | Concurrent writes across multiple executors |
| **Resilience** | Partition failures are isolated; retry only failed partitions |

---

### 4. Critical Implementation Challenges

#### 4.1 Serialization Problem

**The Issue:**

Spark serializes the closure of `foreachPartition()` to send to executors. The closure captured `this` (the `PostgresWriter` instance), which contained non-serializable objects:

```
PostgresWriter (Serializable)
    └── JdbiDaoFactory (Serializable)
            └── HikariDataSource (NOT Serializable)
                    └── Physical connections, thread pool, etc.
```

**The Error:**
```
java.io.NotSerializableException: com.zaxxer.hikari.HikariDataSource
```

**Why It Happened:**
- Spark uses Java serialization to send tasks to executors
- `HikariDataSource` is not `Serializable` (contains live connections, thread pools)
- The entire object graph must be serializable

#### 4.2 Serialization Solution: Lazy Initialization Pattern

**Phase 1: Remove Non-Serializable Fields from Serialization**

```java
@Component
public class JdbiDaoFactory implements Serializable {
    
    // Store only configuration as fields (all Serializable)
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxPoolSize;
    private final long connectionTimeoutMs;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final String poolName;
    
    // Mark DataSource as transient (skip serialization)
    private transient HikariDataSource dataSource;
}
```

**Phase 2: Lazy Recreate on Executor**

```java
private synchronized HikariDataSource getDataSource() {
    if (dataSource == null) {
        // Executor recreates DataSource from serialized config
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(username);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(maxPoolSize);
        cfg.setConnectionTimeout(connectionTimeoutMs);
        cfg.setIdleTimeout(idleTimeoutMs);
        cfg.setMaxLifetime(maxLifetimeMs);
        cfg.setPoolName(poolName);
        dataSource = new HikariDataSource(cfg);
    }
    return dataSource;
}
```

**Result:** The factory is serialized with configuration only; each executor recreates its own connection pool.

#### 4.3 Serialization of Lambda Expressions

**The Issue:**

The `foreachPartition()` lambda itself was not serializable:

```
java.io.NotSerializableException: PostgresWriter$$Lambda$5572/...
```

**Why It Happened:**
- Java lambdas are not inherently `Serializable`
- The lambda captured `this` (the `PostgresWriter`), which was serializable, but the lambda class itself wasn't

**Phase 1: Cast to Serializable Functional Interface**

```java
dataset.foreachPartition((ForeachPartitionFunction<Row>) partition -> {
    // ...
});
```

**Phase 2: Extract to Separate Serializable Class (Final Solution)**

```java
@Slf4j
public class PartitionWriter<T, D> implements Serializable {
    
    private final SerializableSupplier<D> daoSupplier;
    private final SerializableBiFunction<D, List<T>, int[]> upsertFunction;
    private final RowMapper<T> rowMapper;
    private final UUID batchId;
    private final Timestamp ingestedAt;
    private final BatchExecutor batchExecutor;
    private final LongAccumulator accumulator;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final long maxDelayMs;
    private final String tableName;
    
    public void call(Iterator<Row> partition) {
        // DAO created ONCE per partition (on the executor)
        D dao = daoSupplier.get();
        List<T> batch = new ArrayList<>(batchSize);
        
        while (partition.hasNext()) {
            Row row = partition.next();
            batch.add(rowMapper.map(row, batchId, ingestedAt));
            
            if (batch.size() >= batchSize) {
                flushBatch(dao, batch);
            }
        }
        
        if (!batch.isEmpty()) {
            flushBatch(dao, batch);
        }
    }
    
    private void flushBatch(D dao, List<T> batch) {
        // UPSERT with retry and ACK verification
        int[] results = batchExecutor.executeWithRetry(
            () -> upsertFunction.apply(dao, new ArrayList<>(batch)),
            tableName,
            maxRetryAttempts,
            initialRetryDelayMs,
            maxDelayMs
        );
        accumulator.add(batchExecutor.sumResults(results));
        batch.clear();
    }
}
```

#### 4.4 Custom Serializable Functional Interfaces

To ensure functional interfaces are serializable, custom extensions were created:

```java
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

@FunctionalInterface
public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {}
```

**Why This Matters:**
- Standard Java functional interfaces (`Supplier`, `BiFunction`) are NOT `Serializable`
- When passed to `PartitionWriter` as fields, they must be serializable
- The custom interfaces extend both the functional interface AND `Serializable`

---

### 5. Connection Management

#### 5.1 Driver-Side Pool

**Purpose:** Used only for `BatchStateDao` (reading/writing processed file names).

```yaml
postgres:
  driver-pool:
    size: 4              # Small pool
    pool-name: nimbo-batch-pg-pool
```

#### 5.2 Executor-Side Pool

**Purpose:** Each executor creates its own HikariCP pool for writing aggregate data.

```yaml
postgres:
  executor-pool:
    max-pool-size: 2     # Small pool per executor
    connection-timeout-ms: 5000
    max-lifetime-ms: 180000
    pool-name: JdbiExecutorPool
```

**Pool Sizing Calculation:**
```
Total Connections = SparkExecutors × ExecutorCores × PoolSize
                   = 2 × 2 × 2 = 8 connections
```

**Why Small Pools:**
- Spark parallelism already provides concurrency
- Avoids overwhelming PostgreSQL
- Prevents connection storms on failure

---

### 6. Error Handling & Retry Strategy

#### 6.1 Three-Level Retry Architecture

| Level | Mechanism | Max Attempts | Purpose |
|-------|-----------|--------------|---------|
| **Batch** | `BatchExecutor.executeWithRetry()` | 3-5 | Handle transient DB failures |
| **Partition** | Spark `spark.task.maxFailures` | 4 | Handle executor failures |
| **Job** | `BatchJobRunner` explicit retry | Unlimited | Handle persistent failures |

#### 6.2 BatchExecutor with Exponential Backoff

```java
public int[] executeWithRetry(Supplier<int[]> operation, String tableName,
                              int maxRetries, long initialDelayMs, long maxDelayMs) {
    int attempts = 0;
    while (attempts < maxRetries) {
        try {
            int[] results = operation.get();
            verifyAllRowsAffected(results);  // ACK verification
            return results;
        } catch (Exception e) {
            attempts++;
            if (attempts < maxRetries) {
                long delay = Math.min(
                    initialDelayMs * (long) Math.pow(2.0, attempts - 1),
                    maxDelayMs
                );
                sleep(delay);  // Exponential backoff with cap
            }
        }
    }
    throw new RuntimeException("Failed after " + maxRetries + " attempts");
}
```

#### 6.3 ACK Verification

```java
private void verifyAllRowsAffected(int[] results) {
    for (int result : results) {
        if (result != 1) {
            throw new RuntimeException("expected 1 row affected, got " + result);
        }
    }
}
```

**Why This Matters:**
- JDBI batch returns `int[]` where each int is rows affected
- `0` means no row was inserted/updated (shouldn't happen with UPSERT)
- `>1` would indicate a constraint violation
- Throwing ensures retry mechanism is triggered

---

### 7. Idempotency & Exactly-Once Semantics

#### 7.1 UPSERT with batch_id

All aggregate tables use `batch_id` in their primary key:

```sql
-- Traffic Aggregates
PRIMARY KEY (src_ip, window_start, batch_id)

-- Connection Aggregates  
PRIMARY KEY (id, window_start, batch_id)

-- Protocol Aggregates
PRIMARY KEY (l7_protocol, window_start, batch_id)

-- IP Bandwidth Aggregates
PRIMARY KEY (ip, window_start, batch_id)
```

#### 7.2 UPSERT SQL Pattern

```sql
INSERT INTO traffic_aggregates (src_ip, window_start, window_end, 
    in_bytes, out_bytes, ...)
VALUES (:srcIp, :windowStart, :windowEnd, :inBytes, :outBytes, ...)
ON CONFLICT (src_ip, window_start, batch_id) DO UPDATE SET 
    window_end = EXCLUDED.window_end,
    in_bytes = EXCLUDED.in_bytes,
    out_bytes = EXCLUDED.out_bytes,
    ...
```

**Why This Guarantees Idempotency:**
- Same `batch_id` + same data → UPDATE, not INSERT
- Retrying a failed partition writes the same data with the same `batch_id`
- No duplicate rows across retries

#### 7.3 Batch ID Reuse on Failure

```java
UUID batchId = fileDiscoveryService.getFailedBatchId();
if (batchId != null) {
    log.info("Reusing failed batch: {}", batchId);
} else {
    batchId = UUID.randomUUID();
}
```

**Flow:**
1. Batch fails → `batch_id` stored in `FileDiscoveryService`
2. Files NOT marked as processed
3. Next run reuses the same `batch_id`
4. UPSERT overwrites incomplete data with complete data
5. No orphans, no duplicates

---

### 8. Configuration Evolution

#### 8.1 Before: Mixed Config with Hardcoded Values

```yaml
postgres:
  pool:
    size: 20            # Hardcoded
    # No executor pool config
```

#### 8.2 After: Explicit Driver vs Executor Pools

```yaml
postgres:
  driver-pool:          # Driver-side (BatchStateDao + Flyway)
    size: 4
    connection-timeout: 10000
    pool-name: nimbo-batch-pg-pool

  executor-pool:        # Executor-side (writing aggregates)
    max-pool-size: ${EXECUTOR_POOL_SIZE:2}
    connection-timeout-ms: 5000
    idle-timeout-ms: 60000
    max-lifetime-ms: 180000
    pool-name: JdbiExecutorPool
```

---

### 9. Summary of Solved Problems

| Problem | Solution |
|---------|----------|
| **Driver memory exhaustion** | Moved writes to executors via `foreachPartition()` |
| **HikariDataSource serialization** | Lazy initialization pattern with `transient` |
| **Lambda serialization** | Extracted to `PartitionWriter` serializable class |
| **Functional interface serialization** | Created `SerializableSupplier`, `SerializableBiFunction` |
| **Idempotency on retry** | `batch_id` in PRIMARY KEY + UPSERT |
| **Connection management** | Separate pools for driver and executors |
| **Retry without duplicates** | Exponential backoff + ACK verification |

---

### 10. Final Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FINAL PRODUCTION ARCHITECTURE                           │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      DRIVER (Spring)                                 │   │
│  │  ┌───────────────────────────────────────────────────────────────┐  │   │
│  │  │  BatchJobRunner                                               │  │   │
│  │  │  - Generates/reuses batch_id                                  │  │   │
│  │  │  - Orchestrates 4 processors                                  │  │   │
│  │  │  - Marks files processed ONLY after all succeed              │  │   │
│  │  └───────────────────────────────────────────────────────────────┘  │   │
│  │                              │                                      │   │
│  │  ┌───────────────────────────┼───────────────────────────────┐     │   │
│  │  │  PostgresWriter           │  JdbiDaoFactory (Serializable)│     │   │
│  │  │  - Creates PartitionWriter│  - jdbcUrl, username, pass    │     │   │
│  │  │  - foreachPartition()     │  - transient DataSource       │     │   │
│  │  └───────────────────────────┼───────────────────────────────┘     │   │
│  └──────────────────────────────┼─────────────────────────────────────┘   │
│                                  │                                          │
│                    ┌─────────────┼─────────────┐                           │
│                    │ Serialize   │             │                           │
│                    ▼             ▼             ▼                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                 SPARK EXECUTORS (Parallel Writes)                   │   │
│  │                                                                     │   │
│  │  PartitionWriter<T, D> implements Serializable                     │   │
│  │  - daoSupplier: SerializableSupplier<D>                           │   │
│  │  - upsertFunction: SerializableBiFunction                         │   │
│  │  - rowMapper: RowMapper<T>                                        │   │
│  │  - batchId: UUID                                                  │   │
│  │  - ingestedAt: Timestamp                                          │   │
│  │  - batchExecutor: BatchExecutor (Serializable)                   │   │
│  │  - accumulator: LongAccumulator                                   │   │
│  │                                                                     │   │
│  │  call(Iterator<Row>):                                              │   │
│  │    D dao = daoSupplier.get()     ← Lazy init on executor          │   │
│  │    while (rows) {                 ← Accumulate batch              │   │
│  │      batch.add(rowMapper.map())                                   │   │
│  │      if batch.size() >= batchSize:                                │   │
│  │        flushBatch(dao, batch)    ← UPSERT with retry             │   │
│  │    }                                                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                  │                                          │
│                                  ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      POSTGRESQL                                     │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │  UPSERT with batch_id in PRIMARY KEY                        │   │   │
│  │  │  - traffic_aggregates                                       │   │   │
│  │  │  - connection_aggregates                                    │   │   │
│  │  │  - protocol_usage_aggregates                                │   │   │
│  │  │  - ip_bandwidth_aggregates                                  │   │   │
│  │  │  - batch_processor_state (driver only)                     │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 11. Key Lessons Learned

| Lesson | Application |
|--------|-------------|
| **Know your serialization boundary** | Any object crossing from driver to executor must be `Serializable` |
| **Lazy initialization is your friend** | Recreate non-serializable resources on executors from config |
| **Functional interfaces are not Serializable by default** | Create custom extensions or extract to classes |
| **Idempotency is non-negotiable** | `batch_id` in PRIMARY KEY with UPSERT enables safe retries |
| **Connection pools belong on executors** | Each executor creates its own pool with small size |
| **ACK verification prevents silent failures** | Verify every batch write |
| **Exponential backoff prevents cascading failures** | With max cap to avoid unbounded delays |

---

### 12. Conclusion

The transition from driver-centric to executor-centric database writing transformed the Nimbo batch-processor into a truly distributed, scalable system. By addressing serialization challenges through lazy initialization, custom serializable interfaces, and extracted partition writers, the system now scales horizontally across Spark executors without driver memory bottlenecks.

The combination of:
- **`batch_id`-based UPSERT** for idempotency
- **Three-level retry strategy** for resilience
- **ACK verification** for data integrity
- **Separate connection pools** for driver and executors

Ensures the system is production-ready with zero data loss guarantees and exactly-once processing semantics.

---

*Report Date: September 2026*
*Author: Nimbo Batch Team*