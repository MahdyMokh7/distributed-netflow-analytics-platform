# 📚 Complete Guide to JUnit 5 Extension Lifecycle & Your Test Suite

Let me explain the JUnit 5 extension lifecycle first (critical for understanding your tests), then break down each test and each extension.

---

## 🎯 JUnit 5 Extension Lifecycle (Foundation)

### The Complete Lifecycle Order

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    JUNIT 5 EXTENSION LIFECYCLE                                 │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  PHASE 1: CLASS-LEVEL (BeforeAll)                                      │   │
│  │                                                                         │   │
│  │  1. BeforeAllCallback.beforeAll()                                      │   │
│  │     ├── Runs ONCE per test class                                      │   │
│  │     ├── Runs BEFORE any @BeforeAll methods                            │   │
│  │     ├── Runs BEFORE the Spring context is loaded (for extensions)     │   │
│  │     └── Used to: Start containers, create Spark sessions, etc.        │   │
│  │                                                                         │   │
│  │  2. @BeforeAll static methods                                          │   │
│  │     └── Runs ONCE per test class                                      │   │
│  │                                                                         │   │
│  │  3. BeforeEachCallback.beforeEach()                                    │   │
│  │     ├── Runs BEFORE each test method                                  │   │
│  │     └── Used to: Reset state, clear caches                            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                            │
│                                     ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  PHASE 2: TEST-LEVEL (Per Test)                                        │   │
│  │                                                                         │   │
│  │  4. @BeforeEach methods                                                 │   │
│  │     └── Runs BEFORE each test method                                  │   │
│  │                                                                         │   │
│  │  5. @Test method                                                       │   │
│  │     └── The actual test logic                                         │   │
│  │                                                                         │   │
│  │  6. @AfterEach methods                                                 │   │
│  │     └── Runs AFTER each test method                                   │   │
│  │                                                                         │   │
│  │  7. AfterEachCallback.afterEach()                                      │   │
│  │     └── Runs AFTER each test method                                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                            │
│                                     ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  PHASE 3: CLASS-LEVEL (AfterAll)                                       │   │
│  │                                                                         │   │
│  │  8. AfterAllCallback.afterAll()                                        │   │
│  │     └── Runs ONCE per test class                                      │   │
│  │                                                                         │   │
│  │  9. @AfterAll static methods                                           │   │
│  │     └── Runs ONCE per test class                                      │   │
│  │                                                                         │   │
│  │  10. ExtensionContext.Store.CloseableResource.close()                  │   │
│  │      ├── Runs when the Store is closed                                │   │
│  │      ├── Happens AFTER all tests complete                             │   │
│  │      └── Used to: Stop containers, close Spark session                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 The Critical Concept: `ExtensionContext.Store`

The `ExtensionContext.Store` is JUnit 5's mechanism for **sharing state across tests** in a test class (or across the entire test suite).

### Store Scopes

| Scope | How to Get It | Lifetime |
|-------|---------------|----------|
| **Test Method** | `context.getStore(NAMESPACE)` | Single test method |
| **Test Class** | `context.getStore(NAMESPACE)` | Single test class |
| **Root (JVM)** | `context.getRoot().getStore(NAMESPACE)` | Entire test suite (all classes) |

### Why `context.getRoot().getStore()` Matters

```java
// This gets the ROOT store (shared across ALL test classes)
database = context.getRoot().getStore(NAMESPACE)
        .getOrComputeIfAbsent(DATABASE_KEY, ignored -> SharedPostgresHolder.INSTANCE, SharedPostgres.class);
```

**Key insight:** `getRoot()` returns the **JVM-level store**. This means:
- ✅ `PostgresContainerExtension` in `ConnectionProcessorIT` and `PostgresWriterIT` share **THE SAME** PostgreSQL container
- ✅ The container starts once, reused by all tests
- ✅ Stopped only when the JVM exits

---

## 🧩 Extension 1: `PostgresContainerExtension`

### What It Does

Manages a **shared PostgreSQL 16.2 container** for the entire test suite.

### Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    PostgresContainerExtension LIFECYCLE                        │
│                                                                                 │
│  TEST CLASS 1 (ConnectionProcessorIT)                                          │
│  │                                                                             │
│  │  beforeAll()                                                                │
│  │  ├── Check: Is SharedPostgres already in root store?                      │
│  │  │   ├── NO → Create SharedPostgres (starts container)                    │
│  │  │   │       ├── container.start()                                        │
│  │  │   │       ├── Flyway.migrate()                                          │
│  │  │   │       └── Store in root store                                       │
│  │  │   └── YES → Reuse existing container                                    │
│  │  └── Done                                                                  │
│  │                                                                             │
│  │  @BeforeEach                                                                │
│  │  └── resetDatabase() → TRUNCATE tables                                    │
│  │                                                                             │
│  │  Tests run                                                                 │
│  │                                                                             │
│  TEST CLASS 2 (PostgresWriterIT)                                              │
│  │                                                                             │
│  │  beforeAll()                                                                │
│  │  ├── Check: Is SharedPostgres already in root store?                      │
│  │  │   └── YES → Reuse existing container ✅                                  │
│  │  └── Done                                                                  │
│  │                                                                             │
│  │  @BeforeEach                                                                │
│  │  └── resetDatabase() → TRUNCATE tables                                    │
│  │                                                                             │
│  │  Tests run                                                                 │
│  │                                                                             │
│  TEST SUITE END                                                               │
│  │                                                                             │
│  │  SharedPostgres.close()                                                    │
│  │  └── container.stop()                                                      │
│  └── Done                                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Key Methods

| Method | Purpose |
|--------|---------|
| `beforeAll()` | Runs before test class, ensures container is started |
| `jdbi()` | Returns a Jdbi instance connected to the container |
| `jdbcUrl()` | Returns the JDBC URL for Spring configuration |
| `host()` | Returns container host |
| `port()` | Returns mapped port (random) |
| `databaseName()` | Returns "processor_test" |
| `username()` | Returns "processor_user" |
| `password()` | Returns "processor_password" |

### Why `SharedPostgres` is a `CloseableResource`

```java
private static final class SharedPostgres implements ExtensionContext.Store.CloseableResource {
    private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16.2-alpine")...
    
    private SharedPostgres() {
        container.start();
        Flyway.configure()...migrate();
    }
    
    @Override
    public void close() {
        container.stop();  // ← Called when JVM exits
    }
}
```

JUnit 5 automatically calls `close()` when the store is closed (JVM exit).

---

## 🧩 Extension 2: `SparkSessionExtension`

### What It Does

Manages a **shared SparkSession** for the entire test suite.

### Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SparkSessionExtension LIFECYCLE                             │
│                                                                                 │
│  TEST CLASS 1                                                                  │
│  │                                                                             │
│  │  beforeAll()                                                                │
│  │  ├── Check: Is SharedSpark already in root store?                          │
│  │  │   ├── NO → Create SharedSpark                                           │
│  │  │   │       ├── SparkSession.builder().master("local[2]")...getOrCreate()│
│  │  │   │       └── Store in root store                                       │
│  │  │   └── YES → Reuse existing session                                       │
│  │  └── Done                                                                  │
│  │                                                                             │
│  │  Tests run                                                                 │
│  │                                                                             │
│  TEST CLASS 2                                                                  │
│  │                                                                             │
│  │  beforeAll()                                                                │
│  │  └── Reuse existing SparkSession ✅                                         │
│  │                                                                             │
│  TEST SUITE END                                                               │
│  │                                                                             │
│  │  SharedSpark.close()                                                       │
│  │  ├── session.stop()                                                        │
│  │  ├── SparkSession.clearActiveSession()                                     │
│  │  └── SparkSession.clearDefaultSession()                                    │
│  └── Done                                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Key Configuration

```java
SparkSession.builder()
    .master("local[2]")                           // 2 threads for parallelism
    .appName("cube-maker-test-suite")        // Identifiable in UI
    .config("spark.ui.enabled", "false")          // No UI for tests
    .config("spark.sql.shuffle.partitions", "2")  // Small for tests
    .config("spark.sql.session.timeZone", "UTC")  // Deterministic
    .getOrCreate();
```

---

## 🧩 Extension 3: `LocalFileSystemTestConfiguration`

### What It Does

Replaces HDFS with a **local filesystem** for tests (no HDFS container needed).

### How It Works

```java
@Bean
@Primary  // ← Overrides the production FileSystem bean
public FileSystem testFileSystem() throws IOException {
    Configuration conf = new Configuration();
    String uri = hdfsConfig.getUri();
    // If no URI, use temp directory
    if (uri == null || uri.isEmpty()) {
        uri = "file://" + System.getProperty("java.io.tmpdir") + "/nimbo-test/parquet";
    }
    
    // Configure for local filesystem
    conf.set("fs.defaultFS", uri);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    conf.set("fs.hdfs.impl.disable.cache", "true");
    conf.set("fs.file.impl.disable.cache", "true");
    
    return FileSystem.get(URI.create(uri), conf);
}
```

**Key insight:** `@Primary` ensures this bean replaces the production one when Spring loads the test context.

---

## 📝 Test 1: `CubeMakerIT`

### What It Tests

**End-to-end integration test** for the entire cube maker pipeline.

### Test Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    CubeMakerIT FLOW                                       │
│                                                                                 │
│  setUp()                                                                       │
│  ├── Create temp directory (parquetRoot)                                      │
│  ├── Delete existing files                                                    │
│  ├── Clear failed batch state                                                 │
│  └── TRUNCATE all tables (cube_maker_state, traffic_aggregates, ...)    │
│                                                                                 │
│  Test 1: givenPartitionedParquetFiles_whenBatchRuns_thenAllCubesAndFileState   │
│  ├── GIVEN: Generate 5 deterministic Parquet files                            │
│  ├── WHEN: Run full batch job (batchJobRunner.runBatchJob())                  │
│  ├── THEN:                                                                     │
│  │   ├── Verify traffic_aggregates row count matches expected                 │
│  │   ├── Verify connection_aggregates row count matches expected              │
│  │   ├── Verify protocol_usage_aggregates row count matches expected          │
│  │   ├── Verify ip_bandwidth_aggregates row count matches expected            │
│  │   ├── Verify specific IP values are correct                                │
│  │   └── Verify cube_maker_state has 5 files                            │
│                                                                                 │
│  Test 2: givenProcessedAndNewFiles_whenBatchRunsAgain_thenOnlyNewFiles        │
│  ├── GIVEN: 3 files, run batch, then add 2 more files                        │
│  ├── WHEN: Run batch again                                                    │
│  ├── THEN:                                                                    │
│  │   ├── Total processed files = 5                                            │
│  │   └── No duplicates (distinct count = 5)                                   │
│                                                                                 │
│  Test 3: givenMoreFilesThanTheConfiguredMaximum_whenBatchRuns                 │
│  ├── GIVEN: 10 files (more than max-files-per-batch=5)                       │
│  ├── WHEN: Run batch once                                                     │
│  ├── THEN: Only 5 files processed (respects limit)                          │
│                                                                                 │
│  Test 4: givenNoParquetFiles_whenBatchRuns                                   │
│  ├── GIVEN: Empty directory                                                   │
│  ├── WHEN: Run batch                                                          │
│  ├── THEN:                                                                    │
│  │   ├── No files processed (cube_maker_state empty)                    │
│  │   └── No traffic aggregates written                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### The `computeExpectedAggregates()` Method

This is the **most important** part - it simulates Spark's aggregation logic in pure Java to compute expected results.

| Aggregation | Group By | Test Simulates |
|-------------|----------|----------------|
| **Traffic** | `(src_ip, hour_window)` | ✅ Uses `HashMap<String, TrafficAggregate>` |
| **Connection** | `(src_ip, dst_ip, hour_window)` | ✅ Uses `HashMap<String, ConnectionAggregate>` |
| **Protocol** | `(l7_protocol, hour_window)` | ✅ Uses `HashMap<String, ProtocolAggregate>` |
| **Top IP** | `(ip, hour_window)` | ✅ Uses `HashMap<String, TopIpAggregate>` |

**Key: The test computes the exact same grouping that Spark does, using the same deterministic input data.**

---

## 📝 Test 2: `ConnectionProcessorIT`

### What It Tests

**Unit-level integration test** for the `ConnectionProcessor` specifically.

### Test Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    ConnectionProcessorIT FLOW                                  │
│                                                                                 │
│  setUp()                                                                       │
│  ├── Create ProcessorTestFixture (with Postgres + Spark)                      │
│  └── resetDatabase() → TRUNCATE tables                                        │
│                                                                                 │
│  Test 1: givenDifferentPortsAndProtocolsForTheSameIpPair_whenProcessed        │
│  ├── GIVEN: 4 flows with different ports for the same IP pair                │
│  ├── WHEN: Process with ConnectionProcessor                                   │
│  ├── THEN:                                                                    │
│  │   ├── 3 rows in connection_aggregates (3 unique IP pairs)                 │
│  │   ├── 1 row for 10.0.0.1 → 10.0.0.2 (metrics merged)                     │
│  │   ├── in_bytes = 400 (100 + 300)                                          │
│  │   ├── out_bytes = 600 (200 + 400)                                         │
│  │   └── id has 64 characters (SHA-256)                                       │
│                                                                                 │
│  Test 2: givenTheSameBatchTwice_whenProcessed                                 │
│  ├── GIVEN: Same batch ID                                                    │
│  ├── WHEN: Process the same batch twice                                      │
│  ├── THEN: 3 rows (UPSERT, no duplicates) ✅                                  │
│                                                                                 │
│  Test 3: givenNoFlows_whenProcessed                                           │
│  ├── GIVEN: Empty dataset                                                    │
│  ├── WHEN: Process empty flows                                               │
│  ├── THEN: 0 rows in connection_aggregates                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### The `ProcessorTestFixture`

A helper class that:
- Provides a `SparkSession`
- Provides a `Jdbi` instance
- Creates test data (`standardFlows()`, `emptyFlows()`)
- Creates `ConnectionProcessor` with real dependencies

---

## 📝 Test 3: `PostgresWriterIT`

### What It Tests

**Integration test** for the `PostgresWriter` (writes aggregates to PostgreSQL).

### Test Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    PostgresWriterIT FLOW                                       │
│                                                                                 │
│  setUp()                                                                       │
│  ├── Create Spark session                                                    │
│  ├── Create Jdbi instance                                                    │
│  ├── TRUNCATE tables                                                         │
│  ├── Create SimpleMeterRegistry (for metrics)                                │
│  └── Create PostgresWriter with real DAOs                                    │
│                                                                                 │
│  Test 1: givenThreeTrafficRowsAcrossTwoJdbcBatches_whenWritten                │
│  ├── GIVEN: 3 traffic aggregate rows, batch size = 2                          │
│  ├── WHEN: writeTrafficAggregates()                                           │
│  ├── THEN:                                                                    │
│  │   ├── 3 rows written                                                       │
│  │   ├── 3 rows in database                                                   │
│  │   ├── SUM(in_bytes) = 600                                                 │
│  │   ├── SUM(record_count) = 6                                               │
│  │   └── Metrics: cube.maker.output.rows = 3.0                          │
│                                                                                 │
│  Test 2: givenTheSameConnectionBatchTwice_whenWritten                         │
│  ├── GIVEN: Same batch ID                                                    │
│  ├── WHEN: Write the same batch twice                                        │
│  ├── THEN:                                                                    │
│  │   ├── 1 row (UPSERT)                                                       │
│  │   ├── in_bytes = 400                                                       │
│  │   └── out_packets = 6                                                      │
│                                                                                 │
│  Test 3: givenProtocolAndIpBandwidthRows_whenWritten                          │
│  ├── GIVEN: Protocol and bandwidth rows                                      │
│  ├── WHEN: Write both                                                        │
│  ├── THEN:                                                                    │
│  │   ├── protocol_usage_aggregates has 1 row                                  │
│  │   ├── ip_bandwidth_aggregates has 1 row                                    │
│  │   ├── connection_count = 2                                                 │
│  │   └── inbound_bytes = 900                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Key Aspects Tested

| Aspect | How It's Tested |
|--------|-----------------|
| **Batching** | 3 rows with batch size 2 (tests multiple JDBC batches) |
| **UPSERT idempotency** | Same batch written twice |
| **All 4 DAOs** | Traffic, connection, protocol, bandwidth |
| **Metrics** | SimpleMeterRegistry captures counters and timers |
| **Spark partitions** | `repartition(1)` ensures predictable testing |

---

## 🎯 Extension Lifecycle Summary

| Extension | Scope | When It Runs | What It Does |
|-----------|-------|--------------|--------------|
| `PostgresContainerExtension` | Root (JVM) | `beforeAll` | Starts PostgreSQL container, runs Flyway |
| `SparkSessionExtension` | Root (JVM) | `beforeAll` | Creates SparkSession |
| `LocalFileSystemTestConfiguration` | Test context | Context load | Replaces HDFS with local FS |

### The Power of `context.getRoot().getStore()`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SHARED RESOURCES ACROSS TEST CLASSES                        │
│                                                                                 │
│  JVM starts                                                                    │
│  │                                                                             │
│  ├── CubeMakerIT.beforeAll()                                             │
│  │   ├── PostgresContainerExtension: creates container (started)             │
│  │   └── SparkSessionExtension: creates Spark session                        │
│  │                                                                             │
│  ├── CubeMakerIT tests run                                              │
│  │                                                                             │
│  ├── ConnectionProcessorIT.beforeAll()                                       │
│  │   ├── PostgresContainerExtension: reuses existing container ✅            │
│  │   └── SparkSessionExtension: reuses existing session ✅                   │
│  │                                                                             │
│  ├── ConnectionProcessorIT tests run                                         │
│  │                                                                             │
│  ├── PostgresWriterIT.beforeAll()                                            │
│  │   ├── PostgresContainerExtension: reuses existing container ✅            │
│  │   └── SparkSessionExtension: reuses existing session ✅                   │
│  │                                                                             │
│  ├── PostgresWriterIT tests run                                              │
│  │                                                                             │
│  JVM exits                                                                     │
│  │                                                                             │
│  ├── PostgresContainerExtension.close() → container.stop()                   │
│  └── SparkSessionExtension.close() → session.stop()                          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Result:** Only ONE PostgreSQL container and ONE Spark session for the entire test suite, no matter how many test classes.

---

## ✅ Summary

| Aspect | Purpose |
|--------|---------|
| **JUnit 5 Extensions** | Manage shared resources (containers, Spark, filesystem) |
| **`context.getRoot().getStore()`** | Shares resources across all test classes in the JVM |
| **`CloseableResource.close()`** | Cleanup when JVM exits |
| **`@RegisterExtension`** | Registers extension on a field |
| **`@SpringBootTest`** | Loads Spring context with test config |
| **`@DynamicPropertySource`** | Injects container URLs into Spring config |
| **`@TempDir`** | Provides isolated temp directory per test |

**Your test suite is production-grade** with proper resource sharing, deterministic data, and comprehensive coverage. 🚀

# 🎯 `local[2]` vs `local[*]` in Spark

## 📋 Quick Answer

| Mode | Meaning | Threads |
|------|---------|---------|
| **`local[2]`** | Run locally with **exactly 2 threads** | 2 cores used |
| **`local[*]`** | Run locally with **all available cores** | All CPU cores |

---

## 🔍 Detailed Comparison

### `local[2]`

```java
SparkSession.builder()
    .master("local[2]")
    .appName("cube-maker-test-suite")
    .getOrCreate();
```

| Aspect | Value |
|--------|-------|
| **Threads** | Exactly **2** |
| **Cores used** | 2 |
| **CPU usage** | Predictable, bounded |
| **Best for** | Tests (deterministic, fast) |

### `local[*]`

```java
SparkSession.builder()
    .master("local[*]")
    .appName("cube-maker-test-suite")
    .getOrCreate();
```

| Aspect | Value |
|--------|-------|
| **Threads** | All available cores |
| **Cores used** | Depends on machine (e.g., 8, 16, 32) |
| **CPU usage** | High, unpredictable |
| **Best for** | Development, quick experiments |

---

## 📊 Visual Comparison

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    local[2] vs local[*]                                        │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Machine with 8 CPU cores                                              │   │
│  │                                                                         │   │
│  │  local[2]:                                                              │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐     │   │
│  │  │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │     │   │
│  │  │  1  │ │  2  │ │  3  │ │  4  │ │  5  │ │  6  │ │  7  │ │  8  │     │   │
│  │  └──┬──┘ └──┬──┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘     │   │
│  │     │       │                                                           │   │
│  │     └───────┴── Used by Spark (2 threads)                               │   │
│  │                                                                         │   │
│  │  local[*]:                                                              │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐     │   │
│  │  │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │ │ CPU │     │   │
│  │  │  1  │ │  2  │ │  3  │ │  4  │ │  5  │ │  6  │ │  7  │ │  8  │     │   │
│  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘     │   │
│  │     │       │       │       │       │       │       │       │          │   │
│  │     └───────┴───────┴───────┴───────┴───────┴───────┴───────┘          │   │
│  │                     Used by Spark (all 8 threads)                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Why `local[2]` in Your Tests?

### 1. **Determinism**

```java
// With local[2], always 2 partitions
// With local[*] on 8-core machine, could be 8 partitions
```

**Why it matters:** Your tests assert on specific row counts. If the number of partitions changes based on the machine, tests become **non-deterministic**.

### 2. **Resource Isolation**

```
With local[*]:
- Spark uses ALL CPU cores
- Other processes (Docker, IDE, OS) compete for CPU
- Tests could interfere with each other

With local[2]:
- Spark uses only 2 cores
- Other processes have 6+ cores
- More stable test environment
```

### 3. **Reproducibility Across Machines**

| Machine | `local[*]` | `local[2]` |
|---------|-----------|-----------|
| **Developer laptop (4 cores)** | 4 threads | 2 threads |
| **CI server (16 cores)** | 16 threads | 2 threads |
| **Your machine (8 cores)** | 8 threads | 2 threads |

**With `local[2]`, behavior is the same everywhere.** With `local[*]`, it changes.

### 4. **Faster Test Startup**

```
local[*] on 16-core machine:
- Spark creates 16 threads
- More overhead, more memory

local[2]:
- Only 2 threads
- Less overhead
- Faster startup
```

---

## 📊 When to Use Each

| Scenario | Recommended |
|----------|-------------|
| **Unit tests** | `local[2]` |
| **Integration tests** | `local[2]` |
| **CI/CD pipeline** | `local[2]` |
| **Local development** | `local[*]` |
| **Quick experiments** | `local[*]` |
| **Performance testing** | `local[*]` or cluster |

---

## 🔍 Code Example: Your Config

```java
// In SparkSessionExtension.java
SparkSession.builder()
    .master("local[2]")                              // ← 2 threads exactly
    .appName("cube-maker-test-suite")           // Identifiable in logs
    .config("spark.ui.enabled", "false")             // No UI
    .config("spark.sql.shuffle.partitions", "2")     // 2 partitions for shuffle
    .config("spark.sql.session.timeZone", "UTC")     // UTC timezone
    .getOrCreate();
```

**Notice:** `local[2]` matches `shuffle.partitions=2`. **Consistency!**

---

## ✅ Summary

| Aspect | `local[2]` | `local[*]` |
|--------|-----------|-----------|
| **Threads** | 2 (fixed) | All cores |
| **Determinism** | ✅ High | ❌ Machine-dependent |
| **Resource usage** | Bounded | Can saturate CPU |
| **Test stability** | ✅ High | ⚠️ Variable |
| **Best for** | Tests, CI | Development |
| **Reproducibility** | ✅ Same everywhere | ❌ Varies by machine |

**For tests, always use `local[2]` (or `local[N]`) — never `local[*]`.** 🚀