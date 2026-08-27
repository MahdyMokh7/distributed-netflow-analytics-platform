### JDBC vs Spark's JDBC Writer

| Approach | What We Use | Why |
|----------|-------------|-----|
| **Spark's JDBC writer** | `.write().jdbc(...)` | ❌ Less control over batching, upsert, error handling |
| **Hand-rolled JDBC** | `Connection`, `PreparedStatement` | ✅ Full control over everything |

**We use hand-rolled JDBC because:**
1. ✅ Custom upsert logic (`ON CONFLICT`)
2. ✅ Fine-grained batch control
3. ✅ Custom retry logic
4. ✅ Transaction control per batch
5. ✅ `batch_id` injection


## The Upsert Problem - Explained

### What Upsert Does (Current Implementation)

**Upsert = INSERT + ON CONFLICT DO UPDATE**

```sql
INSERT INTO traffic_aggregates (src_ip, window_start, in_bytes, ...) 
VALUES (?, ?, ?, ...)
ON CONFLICT (src_ip, window_start) 
DO UPDATE SET 
    in_bytes = EXCLUDED.in_bytes,
    out_bytes = EXCLUDED.out_bytes,
    ...
```

**Why We Need It:**
- If the same record already exists, UPDATE it instead of failing
- Makes writes **idempotent** - running the same batch twice doesn't create duplicates
- Essential for retry logic and at-least-once processing

---

### Why Spark's JDBC Writer Can't Do Upsert

| Spark JDBC Writer Feature | Supports Upsert? |
|---------------------------|------------------|
| `dataset.write().jdbc()` | ❌ NO |
| `SaveMode.Append` | Only INSERT (fails on duplicate) |
| `SaveMode.Overwrite` | DROP + CREATE (loses all data!) |
| `SaveMode.Ignore` | Skip if table exists (not per row) |
| `SaveMode.ErrorIfExists` | Fail if table exists (not per row) |

**Conclusion:** Spark's built-in JDBC writer **does NOT support `ON CONFLICT`**.

---

## Our Solution: Hybrid Approach

### Option 1: Keep PostgresWriter + Use Spark's Writer Internally (Recommended)

**Strategy:** Keep `PostgresWriter` as a wrapper, but delegate the actual writing to Spark's JDBC writer, then add a custom "upsert" step.

```java
public void write(Dataset<Row> dataset, TableMapping mapping, UUID batchId) {
    // 1. Add audit columns
    Dataset<Row> withAudit = dataset
        .withColumn("batch_id", lit(batchId.toString()))
        .withColumn("ingested_at", lit(Timestamp.from(Instant.now())));
    
    // 2. Use Spark's JDBC writer to insert into a temporary table
    withAudit.write()
        .mode(SaveMode.Append)
        .jdbc(url, mapping.getTableName() + "_tmp", props);
    
    // 3. Run UPSERT via JDBC (from temp table to main table)
    try (Connection conn = dataSource.getConnection()) {
        String upsertSql = buildUpsertSql(mapping);
        conn.prepareStatement(
            "INSERT INTO " + mapping.getTableName() + 
            " SELECT * FROM " + mapping.getTableName() + "_tmp" +
            " ON CONFLICT (" + conflictKeys + ") DO UPDATE SET ..."
        ).execute();
    }
}
```

**Pros:** ✅ Spark handles all type mapping, ✅ We keep upsert
**Cons:** ❌ Requires temporary table, ❌ Two-phase write (more complex)

---

### Option 2: Use Spark's JDBC Writer with a Custom `INSERT` Approach

**Strategy:** Write directly with a custom SQL insert that includes `ON CONFLICT`.

```java
public void write(Dataset<Row> dataset, TableMapping mapping, UUID batchId) {
    // 1. Add audit columns
    Dataset<Row> withAudit = dataset
        .withColumn("batch_id", lit(batchId.toString()))
        .withColumn("ingested_at", lit(Timestamp.from(Instant.now())));
    
    // 2. Use Spark's JDBC writer with a custom INSERT
    //     - This requires a custom JDBC connection
    //     - Or using foreachPartition() with manual JDBC
    withAudit.foreachPartition((Iterator<Row> partition) -> {
        // Manual JDBC with upsert per partition
        // But Spark handles the row -> JDBC mapping for us?
        // Actually, foreachPartition means we're back to manual mapping
    });
}
```

**Pros:** ✅ Single phase, ✅ No temp table
**Cons:** ❌ Back to manual mapping (defeats the purpose)

---

### Option 3: Keep Current PostgresWriter + Remove TableMapper (Recommended)

**Strategy:** Keep `PostgresWriter` as-is but simplify `TableMapper` using **Spark's built-in `Row` → `PreparedStatement` mapping**.

Wait... Spark **does not** have a built-in Row → PreparedStatement mapper. This is what `TableMapper` does.

**But we CAN use `dataset.collectAsList()` or `dataset.toLocalIterator()` and use a library like JDBI or MyBatis.**

---

### Option 4: Use a Batch ORM Library (Most Production-Grade)

**Strategy:** Use JDBI or MyBatis for type-safe mapping with upsert support.

```java
// Use JDBI's ORM-like mapping
@SqlUpdate("INSERT INTO traffic_aggregates (...) VALUES (...) ON CONFLICT (src_ip, window_start) DO UPDATE SET ...")
void upsertTrafficAggregate(@BindBean TrafficAggregate aggregate);
```

**Pros:** ✅ Clean, ✅ Type-safe, ✅ Full upsert support
**Cons:** ❌ Adds a new dependency, ❌ Requires domain objects

---

## My Recommendation: Option 4 (Production-Grade)

### Why This is the Best Senior-Level Solution

| Aspect | Why |
|--------|-----|
| **Type Safety** | JDBI uses Java objects, not Spark Rows |
| **Maintainability** | Clean separation of concerns |
| **Testability** | Can test JDBC logic without Spark |
| **Upsert Support** | Full `ON CONFLICT` support |
| **Performance** | Batch inserts via JDBI's batch API |
| **Simplicity** | No more 500 lines of mapping code |

### Implementation Approach

```java
public class PostgresWriter {
    private final Jdbi jdbi;
    private final TrafficAggregateDao trafficDao;
    
    public long write(Dataset<Row> dataset, TableMapping mapping, UUID batchId) {
        // 1. Collect data from Spark (small data per window)
        Iterator<Row> rows = dataset.toLocalIterator();
        
        // 2. Convert Spark Row → Domain Object
        List<TrafficAggregate> aggregates = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            aggregates.add(TrafficAggregate.fromRow(row, batchId));
        }
        
        // 3. Batch insert with upsert using JDBI
        return trafficDao.upsertBatch(aggregates);
    }
}
```

### Why This Works with Your Data Volume

| Data Volume | How JDBI Handles It |
|-------------|---------------------|
| **247,700 records** (from your log) | Batched inserts (e.g., 1000 per batch) |
| **29,917,488 records** (connection aggregates) | Streaming insert via JDBI's batch API |
| **Memory** | No OOM - JDBI streams the data |

---

## Summary Table

| Option | Complexity | Type Safety | Upsert Support | Code Volume | Recommendation |
|--------|------------|-------------|----------------|-------------|----------------|
| 1. Temp Table + Spark Writer | High | ✅ | ✅ | Medium | ⚠️ Complex |
| 2. Spark + Custom INSERT | High | ❌ | ✅ | High | ❌ |
| 3. Keep Current Writer | Low | ❌ | ✅ | Low | ⚠️ Fragile |
| 4. **JDBI ORM** | Medium | ✅ | ✅ | Low | ✅ **BEST** |

---

## Questions for You

1. **Are you open to adding a new dependency?** (JDBI or MyBatis)

2. **Would you prefer to create domain objects** (e.g., `TrafficAggregate`) and map Spark Rows to them?

3. **What's your opinion on Option 4 vs keeping the current approach?**

4. **Should I provide a detailed implementation plan for JDBI?**

---

## Files to Send

To proceed with the plan, please send:
- `PostgresWriter.java`
- `TableMapper.java`
- `pom.xml`

Once I have these, I can give you a complete, production-grade implementation. 🚀