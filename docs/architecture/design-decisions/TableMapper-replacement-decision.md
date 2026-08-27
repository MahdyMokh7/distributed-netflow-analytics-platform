# Topic 1: TableMapper Replacement - Options Analysis

## Executive Summary

We need to replace the current manual `TableMapper` (500+ lines of fragile type-mapping code) with a production-grade solution. Four options were evaluated based on complexity, maintainability, performance, and data safety.

---

## Option 1: Keep Manual TableMapper (Current)

### What It Is
- Manual mapping of Spark `Row` to JDBC `PreparedStatement`
- 100+ lines of `switch` statements for type conversion
- Hardcoded column names and types
- Manual batch management and upsert logic

### Pros
| Pro | Description |
|-----|-------------|
| ✅ No new dependencies | Uses only JDBC and Spark |
| ✅ Full control | Everything is explicit and customizable |
| ✅ Works now | Already functional in production |

### Cons
| Con | Description | Severity |
|-----|-------------|----------|
| ❌ **Fragile** | Every new table requires manual mapping | 🔴 HIGH |
| ❌ **No type safety** | Column names are hardcoded strings | 🔴 HIGH |
| ❌ **Verbose** | 500+ lines for what Spark can do in 1 line | 🟡 MEDIUM |
| ❌ **Bug-prone** | Manual type conversion is error-prone | 🔴 HIGH |
| ❌ **Maintenance nightmare** | Changes require modifying multiple files | 🔴 HIGH |

---

## Option 2: Spark's Built-in JDBC Writer

### What It Is
```text(java)
dataset.write()
    .mode(SaveMode.Append)
    .jdbc(url, tableName, connectionProperties);
```

### Pros
| Pro | Description |
|-----|-------------|
| ✅ **1 line of code** | Replaces 500+ lines |
| ✅ **Type-safe** | Spark handles all type conversion automatically |
| ✅ **Battle-tested** | Used by thousands of Spark applications |
| ✅ **No new dependencies** | Already part of Spark |
| ✅ **Performance optimized** | Spark uses efficient JDBC batching |

### Cons
| Con | Description | Severity |
|-----|-------------|----------|
| ❌ **No UPSERT support** | Only simple INSERT, no `ON CONFLICT` | 🔴 HIGH |
| ❌ **No idempotency** | Re-running duplicates data | 🔴 HIGH |
| ❌ **No retry/ACK** | No built-in verification | 🟡 MEDIUM |
| ❌ **Limited control** | Can't customize per-row logic | 🟡 MEDIUM |

---

## Option 3: Temp Table + Spark JDBC Writer

### What It Is
1. Spark writes to a temporary table using `dataset.write().jdbc()`
2. Then execute `INSERT INTO main_table SELECT * FROM temp_table ON CONFLICT ...`

### Pros
| Pro | Description |
|-----|-------------|
| ✅ **Uses Spark's JDBC writer** | No manual mapping |
| ✅ **Type-safe** | Spark handles types |
| ✅ **Upsert support** | Via temp table + INSERT/UPDATE |
| ✅ **No new dependencies** | Uses only Spark + JDBC |

### Cons
| Con | Description | Severity |
|-----|-------------|----------|
| ❌ **Two-phase write** | Data written twice (temp + main) | 🟡 MEDIUM |
| ❌ **Two transactions** | If UPSERT fails, temp data remains | 🔴 HIGH |
| ❌ **Temp table management** | Must create/drop per run | 🟡 MEDIUM |
| ❌ **Concurrency issues** | Multiple jobs may conflict on temp tables | 🟡 MEDIUM |
| ❌ **Complex error handling** | Recovery requires manual cleanup | 🔴 HIGH |
| ❌ **Performance overhead** | Data written twice | 🟡 MEDIUM |

---

## Option 4: JDBI/ORM (Recommended)

### What It Is
- Lightweight ORM with declarative SQL
- Type-safe mapping from Java objects to SQL
- Built-in batch support with `@SqlBatch`
- Native `ON CONFLICT` support

### Pros
| Pro | Description |
|-----|-------------|
| ✅ **Single-phase write** | One transaction, atomic operation | 🟢 BEST |
| ✅ **Clean DAO pattern** | Separation of concerns, testable | 🟢 BEST |
| ✅ **Type-safe** | JDBI handles JDBC mapping | 🟢 BEST |
| ✅ **Native UPSERT** | Full `ON CONFLICT` support | 🟢 BEST |
| ✅ **Batch support** | Built-in `@SqlBatch` for performance | 🟢 BEST |
| ✅ **No temp tables** | No cleanup or concurrency issues | 🟢 BEST |
| ✅ **Idempotent** | UPSERT = safe to re-run | 🟢 BEST |
| ✅ **ACK verification** | Batch returns affected row counts | 🟢 BEST |

### Cons
| Con | Description | Severity |
|-----|-------------|----------|
| ⚠️ **Adds new dependency** | JDBI jar (~2MB) | 🟢 LOW |
| ⚠️ **Requires domain models** | Must create POJOs for each table | 🟢 LOW |
| ⚠️ **Row → Object conversion** | Must convert Spark Row to POJO | 🟢 LOW |

---

## Side-by-Side Comparison

| Requirement | Option 1 (Current) | Option 2 (Spark Writer) | Option 3 (Temp Table) | Option 4 (JDBI) |
|-------------|:---:|:---:|:---:|:---:|
| **No manual type mapping** | ❌ | ✅ | ✅ | ✅ |
| **Upsert support** | ✅ | ❌ | ✅ | ✅ |
| **Single transaction** | ✅ | ✅ | ❌ | ✅ |
| **Type safety** | ❌ | ✅ | ✅ | ✅ |
| **Idempotent writes** | ✅ | ❌ | ✅ | ✅ |
| **ACK verification** | ❌ | ❌ | ⚠️ | ✅ |
| **Code volume** | 500+ lines | 1 line | 50 lines | 100 lines |
| **Maintainability** | 🔴 Poor | 🟢 Excellent | 🟡 Good | 🟢 Excellent |
| **Testability** | 🔴 Hard | 🟢 Easy | 🟡 Medium | 🟢 Easy |
| **New dependencies** | None | None | None | JDBI (lightweight) |
| **Production-ready** | ⚠️ Fragile | ⚠️ No upsert | ⚠️ Complex | ✅ Yes |
| **Zero data loss guarantee** | ⚠️ Partial | ❌ | ⚠️ Recovery needed | ✅ Yes |

---

## Decision: Why We Chose Option 4 (JDBI)

### Primary Reason: Zero Data Loss Guarantee

| Concern | How JDBI Solves It |
|---------|-------------------|
| **Atomicity** | Single transaction = all or nothing |
| **ACK verification** | Batch returns affected row counts |
| **No temp tables** | No partial data states |
| **UPSERT** | Idempotent = safe to re-run |

### Secondary Reasons

| Reason | Explanation |
|--------|-------------|
| **Clean Architecture** | DAO pattern follows SOLID principles |
| **Production Standards** | Used by Netflix, Airbnb, and other big data companies |
| **Maintainability** | 100 lines vs 500+ lines |
| **Testability** | Easy to mock DAOs |
| **Scalability** | JDBI batch handles millions of rows |
| **No Temp Table Complexity** | No cleanup, no concurrency conflicts |

### The Senior Engineering Choice

```
Option 4 is the choice because:
1. It guarantees zero data loss (atomic transactions + ACK)
2. It's maintainable (clean DAO pattern)
3. It's production-grade (used in production at scale)
4. It eliminates 400+ lines of fragile code
5. It adds a lightweight dependency (JDBI) instead of complex hacks
```

---

## Implementation Impact Summary

| What Changes | What Stays |
|--------------|------------|
| `PostgresWriter.java` - Major rewrite | `TableSchema.java` - Stays |
| `TableMapper.java` - **DELETE** | `TableMapping.java` - Stays (simplified) |
| `TableMapperTest.java` - **DELETE** | `ColumnTypeConstants.java` - Stays |
| **New:** Domain model classes | All processor classes - No change |
| **New:** JDBI DAO interfaces | `BaseProcessor.java` - Minor change |
| **New:** JDBI dependency in `pom.xml` | Retry/backoff logic - Stays |
| `PostgresWriterTest.java` - Rewrite | Configuration - Stays |

---

## Final Recommendation

**Option 4 (JDBI) is the production-grade, senior-level choice** because it:

1. ✅ Eliminates manual type mapping (500+ lines of fragile code)
2. ✅ Guarantees zero data loss (atomic transactions + ACK)
3. ✅ Maintains idempotency (UPSERT support)
4. ✅ Follows clean architecture (DAO pattern)
5. ✅ Is maintainable and testable
6. ✅ Is battle-tested in production environments

**Ready to proceed with implementation.** 🚀