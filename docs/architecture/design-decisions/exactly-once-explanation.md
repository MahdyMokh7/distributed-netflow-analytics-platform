# 🎯 **Cube Maker: From At-Least-Once to Exactly-Once Semantics**

## 📋 **Executive Summary**

We successfully implemented **exactly-once processing** for the cube maker by introducing three key changes: **`batch_id` in the primary key**, **a retry mechanism with batch ID reuse**, and **custom exception propagation**. This ensures that even when a batch fails mid-execution, retrying the same batch produces the exact same final state without duplicates or data loss.

---

## 🔴 **The Problem We Had: At-Least-Once Semantics**

### Original Architecture (At-Least-Once):

```
Batch 1 (SUCCESS):
├── batchId = UUID-1
├── Reads 10 files
├── Processes all data
├── Writes to database
└── Marks files as processed ✅

Batch 2 (PARTIAL FAILURE):
├── batchId = UUID-2 (NEW)
├── Reads 10 files (same ones!)
├── TrafficProcessor: SUCCESS ✅ (writes with UUID-2)
├── ConnectionProcessor: FAILED ❌ (writes partial with UUID-2)
└── Files NOT marked as processed ❌

Batch 3 (RETRY):
├── batchId = UUID-3 (NEW AGAIN!)
├── Reads same 10 files
├── TrafficProcessor: SUCCESS ✅ (writes with UUID-3)
├── ConnectionProcessor: SUCCESS ✅ (writes with UUID-3)
└── Files marked as processed ✅

RESULT:
├── UUID-1 rows (from successful Batch 1) ✅
├── UUID-2 rows (partial, from failed Batch 2) ❌ LEFTOVER
└── UUID-3 rows (complete, from retry Batch 3) ✅

→ We have DUPLICATE data from different batch IDs!
```

### The Core Issues:

| Issue | Impact |
|-------|--------|
| **New batch ID on every run** | Data from failed batches becomes orphaned |
| **`batch_id` NOT in primary key** | Overwrites old data with new batch ID (lost history) |
| **No retry tracking** | Failed batches are forgotten, leading to duplicates |

---

## ✅ **The Solution: Exactly-Once Semantics**

### Three Key Changes:

---

#### 1. **`batch_id` Added to PRIMARY KEY**

| Before | After |
|--------|-------|
| `PRIMARY KEY (id, window_start)` | `PRIMARY KEY (id, window_start, batch_id)` |
| `ON CONFLICT (id, window_start)` | `ON CONFLICT (id, window_start, batch_id)` |

**Why This Matters:**

| Scenario | Before (Without batch_id) | After (With batch_id) |
|----------|---------------------------|-----------------------|
| Same data, same batch | ✅ UPDATE | ✅ UPDATE |
| Same data, **different batch** | ⚠️ **OVERWRITES** (loses history) | ✅ **NEW ROW** (preserves history) |
| Retry with same batch_id | ✅ UPDATE | ✅ UPDATE |
| Retry with different batch_id | ❌ Mixed data | ✅ Clean separation |

**This ensures that each batch execution is uniquely identifiable and traceable.**

---

#### 2. **Batch ID Reuse on Failure**

**Before:**
```java
// Every run gets a new batch ID
UUID batchId = UUID.randomUUID();
```

**After:**
```java
// Check for failed batch first
UUID batchId = fileDiscoveryService.getFailedBatchId();
if (batchId != null) {
    // Reuse the failed batch ID for retry
    log.info("Reusing failed batch: {}", batchId);
} else {
    // New batch
    batchId = UUID.randomUUID();
}
```

**Why This Matters:**

| Scenario | Before (New ID) | After (Reuse ID) |
|----------|-----------------|------------------|
| **Partial failure** | Retry with new ID → duplicates | Retry with same ID → overwrites correctly |
| **Data consistency** | Mixed batch IDs | All data in a retry has same ID |
| **Debugging** | Hard to trace | Easy to identify retry attempts |

---

#### 3. **Custom Exception with `batch_id` Propagation**

**Before:**
```java
catch (Exception e) {
    log.error("Processor failed");
    throw new RuntimeException("Failed");  // ❌ batch_id lost
}
```

**After:**
```java
catch (Exception e) {
    log.error("Processor failed");
    throw new BatchProcessingException(batchId, "Failed", e);  // ✅ batch_id preserved
}
```

**Why This Matters:**

| Aspect | Before | After |
|--------|--------|-------|
| **Error context** | No batch_id in exception | Batch_id is part of the exception |
| **Retry detection** | Cannot identify which batch failed | Exact batch_id is stored for retry |
| **Audit trail** | Lost context | Complete traceability |

---

## 🔄 **The New Flow: Exactly-Once in Action**

```
Batch 1 (SUCCESS):
├── fileDiscoveryService.getFailedBatchId() → null
├── batchId = UUID.randomUUID() → UUID-1
├── Processors run successfully
├── fileDiscoveryService.markFilesAsProcessed(files, UUID-1) ✅
├── fileDiscoveryService.clearFailedBatch() ✅
└── Files marked as processed ✅

Batch 2 (PARTIAL FAILURE):
├── fileDiscoveryService.getFailedBatchId() → null
├── batchId = UUID.randomUUID() → UUID-2
├── TrafficProcessor: SUCCESS ✅ (writes with UUID-2)
├── ConnectionProcessor: FAILED ❌
│   └── throws BatchProcessingException(UUID-2) ✅
├── Catch → fileDiscoveryService.storeFailedBatch(UUID-2) ✅
├── Files NOT marked as processed ❌
└── Batch ID stored for retry ✅

Batch 3 (RETRY):
├── fileDiscoveryService.getFailedBatchId() → UUID-2 ✅
├── batchId = UUID-2 (REUSED!) ✅
├── TrafficProcessor: SUCCESS ✅ (UPSERT with UUID-2)
├── ConnectionProcessor: SUCCESS ✅ (UPSERT with UUID-2)
├── fileDiscoveryService.markFilesAsProcessed(files, UUID-2) ✅
├── fileDiscoveryService.clearFailedBatch() ✅
└── Files marked as processed ✅

RESULT:
├── UUID-1 rows (from successful Batch 1) ✅
├── UUID-2 rows (complete, from retry Batch 2) ✅
└── All data is consistent and traceable!

→ NO DUPLICATES! EXACTLY-ONCE SEMANTICS! 🎯
```

---

## 🎯 **Why This Achieves Exactly-Once Semantics**

| Requirement | How It's Achieved |
|-------------|-------------------|
| **Idempotency** | Same batch_id → UPSERT updates existing rows, doesn't duplicate |
| **Retry safety** | Failed batch_id is reused, ensuring all data has same ID |
| **No duplicates** | `batch_id` in PK prevents duplicate rows from different batches |
| **No data loss** | Failed batches are retried with same ID, overwriting incomplete data |
| **Traceability** | Each batch execution is uniquely identifiable by its batch_id |

---

## 📊 **Comparison: Before vs After**

| Aspect | Before (At-Least-Once) | After (Exactly-Once) |
|--------|------------------------|----------------------|
| **Retry on failure** | New batch ID → duplicates | Reuse batch ID → overwrite |
| **Partial failure** | Orphaned data with new ID | Retried with same ID |
| **Data consistency** | Multiple batch IDs for same data | Single batch ID per attempt |
| **Duplicate risk** | High (new ID on every run) | Zero (UPSERT with same ID) |
| **Audit trail** | Confusing | Clear traceability |
| **Debugging** | Difficult | Easy (know exact batch) |

---

## 🛡️ **Edge Cases Handled**

| Edge Case | Handling |
|-----------|----------|
| **Retry also fails** | Batch ID stays stored, retried again |
| **Multiple retries** | Same batch ID used, UPSERT repeatedly |
| **Success after retry** | Batch ID cleared, files marked processed |
| **Unexpected exception** | Still stores batch ID (fallback) |
| **Application restart** | Optional: store in DB for persistence |

---

## 🎯 **Key Takeaways for Your Interview**

1. **At-least-once** processes can create duplicates when retrying failed batches.

2. **Exactly-once** requires:
    - `batch_id` in the primary key (to distinguish different attempts)
    - Reusing the same `batch_id` on retry (to overwrite incomplete data)
    - Propagating `batch_id` through exceptions (to know which batch failed)

3. **Result**: The same data processed multiple times produces the same final state – no duplicates, no data loss.

4. **The pattern is universal**: This is how you implement exactly-once in streaming/batch systems.

---

## 🚀 **Conclusion**

By adding `batch_id` to the primary key, implementing batch ID reuse on failure, and propagating `batch_id` through custom exceptions, we transformed the cube maker from **at-least-once** to **exactly-once** semantics. Now, even with failures and retries, each batch execution produces a consistent, deduplicated, and fully traceable result.
