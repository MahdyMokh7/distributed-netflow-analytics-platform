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


