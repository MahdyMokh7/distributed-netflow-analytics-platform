# StarRocks backfill

One-shot Java 17 command-line tool that restores a closed UTC interval from HDFS Parquet files into
the `nimbo.netflow` StarRocks table.

StarRocks reads Parquet directly through its built-in `FILES()` table function. This module does not
need Spark, a Hadoop client, or Spring. Its only runtime library is the MySQL JDBC driver used to send
SQL to the StarRocks frontend.

## Recovery flow

Every run first performs the same read-only checks:

1. Read both schemas with `DESC nimbo.netflow` and `DESC FILES(...)`, then build a projection for every
   non-empty touched hourly Parquet path.
2. Read the same interval's metrics from HDFS and StarRocks and print the comparison.

If the HDFS and StarRocks interval metrics already match, the run stops without changing data.
`BACKFILL_MODE=PLAN` also stops without changing data. Otherwise, `EXECUTE` loads only the requested HDFS
interval into a temporary partition, validates it, deletes that exact interval from the formal table,
inserts the staged rows, validates the published interval, and drops the temporary partition. The final
insert does not name a formal partition. StarRocks expression partitioning routes every row by
`PROCESS_TIME` and automatically creates the daily partition if it does not already exist.

All touched hours are joined with `UNION ALL` inside one HDFS load `INSERT`, so a multi-hour stage is
committed as one StarRocks transaction. After execution, row count, byte and packet sums, and minimum
and maximum timestamp are checked against HDFS.

HDFS access remains hourly because that is the directory layout. A request for 10:15-10:45 reads the
`10` directory, but both the HDFS load and StarRocks delete use the exact half-hour interval `[from, to)`.
The temporary partition is a daily routing container, but it stores only those selected rows. It does
not copy, read, or stage the other 23.5 hours. Empty hourly directories are reported and skipped.

The load mapping is not hardcoded:

- Columns present on both sides are matched without case sensitivity and cast to the StarRocks type.
- `PROCESS_TIME` is matched directly like every other shared column; both writers store it in epoch
  milliseconds.
- `KAFKA_TOPIC`, `KAFKA_PARTITION`, and `KAFKA_OFFSET` are read directly from Parquet like normal
  columns. Missing required metadata stops the run before writing; no synthetic fallback is generated.
- A nullable StarRocks-only evolution column is filled with `NULL` for older Parquet files.
- An HDFS-only column stops the run before writing and prints the required `ALTER TABLE ... ADD COLUMN`.
- A missing non-null StarRocks column or an incompatible type also stops the run before writing.

Those aggregate checks are practical validation signals, not a mathematical proof that every row is
identical. As requested, matching pre-load aggregates are treated as complete and cause the run to skip.

## Idempotency and operational limits

For an existing partition, the formal partition is unchanged while the selected interval is staged and
validated. Publishing is intentionally non-atomic: there is a short gap after the exact interval is
deleted and before the staged rows are inserted. If a run fails in that gap, rerun the same `EXECUTE`.
The retry is idempotent because it deletes the exact interval again before inserting it, so it does not
append a second copy.

The same flow is used when the daily formal partition does not exist. The delete finds no rows, and the
final insert lets StarRocks create the partition. A retry either stops because the metrics already match or
stages and replaces the exact interval again.

Disk overhead is approximately the selected interval, not the complete daily partition. `BACKFILL_TO`
must still be older than `BACKFILL_MINIMUM_AGE_MINUTES`, and the complete interval must be inside the
configured StarRocks retention window. Run during the agreed quiet period with no concurrent load into
the target table while the delete and insert are executing.

A range that crosses UTC midnight is split into one daily execution per touched day.

## Environment

Required variables:

| Variable | Example |
|---|---|
| `STARROCKS_JDBC_URL` | `jdbc:mysql://10.10.61.53:9030/nimbo` |
| `STARROCKS_USERNAME` | `backfill_user` |
| `STARROCKS_PASSWORD` | supplied by a Kubernetes Secret |
| `BACKFILL_FROM` | `2026-09-26T10:00:00Z` |
| `BACKFILL_TO` | `2026-09-26T12:00:00Z` |
| `BACKFILL_HDFS_BASE_PATH` | `hdfs://team1/netflow/parquetFiles` |
| `BACKFILL_HDFS_USERNAME` | `team1` |

Optional variables:

| Variable | Default | Purpose |
|---|---|---|
| `BACKFILL_MODE` | `PLAN` | Use `EXECUTE` only after reviewing the plan output. |
| `BACKFILL_DATABASE` | `nimbo` | Destination database. |
| `BACKFILL_TABLE` | `netflow` | Destination table. |
| `BACKFILL_MINIMUM_AGE_MINUTES` | `60` | Refuse intervals that may still be receiving data. |
| `BACKFILL_RETENTION_DAYS` | `7` | Refuse intervals outside the target table's daily retention. |

The StarRocks FE and BE processes need the repository-managed `hdfs-site.xml` before they can resolve
the `team1` HDFS HA nameservice. Apply the StarRocks Ansible deployment after changing that file. The
logical URI then follows an active NameNode failover instead of depending on one NameNode IP.

## Build and test

```bash
mvn -pl starrocks-backfill -am clean verify
```

Always deploy and run `PLAN` first with the exact production interval. Review its paths, schema mappings,
and HDFS/StarRocks metrics before creating an `EXECUTE` job.
