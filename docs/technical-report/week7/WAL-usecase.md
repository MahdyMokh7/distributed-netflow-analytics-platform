Yes. In PostgreSQL, **WAL = Write-Ahead Log**. It is fundamental to how PostgreSQL makes writes both durable and reasonably fast.

The core idea is:

```text
INSERT / UPDATE
      ↓
PostgreSQL changes pages in memory
      ↓
writes a description of the change to WAL
      ↓
WAL is flushed to disk
      ↓
COMMIT can succeed
      ↓
actual table/index pages can be written later
```

PostgreSQL follows the rule that the WAL describing a change must reach durable storage before the corresponding changed table/index page is allowed to reach disk. Because WAL is mostly sequential, flushing it is generally much cheaper than synchronously flushing many scattered table and index pages for every transaction. PostgreSQL can then reconstruct those pages by replaying WAL after a crash. ([PostgreSQL][1])

WAL is therefore used for **crash recovery**, **transaction durability**, **streaming replication**, and **point-in-time recovery/backups**. In your Patroni HA PostgreSQL cluster, WAL is also what allows replicas to reproduce changes made by the primary.

### Why WAL matters specifically for your slow Cube Maker

Your measurements are very revealing. PostgreSQL has already produced about **524 GB of WAL**, and your live sample showed about **192 MB every 15 seconds**, or roughly **12.8 MB/s**. That corresponds to about **11.5 GB of WAL every 15 minutes**.

But your PostgreSQL currently has:

```text
max_wal_size = 2 GB
```

while your workload generates:

```text
~11.5 GB / 15 minutes
```

That mismatch matters a lot.

PostgreSQL starts a checkpoint when either:

```text
checkpoint_timeout is reached

OR

max_wal_size is about to be exceeded
```

whichever happens first. ([PostgreSQL][2])

So imagine your configuration:

```text
checkpoint_timeout = 15 min
max_wal_size       = 2 GB

WAL generation ≈ 12.8 MB/s
```

At that generation rate:

```text
2 GB / 12.8 MB/s ≈ 160 seconds
                  ≈ 2.7 minutes
```

So instead of approximately:

```text
checkpoint
      |
      |----------- 15 minutes ------------|
      |
checkpoint
```

you are approximately forcing:

```text
checkpoint
   |
 ~2-3 min
   |
checkpoint
   |
 ~2-3 min
   |
checkpoint
   |
 ~2-3 min
   |
checkpoint
```

And your actual statistics support exactly this diagnosis:

```text
requested checkpoints = 468
timed checkpoints     = 18
```

That means PostgreSQL overwhelmingly performed checkpoints because WAL pressure forced them, rather than because the normal checkpoint timer expired.

### Why frequent checkpoints hurt write throughput

A checkpoint means PostgreSQL has to push dirty table/index pages from memory toward persistent storage.

Your workload is already doing huge amounts of:

```text
INSERT
UPSERT conflict checks
index lookups
index modifications
WAL writes
data-file writes
```

Now add frequent checkpoints:

```text
Spark writers
     ↓
PostgreSQL INSERT/UPSERT
     ↓
 ┌───────────────────────────────┐
 │ WAL sequential writes         │
 │ index reads                   │
 │ heap/index modifications      │
 │ frequent checkpoint writes ← │ extra pressure
 └───────────────────────────────┘
                 ↓
              storage
```

So your PostgreSQL storage is competing for I/O.

There's another effect too. With `full_page_writes` enabled, PostgreSQL records a full copy of a page the **first time that page is modified after a checkpoint**. Therefore, having checkpoints too frequently can itself increase WAL generation. PostgreSQL's documentation explicitly calls out both the I/O cost of dirty-page flushing and this additional WAL traffic. ([PostgreSQL][2])

This can become a bad cycle:

```text
heavy writes
   ↓
lots of WAL
   ↓
2 GB max_wal_size reached
   ↓
checkpoint
   ↓
large amount of dirty-page I/O
   ↓
more full-page WAL afterward
   ↓
more WAL
   ↓
next forced checkpoint
```

### What increasing `max_wal_size` actually does

Your diagnosis proposes:

```yaml
postgresql:
  parameters:
    maxWalSize: 16GB
```

while initially keeping:

```text
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
```

because your observed 15-minute WAL volume is roughly 11.5 GB.

This does **not** mean:

> PostgreSQL gets 14 GB more memory.

And it does not mean:

> PostgreSQL writes 8× faster because WAL became 8× bigger.

It means PostgreSQL is allowed to accumulate considerably more WAL before WAL volume itself forces a checkpoint.

You want approximately:

```text
WAL produced during 15 min ≈ 11.5 GB

max_wal_size = 16 GB

             11.5 GB
|--------------------------------|
|                                |
0                              16 GB
```

So ideally the normal:

```text
checkpoint_timeout = 15 minutes
```

becomes the checkpoint trigger rather than repeatedly hitting the WAL limit.

Then:

```text
checkpoint_completion_target = 0.9
```

allows PostgreSQL to spread checkpoint writes over most of that interval instead of producing a big write burst. PostgreSQL recommends this approach to smooth checkpoint I/O. ([PostgreSQL][2])

### But this is important for your project

I would **not expect `max_wal_size=16GB` to magically turn your 132-minute database write into 20 minutes**.

Your measurements show that PostgreSQL writing takes about:

```text
whole batch:       136 min
PostgreSQL writes: 132 min
percentage:        97.5%
```



And your live sessions were frequently waiting on:

```text
IO / DataFileRead
```

which indicates that conflict checks/index operations are reading pages from storage.

You also have enormous output cardinality from a five-file batch:

```text
Source:      13.8 M

Traffic:     13.8 M
Connection:  13.8 M
Top-IP:      27.6 M
Protocol:    20
```

So the first three aggregations are effectively generating tens of millions of PostgreSQL rows.

Therefore think of the optimization like this:

```text
Current problem

                    PostgreSQL write bottleneck
                              │
           ┌──────────────────┼─────────────────┐
           ↓                  ↓                 ↓
      Checkpoints        index/data I/O     huge row count
      too frequent       conflict checks    ~55M rows/batch
           │                  │                 │
     max_wal_size        indexes/cache      architecture /
      2 → 16 GB             issue           data semantics
           │
           ↓
      FIX THIS FIRST
```

Increasing `max_wal_size` is a **low-complexity configuration correction** that removes unnecessary checkpoint pressure.

Then benchmark exactly the same five files again.

If:

```text
132 min → 110 min
```

great—you've demonstrated checkpoint pressure was significant.

If:

```text
132 min → 128 min
```

then checkpoints were only a small part of the total bottleneck, and the larger issue is the tens of millions of indexed UPSERTs.

That's why your diagnosis correctly proposes the sequence:

```text
capacity / retention
       ↓
max_wal_size = 16 GB
       ↓
same 5-file benchmark
       ↓
measure
       ↓
COPY + UNLOGGED staging prototype
       ↓
potential cardinality/schema changes
```



One more important distinction: **UNLOGGED staging** is another way WAL relates to throughput. PostgreSQL does not generate normal WAL for changes to unlogged tables, so Spark can `COPY` millions of rows into an unlogged staging table much more cheaply, then perform one set-based merge into your real logged/indexed table. Your plan proposes exactly this.

So for your Nimbo workload, I would think about WAL optimization as two separate things:

```text
1. max_wal_size 2GB → 16GB
   ↓
   fewer forced checkpoints
   ↓
   less unnecessary I/O pressure


2. Later: UNLOGGED staging + COPY
   ↓
   less WAL during the staging load
   ↓
   fewer expensive individual UPSERT operations
   ↓
   set-based final merge
```

The **first one is the correct next step**. The second is a larger writer-architecture optimization that you should benchmark only after establishing the new WAL/checkpoint baseline.

[1]: https://www.postgresql.org/docs/17/wal-intro.html?utm_source=chatgpt.com "PostgreSQL: Documentation: 17: 28.3. Write-Ahead Logging (WAL)"
[2]: https://www.postgresql.org/docs/17/wal-configuration.html?utm_source=chatgpt.com "PostgreSQL: Documentation: 17: 28.5. WAL Configuration"
