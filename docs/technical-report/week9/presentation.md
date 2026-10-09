# Presentation

## 0. Scope — 20 seconds

- I worked across the analytics path: Cube Maker, REST API, Analytics Query, testing, Spark infrastructure, Helm deployment, Prometheus, Grafana dashboards and alerting.

---

## 1. Cube Maker — 1 minute 40 seconds

High level:

- **Cube Maker** is a scheduled **Spark** application that discovers unprocessed NetFlow Parquet files in **HDFS** and converts them into query-optimized **PostgreSQL aggregate cubes**.

Important details:

- We chose **Spring scheduling** for **simplicity**: Cube Maker runs nearly **continuously**, so **CronJobs** would offer little resource-saving benefit between runs.
- At the start of each run, the **maintenance service** creates required **daily PostgreSQL partitions** and **drops** expired ones.
- **File discovery** processes the **oldest eligible files first**, with a **maximum file count per run** preventing uncontrolled historical-data loading.
- Parquet files are **read once** into one **shared Spark dataset**, persisted with **`MEMORY_AND_DISK`**, reused by all processors, and explicitly **unpersisted** afterward.
- It creates **four hourly cubes**:

    - **Traffic by source IP**: bytes, packets and record count.
    - **Connections** by source IP and destination IP.
    - **Bandwidth by IP** with inbound and outbound semantics.
    - **Protocol usage** with bytes, packets and connection count.

- **`BatchExecutor`** provides **controlled retry** behavior for database writes.

Most valuable point:

> The design avoids repeated HDFS scans and driver-side collection while keeping each batch bounded and recoverable.

---

## 2. Cube Maker PostgreSQL Write — 1 minute

High level:

- Cube Maker uses **batch identity**, **processed-file state**, **staging tables** and **idempotent UPSERTs** to make retries safe.

Important details:

- Every run receives a **`batch_id`**.
- Successfully completed files are recorded in **`cube_maker_state`** table.
- Files are **marked processed only after all required processors complete**.
- A failed batch **reuses its batch ID** during retry.
- Spark writes **per partition**, rather than collecting all aggregate rows on the driver.
- **JDBI** is used instead of **Spark JDBC** because we have **UPSERT** behavior, which Spark JDBC does not provide natively.
- Each Spark partition uses PostgreSQL **binary `COPY`** into permanent **`UNLOGGED` staging tables** (and skip **WAL**).
- The **final merge** is transactional and verifies that **staged and merged row counts match** (with **WAL**).

Use precise wording:

> This provides **exactly-once** behavior within the Cube Maker processing.

---

## 3. PostgreSQL Schema, Flyway and Index Optimization — 55 seconds

High level:

- I designed and evolved the **PostgreSQL cube schema** using versioned **Flyway migrations** and optimized it according to actual **REST query patterns**.

Important details:

- Aggregate tables are **partitioned daily** by **`window_start`**, enabling **retention** and **partition pruning**.
- **IP** addresses use PostgreSQL **`INET`** rather than oversized strings. (**DISK**)
- **Unused connection fields** and **surrogate IDs** were removed. (**DISK**)
- We evaluated **`INT`**, but aggregate **packet and byte** counts need **`BIGINT`**. (**DISK**)
- Removed **redundant indexes** covered by **composite primary keys**. (**DISK**)
- Existing **Flyway Migrations** were preserved; schema changes were added as **new Flyway versions**.

Most valuable point:

> The optimization was based on **real query semantics** and **storage behavior**, not generic indexing rules.

---

## 4. REST API — 45 seconds

- Exposes **4 PostgreSQL cube** families with validated **time ranges**, **pagination**, **sorting** and **query limits**.

---

## 5. Analytics Query — 2 minutes

High level:

- Analytics Query is a standalone service that converts natural-language NetFlow questions into controlled analytics results using existing tools or guarded generated SQL.

Start by contrasting the naïve design:

> The naïve approach is to give the LLM the database schema and question and execute whatever SQL it returns. I deliberately did not implement that because it gives the model responsibility for authorization, datasource selection and database safety.

Actual architecture:

```text
Natural-language question
→ Controller
→ QueryService
→ structured intent (saca)
→ intent validation
→ approved REST tool when possible
→ SQL fallback eligibility
→ semantic schema catalog
→ deterministic datasource routing
→ SQL generation (saca)
→ AST policy validation
→ EXPLAIN safety check
→ read-only execution
→ bounded explanation (saca)
```

Important details:

- **Tool-first:** approved REST tools take priority.
- **SQL execution:** read-only credentials, timeouts and result-size limits.
- **Semantic routing:** Java chooses PostgreSQL aggregates or StarRocks raw data by meaning and grain.
- **Schema scope:** the LLM receives only the selected approved schema.
- **Intent safety:** clarify ambiguity, reject out-of-scope/destructive requests and validate fields/time ranges.
- **Fallback eligibility:** requires one approved table in one datasource.
- **AST validation:** JSqlParser enforces one SELECT, table/column/function allowlists, time/row bounds and no joins/nested queries.
- **EXPLAIN safety:** checks estimated rows and PostgreSQL cost before execution.
- **Bounded explanation:** explanation failure preserves results.

SACA decision:

- Used **SACA-hosted Qwen 3.8** instead of a **custom Text-to-SQL** model.
- Avoided GPU, and serving **resource overhead**; an OpenAI-compatible client keeps the model replaceable.

Most valuable statement:

> The **LLM** interprets and proposes; **deterministic** Java code **validates**, authorizes, routes and executes.

---

## 6. Testing Strategy — 55 seconds

High level:

- **Cube Maker**, **REST API** and **Analytics Query** are tested through **deterministic unit and integration tests** using **real behavior** rather than Mockito-based interaction tests.

Important details:

- **Reusable JUnit extensions** manage **PostgreSQL**, **StarRocks** and **Spark** deterministic HTTP test services.
- **Containers are shared** across appropriate module test suites instead of recreated for every test.
- **Surefire** runs **unit tests**; **Failsafe** runs **integration tests**.

Most valuable point:

> These tests verify real protocols and database behavior, so a passing build gives more confidence than tests that only verify mocked method calls.

---

## 7. Spark Infrastructure — 55 seconds

High level:

- I deployed **Spark** as production-managed **Kubernetes infrastructure** with **persistent worker storage**, **high-availability HDFS integration** and **durable application history**.

Important details:

- **1 Spark Master**, **4 Workers** and **1 History Server**.
- Each worker offers **1 Spark cores** and **3 GB** of executor memory capacity, with **Kubernetes limits** of **2 CPUs** and **4 GiB**.
- Cube Maker uses **4 executors**, **one per worker**, each with **1 core** and **2 GB of heap memory**.
- **StatefulSets** reconnect recreated workers to their own **PVCs**, preserving **executor logs** for **failure diagnosis** (automatic **shuffle recovery** is not configured but could be used).
- **PVCs** let **Kubernetes** manage **storage allocation**, instead of hard-coding with **`hostPath`**.
- Spark **event logs** are stored in **HDFS** and read by the **History Server**.

> **Service exposure:**
> - Spark **RPC** and **workers** stay **internal** via **`ClusterIP`**, **limiting outside access** to job submission; only **master/history UIs** use **LoadBalancer**.

---

## 8. Cube Maker Helm Chart — 35 seconds

High level:

- The **Cube Maker Helm chart** packages the application’s **runtime configuration**, **health**, **resources**, **secrets** and **Spark connectivity** as a **repeatable deployment**.

Important details:

- **1 Cube Maker replica** prevents duplicate scheduling while **Spark executors** handle **parallel processing**.
- **Database credentials** come from **Kubernetes Secrets**.
- **Startup, readiness and liveness probes** use **Spring Boot Actuator**.
- **`spark.driver.host`** uses **`${DRIVER_HOST}`** (the **pod IP**), keeping executors connected after **rescheduling**.

---

## 9. Analytics Query Helm Chart — 35 seconds

High level:

- The Analytics Query chart deploys the service as a horizontally available, resource-bounded application with secure external dependencies.

Important details:

- **2 replicas** for high **availability**.
- **LLM**, **PostgreSQL** and **StarRocks** **credentials** come from Kubernetes **Secret**.
- **StarRocks schema** metadata **refreshes** every **15 minutes**.

---

## 10. Prometheus and Grafana Infrastructure — 35 seconds

Prometheus details:

- It has 15-day retention and a 20 GiB PVC.

Grafana details:

- Grafana uses a 2 GiB PVC so internal state survives pod recreation.

---

## 11. Grafana Dashboards and Cube Maker Alerts — 45 seconds

High level:

- I built dashboards to help us for monitoring.


- **Storage** dashboard

- **Cube Maker** dashboard

- **Analytics Query** dashboard (RED approach)
    - Request **rate**.
    - **Errors** and SQL rejection reasons.
    - End-to-end **duration**.

Most valuable point:

> The alerts focus on conditions requiring action, while dashboards support diagnosis without creating unnecessary operational noise.

---

## Closing — 20 seconds

- This was a team effort, and I am happy to discuss any technical details during the questions.

## Time allocation

| Topic | Time |
|---|---:|
| Opening | 0:20 |
| Cube Maker | 1:40 |
| Reliability and PostgreSQL writes | 1:00 |
| Schema and indexing | 0:55 |
| REST API | 0:45 |
| Analytics Query | 2:00 |
| Testing | 0:55 |
| Spark infrastructure | 0:55 |
| Cube Maker Helm | 0:35 |
| Analytics Query Helm | 0:35 |
| Prometheus and Grafana infrastructure | 0:35 |
| Dashboards and alerts | 0:45 |
| Closing | 0:20 |
| **Total** | **10:00** |
