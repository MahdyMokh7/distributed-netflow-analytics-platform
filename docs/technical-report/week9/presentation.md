# Presentation

prompt:
these are the major topics i have to give presentation and i only have 10 minutes and i have to say titles with a bit explanation one or two sentences of each

these modules and works are for mine but in this development certainly there things that are move valuable to get said in the presentation which is for the technical project managers which are technical

these are the work i did but they have some very important and valuable things in them organize my presentaion so that i get mostly seen

these are my reposniblites (so first inspect hte entire things i wrote below precisely):
1. cube-maker
2. analytics-query
3. rest-api(postgresql side)
4. the modules said tests
5. infra spark
6. infra promo
7. infra grafana
8. the helm chart of cube-maker
9. the helm chart of analytics-query
10. dashobard of grafana storage, cube-maker, analytics
11. alert of cube-maker
    (the order shoudl be justified and the time for each one shoudl be approtiate to its value in presentaion)

some things that i have in mind but are just things for you to rember i dont say i shoudl say these or not say these but from these and many morethat i currently dont rember you shoudl pick the most valuable ones
first thing of the ones i did not write
the things i exposed or not with servies of differn things
postgresql sql indexing
flyway miogrations
WAL and stagging of tables of postgresql
per partition write&#x20;
exaclty once semantic of cube-maker with batch-id and upsert and cube-maker-state table
history server of spark is in HDFS
grafana PVC saving state
spark statefullset with pvc is better than host-path and why not just the definition of statefullset
rest-api layers with dto and model and repo(JPA and spring JPA) and service controller
our aggeregations of each table their definition and&#x20;
the tables i optimized for disk their key, index types of fileds
having JDBI instead of jdbc and spark jdbc in my reason and pros and cons for me and with DAO
persist the read for spark and of parquet files and don't read per processor&#x20;
spring scheduler instead of chron job
the helm details the important ones for each infra or module
the details of config of spark memory cpu cores number of exe workers&#x20;
PostgreSQL indexing&#x20;
read parquet files with constraint of number of parquet files not time with discovery of old files.
batch-exectuor with retry in cube-maker
test extension and containerized  test which is imprtant for them
my magnifisent complete architecture of analytics-query (and any detail important and valuable) and the naive version i did not implement which is just give hte table schema and the qeustion to llm and say give me auery
saca better than custom model specific text-to-sql resource presusre i had

do not give me a presentaion instead give me bullet by bullet things for topics mentioned ornot mentinoed that you shoudl read my projecrt nad extract the important ones from  my scope and write it in  a way that in 10 minutes it shoudl get finished

you are too much focusing on the ithngs i wrote in prompt these are some details that i think some of them are valuable first we should say each of hte 10 mentioned i scope and in each scope tell the imporatnt details but at first for each of the 10 i wrote we should say its high level thing and then some valauble imporatnat detials in bulletes

You listed 11 responsibility areas. The best order is the system’s value flow:

```text
Data processing → storage → API → intelligent query
→ testing → deployment → monitoring → alerting
```

This makes the presentation coherent and shows end-to-end ownership.

## 0. Scope — 20 seconds

- I worked across the analytics path: Cube Maker, REST API, Analytics Query, testing, Spark infrastructure, Helm deployment, Prometheus, Grafana dashboards and alerting.

---

## 1. Cube Maker — 1 minute 40 seconds

High level:

- Cube Maker is a scheduled Spark application that discovers unprocessed NetFlow Parquet files in HDFS and converts them into query-optimized PostgreSQL aggregate cubes.

Important details:

- It creates four hourly cubes:

    - Traffic by source IP: bytes, packets and record count.
    - Connections by source IP and destination IP.
    - Bandwidth by IP with inbound and outbound semantics.
    - Protocol usage with bytes, packets and connection count.

- File discovery processes the oldest eligible files first and bounds each run by a maximum file count. This prevents one run from loading an uncontrolled amount of historical data.
- Parquet files are read once into one shared Spark dataset, persisted with `MEMORY_AND_DISK`, reused by all processors, and explicitly unpersisted afterward.
- Spring scheduling keeps execution, state, health and metrics inside the same application lifecycle; Kubernetes CronJobs would introduce a separate pod lifecycle for every run.
- `BatchExecutor` provides controlled retry behavior for database writes.

Most valuable point:

> The design avoids repeated HDFS scans and driver-side collection while keeping each batch bounded and recoverable.

---

## 2. Reliability and PostgreSQL Write Design — 1 minute

High level:

- Cube Maker uses batch identity, processed-file state, staging tables and idempotent UPSERTs to make retries safe.

Important details:

- Every run receives a `batch_id`.
- Successfully completed files are recorded in `cube_maker_state`.
- Files are marked processed only after all required processors complete.
- A failed batch reuses its batch ID during retry.
- Spark writes per partition, rather than collecting all aggregate rows on the driver.
- Each Spark partition uses PostgreSQL binary `COPY` into permanent `UNLOGGED` staging tables.
- Staging rows contain Spark partition and attempt identifiers, allowing a retried partition to replace its previous attempt.
- The final merge is transactional and verifies that staged and merged row counts match.

WAL decision:

- Staging tables are `UNLOGGED`, reducing unnecessary WAL pressure for temporary data.
- Final aggregate tables remain durable.
- Recovery remains possible because HDFS Parquet files are the durable source and processed-file state controls replay.

Use precise wording:

> This provides exactly-once behavior within the Cube Maker processing protocol through stable batch identity, state tracking and idempotent writes.

---

## 3. PostgreSQL Schema, Flyway and Index Optimization — 55 seconds

High level:

- I designed and evolved the PostgreSQL cube schema using versioned Flyway migrations and optimized it according to actual REST query patterns.

Important details:

- Aggregate tables are partitioned daily by `window_start`, enabling retention and partition pruning.
- IP addresses use PostgreSQL `INET` rather than oversized strings.
- Connection identity uses a compact 32-byte SHA-256 value stored as `BYTEA`.
- Unused connection fields and surrogate IDs were removed.
- Counters remain `BIGINT` because aggregated packet and record counts can exceed integer limits.
- Redundant indexes already covered by composite primary keys were removed.
- Source and destination connection indexes were retained because REST queries use both access paths.
- Existing migrations were preserved; schema changes were added as new Flyway versions.

Most valuable point:

> The optimization was based on real query semantics and storage behavior, not generic indexing rules.

---

## 4. REST API — 45 seconds

High level:

- The PostgreSQL side of the REST API exposes the four analytical cube families through validated and bounded HTTP contracts.

Important details:

- The application uses clear layers:

    - Request and response DTOs for public contracts.
    - Controllers for HTTP handling.
    - Services for business behavior.
    - Spring Data JPA repositories for PostgreSQL queries.
    - Models representing the final migrated cube schema.

- Spring Boot provides the API foundation through Spring MVC controllers, Bean Validation, Spring Data JPA repositories, pageable queries and centralized exception handling.
- It supports IP and time filtering, pagination, sorting, limits and controlled error responses.
- Time ranges are bounded to prevent expensive unbounded scans.
- Aggregate definitions in REST queries were aligned with the Spark cube definitions so both layers represent the same analytical meaning.

---

## 5. Analytics Query — 2 minutes

High level:

- Analytics Query is a standalone service that converts natural-language NetFlow questions into controlled analytics results using existing tools or guarded generated SQL.

Start by contrasting the naïve design:

> The naïve approach is to give the LLM the database schema and question and execute whatever SQL it returns. I deliberately did not implement that because it gives the model responsibility for authorization, datasource selection and database safety.

Actual architecture:

```text
Natural-language question
→ structured intent
→ intent validation
→ approved REST tool when possible
→ SQL fallback eligibility
→ semantic schema catalog
→ deterministic datasource routing
→ SQL generation
→ AST policy validation
→ EXPLAIN safety check
→ read-only execution
→ bounded explanation
```

Important details:

- The tool-first path uses trusted PostgreSQL REST endpoints for known traffic, connection, bandwidth and protocol operations.
- If no tool matches, SQL fallback is allowed only for clear, read-only, single-operation NetFlow questions.
- The semantic catalog contains table grain, meanings, dimensions, metrics and allowed capabilities—not merely physical column names.
- PostgreSQL is selected when aggregate cubes preserve the requested semantics; StarRocks is selected when approved raw dimensions are necessary.
- Java makes the datasource decision, not the LLM.
- The model receives only the selected datasource’s approved schema.
- Generated SQL is parsed with JSqlParser and restricted to:

    - One read-only statement.
    - Allowlisted tables, columns and functions.
    - One datasource.
    - Bounded time and result size.
    - Controlled joins.

- PostgreSQL and StarRocks use separate read-only credentials, small connection pools, statement timeouts and datasource-specific `EXPLAIN` checks.
- Tool failure never silently triggers SQL fallback.
- Explanation failure does not discard successful query data.

SACA decision:

- I used the internal SACA-hosted Qwen 3.8 model instead of operating a custom Text-to-SQL model.
- This avoided GPU allocation, model serving, training and lifecycle pressure while keeping the model replaceable behind a small OpenAI-compatible client.

Most valuable statement:

> The LLM interprets and proposes; deterministic Java code validates, authorizes, routes and executes.

---

## 6. Testing Strategy — 55 seconds

High level:

- Cube Maker, REST API and Analytics Query are tested through deterministic unit and integration tests using real behavior rather than Mockito-based interaction tests.

Important details:

- Reusable JUnit extensions manage PostgreSQL, StarRocks and deterministic HTTP test services.
- Containers are shared across appropriate module test suites instead of recreated for every test.
- Unit and containerized integration suites cover the critical paths, with JaCoCo and Sonar tracking both line and branch coverage.

Most valuable point:

> These tests verify real protocols and database behavior, so a passing build gives more confidence than tests that only verify mocked method calls.

---

## 7. Spark Infrastructure — 55 seconds

High level:

- I deployed Spark as production-managed Kubernetes infrastructure with persistent worker storage, high-availability HDFS integration and durable application history.

Important details:

- One Spark master and four workers.
- Workers are configured with two cores and 3 GB Spark memory, with Kubernetes limits of two CPUs and 4 GiB.
- Cube Maker uses four executors, one core and 2 GB per executor.
- Spark uses adaptive query execution, Kryo serialization, compressed shuffle and UTC.
- Worker StatefulSets provide stable identity and dedicated 20 GiB PVCs.
- PVCs are preferable to `hostPath` because storage is managed through Kubernetes rather than coupling pods directly to a node filesystem path.
- Spark event logs are written to HDFS, and the History Server reads them from there.
- History therefore survives pod recreation and is cleaned with a controlled seven-day retention policy.

Service exposure:

- Spark RPC and worker services remain internal through `ClusterIP`; only the Spark master and History Server UIs are exposed through controlled LoadBalancer services.

---

## 8. Cube Maker Helm Chart — 35 seconds

High level:

- The Cube Maker Helm chart packages the application’s runtime configuration, health, resources, secrets and Spark connectivity as a repeatable deployment.

Important details:

- One application replica prevents overlapping schedulers.
- Database credentials come from Kubernetes Secrets.
- Startup, readiness and liveness probes use Spring Boot Actuator.
- CPU and memory requests and limits are explicitly bounded.
- Graceful termination allows scheduled work to finish cleanly.
- The Spark driver address is injected from the pod IP rather than hard-coded to a server.
- The Cube Maker API remains internal through `ClusterIP`, while its Spark UI is exposed separately through a LoadBalancer.

---

## 9. Analytics Query Helm Chart — 35 seconds

High level:

- The Analytics Query chart deploys the service as a horizontally available, resource-bounded application with secure external dependencies.

Important details:

- Two replicas with rolling updates and zero planned unavailability.
- One LoadBalancer exposes the UI and analytics APIs; database and dependency connections remain internal.
- LLM, PostgreSQL and StarRocks credentials come from an existing Kubernetes Secret.
- PostgreSQL, StarRocks catalog and StarRocks execution use small separate pools.
- SQL length, result size, statement timeout, `EXPLAIN` timeout and HTTP concurrency are bounded.
- StarRocks schema metadata refreshes every 15 minutes.
- Health probes and graceful shutdown are included.

---

## 10. Prometheus and Grafana Infrastructure — 35 seconds

High level:

- I implemented centralized metrics collection and persistent visualization infrastructure for application and platform health.

Prometheus details:

- Prometheus discovers and scrapes application and infrastructure targets.
- Only its operational UI is exposed through a LoadBalancer; scrape targets remain internal.
- It has 15-day retention and a 20 GiB PVC.
- Kubernetes discovery uses scoped RBAC and a dedicated service account.
- The container runs as non-root with unnecessary capabilities removed.

Grafana details:

- Prometheus is provisioned as the main metrics datasource.
- Only the Grafana UI is exposed through a LoadBalancer; its Prometheus and StarRocks datasource connections remain internal.
- Grafana uses a 2 GiB PVC so internal state survives pod recreation.
- Dashboards and alert rules are additionally stored in Git and provisioned through Helm.
- Admin and datasource credentials use Kubernetes Secrets.

---

## 11. Grafana Dashboards and Cube Maker Alerts — 45 seconds

High level:

- I built dashboards that answer operational questions instead of displaying every available metric.

Storage dashboard:

- Shows current PostgreSQL, Kafka, HDFS and StarRocks storage usage.
- Also breaks usage down by database replica, Kafka broker, HDFS DataNode and StarRocks backend.

Cube Maker dashboard:

- Shows application health, run state, retry/exactly-once state, failed-batch recovery, run outcomes, stage duration, processed input and PostgreSQL rows written.

Analytics Query dashboard:

- Uses the RED approach:

    - Request rate.
    - Errors and SQL rejection reasons.
    - End-to-end duration.

- Also shows dependency latency, active requests and collapsed component diagnostics to reduce cognitive load.

Cube Maker alerts:

- Repeated or unresolved failed batch.
- Batch execution stuck beyond its expected interval.

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
