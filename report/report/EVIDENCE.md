# Evidence and source map

## Review basis

Prepared on **10 October 2026** from the local Nimbyte source tree at **`891e9f0`**, existing technical reports and presentation, Git author history, supplied troubleshooting context and saved browser captures. The source tree's pre-existing presentation edit was preserved.

No build, production query, deployment, failure injection or benchmark was run to generate this report. There were no local Surefire/Failsafe result files found in the inspected tree, so test-source existence is not represented as a fresh passing test result.

Paths below are relative to the original Nimbyte repository. They are source references, not links to a public repository that has not yet been supplied.

## Claim-to-source mapping

| Claim | Source to inspect |
| --- | --- |
| Scheduling, shared cache, processor sequence and state update | `cube-maker/src/main/java/com/nimbo/cubemaker/job/BatchJobRunner.java` |
| Oldest bounded discovery, in-memory failed ID, state fallback | `cube-maker/src/main/java/com/nimbo/cubemaker/discovery/FileDiscoveryService.java` |
| Cube definitions | `cube-maker/src/main/java/com/nimbo/cubemaker/processor/` |
| COPY orchestration, staging count, merge transaction, final recount | `cube-maker/src/main/java/com/nimbo/cubemaker/writer/PostgresWriter.java` |
| Binary COPY and per-partition transaction | `cube-maker/src/main/java/com/nimbo/cubemaker/writer/StagingPartitionWriter.java` |
| Merge SQL and attempt cleanup | `cube-maker/src/main/java/com/nimbo/cubemaker/writer/StagingTable.java` |
| Explicit batched UPSERT retries | `cube-maker/src/main/java/com/nimbo/cubemaker/writer/PartitionWriter.java`, `BatchExecutor.java` in the same directory |
| Storage/index evolution | `cube-maker/src/main/resources/db.migration/V7__optimize_aggregate_indexes.sql`, `V8__optimize_aggregate_table_storage.sql`, `V9__restore_bigint_counters_and_compact_connection_hash.sql` |
| UNLOGGED staging and daily partitions | `V11__aggregate_staging.sql`, `V10__daily_partitions.sql` in the same migration directory |
| PostgreSQL maintenance | `cube-maker/src/main/java/com/nimbo/cubemaker/retention/RetentionMaintenanceService.java` |
| REST serving contracts | `rest-api/src/main/java/`, especially controller, service and DAO implementations; commits `42dc0f9`, `5c8ba98`, `4e0f811` |
| Analytics orchestration and fallback eligibility | `analytics-query/src/main/java/com/nimbo/analyticsquery/planner/` |
| Intent and semantic routing | `analytics-query/src/main/java/com/nimbo/analyticsquery/intent/`, `routing/DataSourceRouter.java` |
| Catalog refresh and curated schema | `analytics-query/src/main/java/com/nimbo/analyticsquery/schema/StarRocksSemanticCatalogSource.java` and adjacent catalog classes |
| AST policy and semantic SQL validation | `analytics-query/src/main/java/com/nimbo/analyticsquery/sql/` |
| EXPLAIN and bounded execution | `analytics-query/src/main/java/com/nimbo/analyticsquery/sql/execution/` |
| Hosted-model adapter and bounded explanation | `analytics-query/src/main/java/com/nimbo/analyticsquery/llm/`, `explanation/` |
| Configurable analytics limits | `analytics-query/src/main/resources/application.yml` |
| Unit/integration separation | Root `pom.xml` Surefire and Failsafe configuration; module `src/test/java/` trees |
| Spark topology, worker storage and history settings | `infra/helm-charts/spark/values.yaml` and `templates/` |
| Pod-IP driver networking | `charts/cube-maker/values.yaml`, `templates/deployment.yaml` |
| PostgreSQL memory experiment | `infra/helm-charts/postgresql-ha/values.yaml` |
| Monitoring storage, discovery and permissions | `infra/helm-charts/prometheus/values.yaml` and `templates/` |
| Dashboards and alert definitions | `infra/helm-charts/grafana/dashboards/`, `alerting/` |
| Build, prepare and release workflows | `jenkins/Jenkinsfile.ci`, `Jenkinsfile.prepare`, `Jenkinsfile.cd`, `scripts/cd/helm-deploy.sh` |
| Historical design rationale | `docs/technical-report/`, especially weeks 3–9; `docs/plan/Mehdy/` |

## Saved observations

| Evidence | What it supports | What it does not prove |
| --- | --- | --- |
| Spark stages capture | Four successful partition tasks in traffic stage 281 (~19 s) and connection stage 293 (~22 s); visible preparation/shuffle stages | Per-executor database-only duration, driver merge duration, or sustained throughput |
| Cube Maker Grafana capture | Four reported successes in the displayed 12-hour range, long write phases, source/output counters and state indicators | Global exactly-once delivery, crash-safe failed-batch identity, or present health |
| SonarQube capture | Displayed analysis dated Oct 9: quality gate passed, 80.8% new-code coverage, 0.57% new-code duplication | Overall coverage, coverage for one author, absence of all defects, or latest CI state |
| Nimbo Insights capture | A functioning rendered question-entry UI with a populated example | Successful execution of that question; the inspected image shows no result |
| Jenkins pages | Existing CI/release/prepare workflows and displayed historical execution information | All builds passing, a successful current rollout, or correspondence to every local change |

The SonarQube page's selected “New Code” scope is the last 60 days. Its displayed figures apply to that scope and analysis, not necessarily the whole repository.

## Deliberate corrections to broad claims

- “Exactly once” is replaced with specific state, transaction and replay boundaries.
- “PVC means node-independent recovery” is not claimed; storage backend and binding constraints matter.
- “Four workers means four executors with the same resource settings” is not assumed. The inspected Spark chart advertises two cores/3g per worker, while Cube Maker's base production YAML requests three one-core/1g executors; earlier presentation/runtime examples differ. Effective runtime configuration must be checked before quoting executor sizing.
- “More RAM made PostgreSQL faster” remains an experiment until comparable measurements exist.
- “All valid analytics questions work” is not claimed from a loaded UI or healthy endpoint.
- Previously suggested CD fixes are not described as shipped behavior merely because they appeared in a conversation.

## How to strengthen the report later

Attach sanitized, commit-linked test/coverage artifacts; capture successful analytics results on every intended path; record input size, exact runtime settings and phase timings for comparable batches; and document controlled restart/replay tests. Keep expected behavior separate from measured results.
