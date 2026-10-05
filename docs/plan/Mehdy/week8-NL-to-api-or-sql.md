Below is the cleaned-up version I would treat as the **implementation specification for the `analytics-query` feature**. It consolidates the decisions from the whole discussion, removes the alternatives we rejected, and turns them into a six-day execution plan.

# Analytics Query — Final Architecture

```text
                                      ┌─────────────────────────────┐
                                      │            User             │
                                      │ Natural-language question   │
                                      │          or SQL             │
                                      └──────────────┬──────────────┘
                                                     │
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         analytics-query                                          │
│                 Java 17 + Spring Boot 3.4.3 + Maven                             │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                         REST API Layer                                     │  │
│  │                                                                            │  │
│  │  POST /api/analytics/query                                                 │  │
│  │  POST /api/analytics/sql/generate                                          │  │
│  │  POST /api/analytics/sql/execute                                           │  │
│  └────────────────────────────────┬───────────────────────────────────────────┘  │
│                                   │                                              │
│                    request / Bean Validation                                     │
│                                   │                                              │
│                                   ▼                                              │
│                     ┌─────────────────────────┐                                  │
│                     │ AnalyticsQueryService   │                                  │
│                     │ Main orchestration      │                                  │
│                     └────────────┬────────────┘                                  │
│                                  │                                               │
│                                  ▼                                               │
│                     ┌─────────────────────────┐                                  │
│                     │ InternalLlmClient       │                                  │
│                     │ Company internal LLM    │                                  │
│                     └────────────┬────────────┘                                  │
│                                  │                                               │
│                         NL → structured intent                                    │
│                                  │                                               │
│                                  ▼                                               │
│                     ┌─────────────────────────┐                                  │
│                     │ AnalysisIntent          │                                  │
│                     │ status                  │                                  │
│                     │ query requirements      │                                  │
│                     │ clarification           │                                  │
│                     └────────────┬────────────┘                                  │
│                                  │                                               │
│                                  ▼                                               │
│                     ┌─────────────────────────┐                                  │
│                     │ IntentValidator         │                                  │
│                     └────────────┬────────────┘                                  │
│                                  │                                               │
│              ┌───────────────────┼────────────────────┐                          │
│              │                   │                    │                          │
│              ▼                   ▼                    ▼                          │
│   NEEDS_CLARIFICATION       OUT_OF_SCOPE /        ANALYZABLE                     │
│                             REJECTED /                │                          │
│                        UNSUPPORTED_ANALYSIS            ▼                          │
│              │                   │          ┌────────────────────────┐           │
│              ▼                   ▼          │ AnalyticsPlanner       │           │
│       controlled response  controlled       └────────────┬───────────┘           │
│                            response                       │                       │
│                                               Tool available?                    │
│                                              /               \                   │
│                                            YES                NO                 │
│                                             │                  │                 │
│                                             ▼                  ▼                 │
│                              ┌──────────────────────┐   ┌──────────────────────┐  │
│                              │ ToolMatcher /       │   │ SQL Fallback         │  │
│                              │ ToolBasedExecutor   │   │ EligibilityPolicy    │  │
│                              └──────────┬───────────┘   └──────────┬───────────┘  │
│                                         │                         │              │
│                         ┌───────────────┼──────────────┐          │              │
│                         │               │              │          │              │
│                         ▼               ▼              ▼          │              │
│                   TrafficTool   ConnectionsTool  BandwidthTool    │              │
│                                         │              │          │              │
│                                         └──── ProtocolTool        │              │
│                         │                                      eligible?         │
│                         ▼                                      /      \          │
│                RestApiClient                                 NO        YES        │
│                         │                                     │          │        │
│                         ▼                                     ▼          ▼        │
│                    existing                               controlled  Query       │
│                    rest-api                               response    Requirements│
│                         │                                                │        │
│                         ▼                                                ▼        │
│                PostgreSQL / StarRocks                      SemanticSchemaCatalog  │
│                                                                         │        │
│                                                          ┌──────────────┴─────┐  │
│                                                          │                    │  │
│                                                          ▼                    ▼  │
│                                                 PostgreSQL schema      StarRocks   │
│                                                 capabilities           schema      │
│                                                          │                    │  │
│                                                          └─────────┬──────────┘  │
│                                                                    ▼             │
│                                                        DataSourceRouter           │
│                                                                    │             │
│                                                      ┌─────────────┴──────────┐  │
│                                                      ▼                        ▼  │
│                                                 POSTGRESQL                STARROCKS│
│                                                      │                        │  │
│                                                      └────────────┬───────────┘  │
│                                                                   ▼              │
│                                                    SQL Generation Context        │
│                                                    selected schema only          │
│                                                                   │              │
│                                                                   ▼              │
│                                                       InternalLlmClient          │
│                                                           NL → SQL               │
│                                                                   │              │
│                                                                   ▼              │
│                                                       Generated SQL              │
│                                                                   │              │
│                                                                   ▼              │
│                                                       SQL Parser / AST           │
│                                                                   │              │
│                                                                   ▼              │
│                                                       SQL Policy Validator       │
│                                                                   │              │
│                                                                   ▼              │
│                                                       EXPLAIN / Cost Check       │
│                                                                   │              │
│                                                                   ▼              │
│                                                       ReadOnlySqlExecutor        │
│                                                                   │              │
│                                                      ┌────────────┴──────────┐   │
│                                                      ▼                       ▼   │
│                                                 PostgreSQL               StarRocks│
│                                                                                  │
│                         ┌──────────────────────────────────────────────────────┐   │
│                         │                Query Result                         │   │
│                         └──────────────────────────┬───────────────────────────┘   │
│                                                    │                              │
│                                                    ▼                              │
│                                        ┌────────────────────────┐                 │
│                                        │ ExplanationService     │                 │
│                                        └────────────┬───────────┘                 │
│                                                     │                             │
│                                                     ▼                             │
│                                        Internal company LLM                       │
│                                        result → short explanation                 │
│                                                     │                             │
│                                                     ▼                             │
│                                        ┌────────────────────────┐                 │
│                                        │ ResponseFormatter      │                 │
│                                        └────────────┬───────────┘                 │
│                                                     │                             │
│  ┌──────────────────────────────────────────────────┴──────────────────────────┐  │
│  │                         Observability                                      │  │
│  │                                                                            │  │
│  │ AnalyticsQueryMetrics → Micrometer → Actuator → /actuator/prometheus      │  │
│  │                                                   ↓                        │  │
│  │                                              Prometheus                     │  │
│  │                                                   ↓                        │  │
│  │                                                Grafana                      │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

The architecture retains the agreed tool-first behavior and guarded SQL fallback, but moves the feature into its own deployable module. The semantic catalog and deterministic datasource router select PostgreSQL or StarRocks before SQL generation; the generated SQL is then independently validated before execution.

---

# Final Contracts and Requirements

These are the decisions that should be treated as fixed requirements for V1.

## 1. Module contract

The module is:

**`analytics-query`**

It is:

* a new Maven module in the existing multi-module project;
* written in Java 17;
* built with Maven;
* a standalone Spring Boot 3.4.3 application;
* independently containerized and deployed;
* integrated into the existing Nimbo Helm and Jenkins conventions;
* separate from `rest-api`;
* allowed to call `rest-api` for existing approved analytics functionality;
* allowed to directly query PostgreSQL or StarRocks only through the guarded read-only SQL fallback path.

Spring is used for infrastructure: Spring MVC, dependency injection, configuration properties, validation, HTTP clients, Actuator, Micrometer and datasource lifecycle. We deliberately do not need JPA/Hibernate, Spring Batch, WebFlux, Spring Cloud or other unnecessary Spring layers.

---

# 2. API contract

The module exposes three ordinary API endpoints.

### Main analytics endpoint

**`POST /api/analytics/query`**

Input:

* natural-language analytics question.

Responsibilities:

* interpret the question;
* validate its meaning;
* determine whether an approved tool can answer it;
* prefer the tool path;
* use guarded SQL fallback only when necessary and allowed;
* execute the request;
* optionally generate a short explanation;
* return structured data plus metadata.

This is the primary application endpoint.

---

### SQL-generation endpoint

**`POST /api/analytics/sql/generate`**

Input:

* natural-language analytics question.

Responsibilities:

* understand what information the question requires;
* check SQL-fallback eligibility;
* choose the datasource;
* expose only the chosen datasource's semantic schema to the LLM;
* generate SQL;
* parse and validate it;
* return the generated SQL and selected datasource.

It must **not execute the SQL**.

---

### SQL-execution endpoint

**`POST /api/analytics/sql/execute`**

Input:

* SQL query.

Responsibilities:

* parse it;
* inspect the AST;
* identify referenced tables;
* determine or verify the datasource;
* enforce the same SQL policy used by generated SQL;
* perform safety/cost checks;
* execute using restricted read-only credentials;
* return the result.

The three endpoints remain ordinary endpoints of the same application and reuse the same underlying services rather than creating three separate implementations.

---

# 3. Main response contract

A successful response from the main endpoint follows this semantic shape:

```text
status
executionMode
dataSource
data
explanation
```

Example semantics:

```text
status:
    SUCCESS

executionMode:
    TOOL
    or
    GENERATED_SQL

dataSource:
    POSTGRESQL
    or
    STARROCKS

data:
    authoritative structured result

explanation:
    short LLM-generated interpretation of the result
```

The raw structured `data` is always authoritative.

The `explanation` is supplementary and must never replace, alter or contradict the returned data. This contract was explicitly agreed during the naming and explanation design.

---

# 4. Result status contract

The application should support controlled statuses including:

* `SUCCESS`
* `NEEDS_CLARIFICATION`
* `UNSUPPORTED_ANALYSIS`
* `OUT_OF_SCOPE`
* `REJECTED`
* `QUERY_REJECTED`
* `DEPENDENCY_UNAVAILABLE`
* `EXECUTION_FAILED`

The Java application determines these statuses. Free-form LLM wording does not determine the system state.

---

# 5. LLM contract

Only the company's **internal LLM** is allowed.

It is hosted on company infrastructure and will be accessed using the credentials/API information provided by the company.

There is no external-cloud LLM fallback.

The LLM has three logically different responsibilities:

### Intent interpretation

Natural language → structured `AnalysisIntent`.

It receives only what is necessary to understand the supported analytics domain.

### SQL generation

Natural language + selected datasource + approved semantic schema → candidate SQL.

It does not choose the physical datasource.

### Result explanation

Question + bounded query result + relevant semantics → short human-readable explanation.

It does not recalculate the result or invent additional data.

The internal LLM implementation is hidden behind one application abstraction so API-specific behavior stays isolated. The internal API information required before final integration includes its base URL, authentication format, model identifier, request/response schema, structured-output support and timeout characteristics.

---

# 6. Explanation contract

Explanation happens **after successful query execution**.

```text
question
+
actual structured result
+
relevant context
        ↓
internal LLM
        ↓
short data-analysis explanation
```

Requirements:

* explanation is based only on the returned result and known semantics;
* result data remains authoritative;
* explanation must not introduce unsupported security conclusions;
* only a bounded amount of result data is sent;
* explanation generation has its own timeout;
* if explanation generation fails, the successful query still returns `SUCCESS`;
* `explanation` may be `null` when that step fails.

This failure behavior is deliberate and prevents a nonessential presentation step from invalidating a successful database operation.

---

# 7. Tool-first execution contract

Known analytics functionality must use approved existing APIs before considering generated SQL.

Initial tool families are based on existing Nimbo functionality:

* traffic;
* connections;
* bandwidth;
* protocols.

Potential supported intent families include traffic by IP, connections by IP, top bandwidth IPs, protocol usage, top connections, top destinations, top sources and ranked IP activity.

A tool should only be registered if `rest-api` can actually satisfy that operation correctly.

Because `analytics-query` is now a separate application, tools call existing `rest-api` endpoints through a centralized HTTP client layer.

Most importantly:

> A failed tool execution does not trigger SQL fallback.

SQL fallback exists because no approved tool can satisfy an otherwise valid request, not because an existing tool or dependency happened to fail.

---

# 8. SQL-fallback eligibility contract

No tool does **not** automatically mean generated SQL.

The question must be:

* inside the analytics domain;
* read-only;
* sufficiently unambiguous;
* answerable from known semantic data;
* answerable using one datasource;
* within supported time constraints;
* one logical analysis operation for V1.

If those conditions do not hold, return the relevant clarification, unsupported, out-of-scope or rejected status.

---

# 9. Semantic schema contract

The semantic catalog is application-owned metadata describing what the available tables mean.

### PostgreSQL

The V1 catalog includes the four analytics aggregate families:

* `traffic_aggregates`
* `connection_aggregates`
* `ip_bandwidth_aggregates`
* `protocol_usage_aggregates`

### StarRocks

The V1 catalog includes:

* `nimbo.netflow`

Each catalog entry should describe:

* datasource;
* table;
* columns;
* types;
* grain;
* time column;
* dimensions;
* metrics;
* relevant relationships;
* supported capabilities.

The semantic catalog allows routing based on what information can actually be answered rather than simply matching table names.

---

# 10. Datasource-routing contract

The physical datasource is chosen by deterministic Java application logic.

The LLM does not choose PostgreSQL or StarRocks.

Conceptually:

```text
QueryRequirements
       +
SemanticSchemaCatalog
       ↓
DataSourceRouter
       ↓
POSTGRESQL or STARROCKS
```

Only after this choice is made is the SQL-generation LLM invoked.

The selected datasource's schema is then the only schema included in the SQL-generation context.

---

# 11. SQL generation contract

For PostgreSQL-selected requests, the LLM receives only approved PostgreSQL analytics tables and their semantics.

For StarRocks-selected requests, it receives only approved StarRocks tables and their semantics.

The LLM produces a candidate query.

That query has **zero execution authority** until it passes the application's parser, structural checks, policy checks and cost checks.

---

# 12. SQL validation contract

Generated SQL and SQL submitted to `/sql/execute` are treated as untrusted input.

Validation should use a mature Java SQL parser library rather than regexes or manual parsing.

The first parser to evaluate is **JSqlParser**.

The AST-based validator must ensure at minimum:

* exactly one statement;
* read-only query;
* no `INSERT`;
* no `UPDATE`;
* no `DELETE`;
* no `MERGE`;
* no `CREATE`;
* no `ALTER`;
* no `DROP`;
* no `TRUNCATE`;
* no prohibited procedure/function calls;
* no dangerous copy/import operations;
* no system/catalog access;
* only allowlisted tables;
* no cross-PostgreSQL/StarRocks query;
* allowed functions;
* allowed joins;
* bounded result size;
* appropriate time bounds where required.

The parser also verifies that the generated query stayed inside the datasource already selected by the router.

---

# 13. SQL execution contract

SQL execution uses separate read-only database identities.

The executor must enforce:

* restricted database privileges;
* read-only behavior;
* bounded connection pools;
* statement timeout;
* maximum returned rows;
* maximum response payload;
* query cancellation on timeout;
* no cross-database SQL execution.

JDBC/JDBI plus HikariCP should be used instead of JPA/Hibernate because queries are dynamic.

---

# 14. Query-cost contract

Valid SQL does not automatically mean acceptable SQL.

Before execution, the system should use a datasource-appropriate `EXPLAIN`/plan check where feasible.

The V1 goal is conservative protection against obviously expensive queries, not building a sophisticated optimizer.

Thresholds should be configuration-driven.

---

# 15. Time contract

All internal time semantics remain **UTC**, consistent with the rest of Nimbo.

Natural-language expressions such as:

* last hour;
* today;
* yesterday;
* previous 30 minutes;

must ultimately become explicit UTC ranges before execution.

---

# 16. Library-first contract

Infrastructure capabilities should not be reimplemented if mature built-in or established libraries already solve them.

Preferred choices:

| Need                   | Preferred solution                   |
| ---------------------- | ------------------------------------ |
| REST API               | Spring MVC                           |
| JSON                   | Jackson                              |
| request validation     | Jakarta Bean Validation              |
| configuration          | Spring `@ConfigurationProperties`    |
| dependency injection   | Spring                               |
| outbound HTTP          | Spring `RestClient`                  |
| internal LLM transport | `RestClient` around company API      |
| SQL parsing            | JSqlParser initially                 |
| JDBC pooling           | HikariCP                             |
| dynamic SQL execution  | JDBC/JDBI                            |
| resilience             | Resilience4j where appropriate       |
| metrics                | Micrometer                           |
| Prometheus exposure    | Spring Boot Actuator                 |
| OpenAPI                | Springdoc/current project convention |
| unit testing           | JUnit 5, Mockito, AssertJ            |
| integration testing    | Testcontainers                       |
| container build        | Jib                                  |
| deployment             | Helm                                 |
| CI/CD                  | existing Jenkins architecture        |

Custom code should concentrate on actual Nimbo domain behavior: intent semantics, planning, capability mapping, routing and policy.

---

# 17. Metrics contract

The new module must contain a dedicated metrics component, for example conceptually:

**`AnalyticsQueryMetrics`**

Metric families should include:

* request count by controlled status;
* execution count by mode and datasource;
* LLM duration;
* tool/API duration;
* SQL-generation duration;
* SQL-validation duration;
* SQL-execution duration;
* SQL rejection count and controlled reason;
* explanation duration;
* explanation failure count;
* general failure count by stage;
* active/in-flight requests.

Metrics must use low-cardinality labels.

Questions, SQL strings, IP addresses, request IDs and raw exception messages must never become metric labels.

---

# 18. Prometheus contract

Use:

* Spring Boot Actuator;
* Micrometer Prometheus registry.

Expose at least:

```text
/actuator/health
/actuator/prometheus
```

Prometheus should scrape the module using the same mechanism already established by Nimbo rather than creating a special monitoring solution.

---

# 19. Grafana contract

Create a Git-managed dashboard:

**Nimbo — Analytics Query**

It should show at least:

* request rate;
* status distribution;
* success/failure rate;
* tool versus generated-SQL executions;
* PostgreSQL versus StarRocks executions;
* LLM latency;
* SQL-generation latency;
* SQL-validation latency;
* SQL-execution latency;
* explanation latency;
* SQL rejection reasons;
* dependency failures;
* active requests.

---

# 20. Configuration and secrets contract

Configuration should be externalized.

Configuration categories include:

* internal LLM URL;
* internal LLM model;
* LLM timeouts;
* `rest-api` URL;
* PostgreSQL read-only connection;
* StarRocks read-only connection;
* question-length limit;
* SQL-length limit;
* result-row limit;
* payload limit;
* statement timeout;
* cost thresholds;
* concurrency limits.

Sensitive values such as:

* LLM API key;
* database passwords;

belong in Kubernetes Secrets, never committed source files or plain Helm values.

---

# 21. Failure contract

Expected behavior includes:

```text
LLM unavailable before intent generation
→ DEPENDENCY_UNAVAILABLE

rest-api tool unavailable
→ tool/dependency failure
→ DO NOT try SQL fallback

generated SQL rejected
→ QUERY_REJECTED
→ do not execute

PostgreSQL unavailable
→ EXECUTION_FAILED / dependency failure

StarRocks unavailable
→ EXECUTION_FAILED / dependency failure

explanation LLM call fails after successful query
→ SUCCESS
→ data returned
→ explanation = null
```

---

# 22. V1 scope boundaries

The following are deliberately not part of this six-day implementation:

* multi-operation planning;
* conversational memory;
* agent loops;
* automatic self-correcting SQL loops;
* LLM-selected physical datasource;
* cross-database generated SQL;
* PostgreSQL + StarRocks result composition;
* arbitrary database/schema access;
* writes;
* arbitrary administrative SQL;
* unsupported malware/anomaly/security conclusions;
* rich conversational frontend;
* streaming responses.

The first release remains a bounded analytics-query service, not a general-purpose AI agent.

---

# Phase 1 — Day 1: Module Foundation, API Contracts and Internal LLM Integration

## Objective

Finish Day 1 with a standalone `analytics-query` application that builds, starts, exposes the three API contracts and can convert natural language into a validated structured intent.

No database querying is required yet.

### Phase 1.1 — Maven module

Requirements:

* create the `analytics-query` Maven module;
* register it in the project's root reactor;
* follow the existing dependency-management and plugin conventions;
* use Java 17 and Spring Boot 3.4.3;
* inherit existing Maven/Sonar/test conventions rather than duplicating configuration;
* only depend on `common` if there is genuinely reusable code there;
* do not introduce a compile-time dependency on `rest-api`.

Validation:

* reactor build recognizes the module;
* module compiles independently;
* test lifecycle runs through Maven normally.

---

### Phase 1.2 — Spring Boot application foundation

Requirements:

Include only the capabilities required for this service:

* Spring MVC;
* Bean Validation;
* Jackson;
* configuration properties;
* Spring HTTP client;
* Actuator;
* Micrometer;
* Prometheus registry;
* JDBC;
* HikariCP;
* testing dependencies.

Avoid unrelated Spring components such as JPA/Hibernate.

Create initial health/readiness configuration so operational integration does not become an end-of-project surprise.

---

### Phase 1.3 — Application structure

Establish clear packages/boundaries for:

* controllers;
* API request/response models;
* intent models;
* query-requirement and capability models;
* LLM integration;
* planner;
* tools;
* `rest-api` clients;
* semantic schema;
* routing;
* SQL generation/validation/execution;
* explanation;
* metrics;
* configuration;
* exception handling.

The intention is maintainability, not maximum layering. Avoid empty abstractions that serve no actual purpose. Keep `QueryRequirements` near the intent/planning boundary initially; create a separate `requirements` package only if the number of cohesive requirement types makes that separation useful.

---

### Phase 1.4 — Three API contracts

Implement the HTTP contract for:

* `POST /api/analytics/query`
* `POST /api/analytics/sql/generate`
* `POST /api/analytics/sql/execute`

At this stage some lower layers may be stubbed, but:

* request JSON should deserialize;
* request validation should work;
* response formats should be fixed;
* error responses should follow one consistent structure;
* OpenAPI should already recognize all three endpoints.

---

### Phase 1.5 — Status and result contracts

Finalize:

* application statuses;
* execution modes;
* datasource enum;
* main response format;
* SQL-generation response;
* SQL-execution response;
* consistent error response.

Avoid changing these casually later because tests, UI, Swagger and clients will depend on them.

---

### Phase 1.6 — Internal LLM adapter

Create the abstraction around the company's internal LLM.

Requirements:

* isolate base URL/authentication/model details;
* support intent generation;
* leave the same abstraction ready for SQL generation and explanation;
* configure explicit timeout;
* never hardcode credentials;
* support structured JSON output if the internal model API provides it;
* otherwise strictly parse JSON into typed DTOs.

Once the company's actual API/key/OpenCode-related details are received, only this adapter/configuration layer should need protocol-specific changes.

---

### Phase 1.7 — Intent interpretation

Implement structured intent interpretation for the V1 analytics domain without requiring classification into a fixed `AnalyticsOperation` enum.

Keep this flow:

```text
natural-language question
→ LLM structured intent
→ AnalysisIntent
→ IntentValidator
→ status gate
→ planner only for SUCCESS
```

`AnalysisIntent` should contain only:

* status;
* query requirements;
* clarification.

`QueryRequirements` is the core semantic representation of the request and should describe:

* dimensions;
* metrics and their aggregations;
* filters;
* IP, source, destination and direction semantics;
* a bounded time range;
* grouping and granularity;
* sorting and ranking;
* limit;
* distinct requirements.

During intent interpretation, the model must not return SQL, datasource selection, database/table/column names, endpoints, tool names or executable instructions. Fixed known operations remain planner/tool capabilities; they do not limit all valid analytics questions.

StarRocks fields exposed during interpretation must come only from the curated semantic catalog. Physical column existence alone does not make a field available to the model.

Use only explicit, meaning-preserving defaults during interpretation:

* an omitted optional filter means all applicable values;
* an omitted traffic direction is `UNSPECIFIED`;
* an omitted granularity is `null`;
* a top/bottom request without a limit uses `10`;
* "recent", "current" or "latest" without a precise range means the last hour.

Do not invent defaults for a completely missing time context, ambiguous IP role, metric or analysis meaning. Return `NEEDS_CLARIFICATION` in those cases. The prompt must list these defaults explicitly rather than asking the model to apply unspecified "sensible defaults".

---

### Phase 1.8 — Intent validation

Validate model output after parsing.

Do not add or retain a regex-based `IntentSafetyPolicy` that attempts to classify the raw natural-language question. Destructive requests and unsupported security conclusions are classified into the structured intent statuses by the intent-interpretation contract; `IntentValidator` validates the resulting structure and status consistency. Regex matching is not reliable enough to act as an NLP security boundary.

Validate:

* missing fields;
* status is one of the allowed intent-stage statuses;
* contradictory fields;
* invalid or unsupported semantic requirement values;
* invalid time ranges;
* illegal limits;
* invalid semantic-catalog fields or aggregations;
* invalid IP/CIDR values;
* clarification consistency when the model reports ambiguity;
* `UNSUPPORTED_ANALYSIS`, `OUT_OF_SCOPE` or `REJECTED` containing query requirements or clarification data.

Only `SUCCESS` may proceed to planning or execution. The main orchestration must enforce this gate before invoking `ToolMatcher` or SQL-fallback eligibility; `NEEDS_CLARIFICATION`, `UNSUPPORTED_ANALYSIS`, `OUT_OF_SCOPE` and `REJECTED` return controlled responses immediately.

For `NEEDS_CLARIFICATION`, return the validated `clarification.question` as the response message. V1 does not keep conversational state or continue automatically; the user submits a revised, complete question.

Use the existing mature mechanisms at their proper layers:

* Jakarta Bean Validation for structural constraints;
* strict Jackson deserialization for malformed or unexpected LLM fields;
* the curated semantic catalog and explicit Java checks for NetFlow-specific cross-field rules;
* JSqlParser, the SQL policy and a read-only database identity later for enforceable SQL safety.

Company gateway guardrails may be used as defense in depth when their actual configuration and guarantees are known, but they do not replace the NetFlow intent contract or Java validation. Do not add another dependency solely for the current small IPv4/CIDR validation; reconsider a dedicated IP-address library if IPv6 or richer network semantics enter scope.

---

### Phase 1.9 — Day 1 tests

Cover:

* valid analytics request;
* paraphrased valid request;
* approved safe defaults;
* ambiguous request;
* missing essential information returning the expected clarification question;
* out-of-domain question;
* write/destructive request;
* unsupported inference;
* malformed LLM JSON;
* LLM timeout;
* blank/oversized HTTP request.

### Day 1 exit criteria

By end of Day 1:

* `analytics-query` exists as a working Maven/Spring Boot service;
* `mvn verify` works for the module;
* all three endpoints exist;
* validation works;
* internal LLM abstraction works or has a production-realistic mock until credentials arrive;
* natural language can produce a validated `AnalysisIntent`;
* unsafe requests cannot proceed further.

---

# Phase 2 — Day 2: Tool-First Analytics Execution

## Objective

Deliver the first genuinely useful end-to-end feature before generated SQL exists.

A supported natural-language request should reach the existing Nimbo APIs and return real structured analytics data.

### Phase 2.1 — Inspect existing REST capabilities

Before coding adapters, confirm the exact contracts of:

* traffic;
* connections;
* bandwidth;
* protocol endpoints.

Determine:

* supported filters;
* time-range semantics;
* pagination/limits;
* response fields;
* PostgreSQL/StarRocks behavior already hidden behind those APIs;
* error behavior.

The tool catalog must reflect actual existing capabilities rather than assumptions.

---

### Phase 2.2 — REST client layer

Implement centralized Spring HTTP clients for `rest-api`.

Requirements:

* one common base configuration;
* configured connection/read timeouts;
* consistent serialization;
* controlled error mapping;
* optional Resilience4j behavior if consistent with the existing project;
* no HTTP details scattered throughout tool classes.

---

### Phase 2.3 — Tool abstractions

Implement tool adapters for the supported existing API families.

Each tool should:

* declare the semantic requirements it can satisfy;
* convert matched query requirements into the appropriate REST request;
* invoke only its predefined read-only PostgreSQL-backed REST operation;
* execute the request;
* normalize the returned result into the module's `AnalyticsResult` format.

Avoid duplicating business logic already implemented by `rest-api`.

---

### Phase 2.4 — Tool registry and matcher

Create a `ToolMatcher` over the registry of approved tools. It deterministically checks whether a tool's finite capabilities satisfy the validated `QueryRequirements`; it does not classify every successful request into a fixed operation enum.

Requirements:

* deterministic selection;
* no LLM decision about Java tool classes;
* no matching capability means no tool and may proceed to SQL-fallback eligibility;
* duplicate conflicting registrations detected at startup or tests.

Keep matching and registry lookup together initially; do not add another capability-checking abstraction unless this responsibility genuinely grows.

---

### Phase 2.5 — Main planner

The planner now decides:

```text
validated AnalysisIntent
    ↓
status == SUCCESS?
    ├── NO  → return its controlled status; execute nothing
    └── YES
         ↓
validated query requirements
    ↓
ToolMatcher finds an approved tool?
    ↓
YES
    ↓
execute approved tool
```

This status check is the mandatory intent-policy enforcement point. It must run before tool matching, REST calls, SQL generation, datasource routing or database access.

For Day 2, no-tool cases may still return unsupported because SQL fallback is introduced on Day 3.

Once fallback is available, the planner sends a no-match result through SQL eligibility, checks the requirements against the semantic catalog, and delegates PostgreSQL/StarRocks selection to the deterministic Java router. Tool failure or an empty successful tool result does not trigger SQL fallback.

---

### Phase 2.6 — Normalize tool results

Create one internal result format so later explanation, metrics and response formatting do not care whether the source was:

* REST tool;
* generated SQL.

Include:

* datasource where known;
* execution mode;
* column names;
* rows;
* row count;
* relevant metadata.

---

### Phase 2.7 — Failure behavior

Implement the permanent rule now:

> If an approved tool exists but fails, the application reports that failure. It does not generate SQL as a backup.

Test:

* every non-`SUCCESS` status returns without invoking the tool matcher, REST client or SQL fallback;
* REST timeout;
* 5xx response;
* zero results;
* malformed downstream response.

Zero rows are valid results, not fallback triggers.

### Day 2 exit criteria

A request such as an existing traffic/bandwidth/connections/protocol query works:

```text
natural language
→ intent
→ planner
→ tool
→ rest-api
→ result
→ API response
```

The feature is usable even though SQL fallback is not finished.

---

# Phase 3 — Day 3: Semantic Catalog, Datasource Routing and NL→SQL Generation

## Objective

Implement the safe decision-making layer required before any generated SQL can be executed.

By end of Day 3, `/api/analytics/sql/generate` should work completely, including SQL validation, but execution can remain for Day 4.

### Phase 3.1 — Semantic schema catalog

Build the authoritative semantic description of:

**PostgreSQL**

* traffic aggregates;
* connection aggregates;
* IP bandwidth aggregates;
* protocol usage aggregates.

**StarRocks**

* `nimbo.netflow`.

For every table document:

* exact table name;
* datasource;
* available columns;
* SQL types;
* business meaning;
* aggregation grain;
* timestamp column;
* available dimensions;
* available metrics;
* meaningful table relationships;
* analyses that can/cannot be answered correctly.

Do not expose staging tables, state tables, PostgreSQL catalogs or unrelated schemas.

Use two catalog sources behind the same semantic-catalog contract:

* PostgreSQL aggregate metadata is curated and fixed from the final migrated cube schema owned by this project.
* StarRocks physical table/column metadata is discovered from `INFORMATION_SCHEMA` at startup and every 15 minutes by default.

StarRocks remains authoritative for physical column existence, SQL type and nullability. Column comments are used as meanings when available; otherwise meanings and conservative semantic roles are derived deterministically from established NetFlow naming conventions. Optional application-owned overrides are reserved for exceptional or ambiguous fields rather than required for every column. Columns whose role cannot be inferred safely are not exposed. Publish changed catalog snapshots atomically, and retain the last known-good snapshot if a periodic refresh fails.

---

### Phase 3.2 — Query requirement model

Represent what answering a question requires independently from physical schema.

Requirements can describe:

* dimensions;
* metrics and aggregation needs;
* filters;
* IP, source, destination and direction semantics;
* distinct-count needs;
* grouping and optional temporal bucket granularity;
* requested time range;
* sorting/ranking;
* limits.

Use a small typed vocabulary for dimensions, metrics, granularity, aggregation and time range rather than matching words in the original question. `QueryRequirements` is the validated object passed from intent interpretation to the planner and datasource router.

Do not add a raw-versus-aggregated datasource hint to `QueryRequirements`.
Phase 3.4 determines whether a table preserves the requested semantics by
comparing these requirements with the catalog's table grain and fields.

---

### Phase 3.3 — SQL fallback eligibility

Determine whether an otherwise unsupported request is allowed into NL→SQL.

Reject or clarify requests that:

* are ambiguous;
* are outside network analytics;
* require unavailable information;
* imply writes;
* require unsupported inference;
* need multiple physical databases;
* require multiple logical operations beyond V1 scope.

---

### Phase 3.4 — Deterministic datasource router

Implement routing based on:

* `QueryRequirements`;
* `SemanticSchemaCatalog`.

General intention:

* use PostgreSQL when its aggregates correctly preserve the semantics required;
* use StarRocks when raw/recent dimensions are needed and the aggregate tables cannot correctly answer the question.

The router—not the LLM—owns the physical datasource decision.

Add comprehensive routing tests because incorrect routing can produce logically incorrect results even if generated SQL is syntactically valid.

---

### Phase 3.5 — SQL generation context

After routing, construct a context containing only:

* selected datasource;
* allowlisted tables for that datasource;
* allowlisted columns;
* column meanings;
* grain;
* time semantics;
* relationships;
* query restrictions.

Never provide both entire database schemas and ask the model to choose.

---

### Phase 3.6 — NL→SQL generation

Use the internal company LLM to produce candidate SQL.

Requirements:

* structured response rather than arbitrary prose where possible;
* preserve selected datasource;
* enforce maximum generated SQL size;
* explicit instruction for one read-only query;
* no schema discovery;
* only catalog-approved tables and columns;
* no invented tables or columns.

---

### Phase 3.7 — SQL parser integration

Evaluate and integrate a mature Java parser, starting with JSqlParser.

Verify it against the exact restricted query syntax required for:

* PostgreSQL;
* StarRocks.

Do not write an SQL parser manually.

---

### Phase 3.8 — First-stage SQL validation

Parse generated SQL and verify:

* syntactic parse success;
* one statement;
* supported query statement type;
* referenced tables can be extracted;
* all tables belong to the selected datasource;
* no unknown tables.

Full policy and execution hardening continue Day 4.

---

### Phase 3.9 — Complete `/sql/generate`

The endpoint flow becomes:

```text
question
→ intent / requirements
→ fallback eligibility
→ schema catalog
→ datasource router
→ LLM
→ SQL parser
→ validation
→ response
```

The endpoint does not execute the query.

### Day 3 exit criteria

* semantic catalog is complete for V1;
* routing tests pass;
* fallback eligibility works;
* SQL generator sees only one datasource's schema;
* generated SQL is parsed;
* `/api/analytics/sql/generate` works end-to-end;
* no generated SQL can yet bypass validation into a database.

---

# Phase 4 — Day 4: SQL Policy, Safe Execution and LLM Explanation

## Objective

Finish the complete fallback path and make all three API endpoints operational.

### Phase 4.1 — Full AST policy validation

Expand the SQL validator to enforce all V1 SQL policy.

Requirements:

* one statement;
* SELECT/read-only queries only;
* allowlisted tables only;
* selected datasource only;
* allowed functions;
* controlled joins;
* no database administration;
* no system/catalog access;
* bounded time where required;
* bounded result set;
* no cross-datasource references.

Validation failures must produce controlled reasons suitable for logs/metrics without exposing excessive internals.

---

### Phase 4.2 — Read-only identities

Provision/configure dedicated credentials for generated SQL.

PostgreSQL and StarRocks credentials must have only the minimum read privileges needed for the approved analytics schema.

For PostgreSQL schema/role changes managed through the application's schema process, create a new Flyway migration rather than changing an existing migration.

---

### Phase 4.3 — Datasource executors

Implement the datasource-specific JDBC execution behind a common abstraction.

Requirements:

* HikariCP;
* small bounded pools;
* read-only mode where supported;
* prepared/session controls where possible;
* maximum result rows;
* maximum serialized result size;
* statement timeout;
* cancellation/cleanup after timeout;
* resource closure guaranteed.

Avoid JPA because these are dynamic queries.

---

### Phase 4.4 — Query plan safety

Implement datasource-appropriate `EXPLAIN` safety checks where practical.

V1 requirements:

* configurable limits;
* reject clearly pathological plans;
* log controlled rejection reason;
* do not pretend PostgreSQL and StarRocks expose identical plan semantics;
* avoid building sophisticated cost modelling.

---

### Phase 4.5 — Complete `/sql/execute`

Endpoint flow:

```text
submitted SQL
→ parse
→ extract tables
→ identify/verify datasource
→ policy validation
→ EXPLAIN check
→ read-only executor
→ normalized result
```

This endpoint must use exactly the same validation/execution components used by the main `/query` SQL-fallback path.

---

### Phase 4.6 — Full main SQL fallback

Connect the main orchestration:

```text
no tool
→ fallback eligible
→ requirements
→ schema catalog
→ datasource routing
→ SQL generation
→ validation
→ EXPLAIN
→ read-only execution
→ result
```

A failed existing tool still does not reach this path.

---

### Phase 4.7 — Explanation service

After a successful tool or SQL result, call the internal LLM with:

* original question;
* bounded result rows;
* relevant column semantics;
* requested time range where useful.

Requirements:

* short data-analysis style explanation;
* factual language;
* no unsupported conclusions;
* no changing/recomputing result data;
* explicit timeout;
* bounded payload to the LLM.

If explanation fails:

```text
query result remains SUCCESS
explanation = null
```

---

### Phase 4.8 — All endpoint integration tests

Test:

* tool main endpoint;
* SQL-fallback main endpoint;
* `/sql/generate`;
* `/sql/execute`;
* PostgreSQL execution;
* StarRocks execution;
* write SQL rejection;
* multi-statement rejection;
* system-table rejection;
* unknown-table rejection;
* cross-datasource rejection;
* query timeout;
* explanation failure.

### Day 4 exit criteria

All three endpoints perform their final intended function safely.

The complete primary flow now works from natural-language request through execution and explanation.

---

# Phase 5 — Day 5: Production Hardening, Metrics, Prometheus, Grafana and Comprehensive Testing

## Objective

Turn the functional service into a production-operable Nimbo component.

### Phase 5.1 — Explicit timeout strategy

Configure independent timeouts for:

* intent LLM request;
* SQL-generation LLM request;
* explanation LLM request;
* `rest-api` tool request;
* PostgreSQL execution;
* StarRocks execution;
* query-plan inspection;
* overall HTTP request where appropriate.

Avoid accidental long-lived requests.

---

### Phase 5.2 — Resilience behavior

Use established libraries such as Resilience4j where beneficial and consistent with Nimbo.

Define deliberately:

* which dependencies may retry;
* which errors are non-retryable;
* circuit-breaker behavior if needed;
* no retries that can accidentally execute duplicate expensive SQL unnecessarily.

---

### Phase 5.3 — Concurrency and resource protection

Because LLM requests can be long-running:

* bound concurrent analytics operations;
* keep DB pools intentionally small;
* ensure HTTP client resources are bounded;
* add request/result limits;
* ensure the service cannot overwhelm `rest-api`, PostgreSQL, StarRocks or the internal LLM.

---

### Phase 5.4 — Central metrics component

Implement `AnalyticsQueryMetrics` using Micrometer.

Instrument:

* request totals;
* statuses;
* execution mode;
* datasource;
* intent/LLM latency;
* tool latency;
* SQL-generation latency;
* SQL-validation latency;
* SQL-execution latency;
* SQL rejections;
* explanation latency/failures;
* failures by controlled stage;
* active request gauge.

Metric naming and labels must remain low-cardinality.

---

### Phase 5.5 — Actuator / Prometheus exposure

Configure:

* health endpoint;
* readiness/liveness endpoint behavior;
* Prometheus actuator endpoint;
* Micrometer Prometheus registry.

Verify the custom metrics physically appear in `/actuator/prometheus`.

---

### Phase 5.6 — Grafana dashboard

Create the source-controlled **Nimbo — Analytics Query** dashboard.

Minimum panels:

* request rate;
* status distribution;
* success/error trends;
* tool vs generated-SQL ratio;
* PostgreSQL vs StarRocks ratio;
* LLM latency percentiles;
* SQL generation latency;
* SQL validation latency;
* DB execution latency;
* explanation latency;
* SQL rejection breakdown;
* dependency failures;
* active requests.

Follow the existing repository convention for Grafana rather than creating a parallel provisioning mechanism.

---

### Phase 5.7 — Structured logging

Log useful low-risk operational fields such as:

* correlation/request ID;
* matched tool/capability when applicable;
* execution mode;
* selected datasource;
* status;
* stage;
* row count;
* duration;
* controlled rejection category.

Do not routinely log:

* complete prompts;
* raw query results;
* sensitive NetFlow data;
* credentials.

Generated SQL logging should follow the project's security/logging policy rather than being blindly enabled.

---

### Phase 5.8 — Unit and component tests

Cover all core policy logic independently:

* intent validation;
* planner;
* tool registry;
* capability catalog;
* requirements;
* router;
* fallback policy;
* SQL parser integration;
* SQL policy;
* cost gate;
* explanation handling;
* metrics wiring.

---

### Phase 5.9 — Integration tests

Use Testcontainers where practical.

Cover:

* PostgreSQL read-only path;
* relevant StarRocks path if supported in existing test infrastructure;
* REST API tool client;
* query timeout;
* connection failure;
* no rows;
* malformed LLM response;
* internal LLM unavailable;
* rejected SQL never reaching executor.

Mock the internal company LLM in CI so builds are deterministic.

---

### Phase 5.10 — Evaluation/adversarial dataset

Maintain roughly 50–100 representative natural-language requests.

Include:

* valid supported questions;
* paraphrases;
* ambiguous questions;
* unsupported questions;
* out-of-scope questions;
* write/destructive requests;
* prompt-injection attempts;
* SQL-fallback cases;
* routing edge cases.

If Persian support is a project requirement, include both Persian and English before release.

### Phase 5.11 — StarRocks semantic comment enrichment

Make `starrocks-writer`, as the owner of the StarRocks ingestion schema, enrich
`nimbo.netflow` column comments from the active NetFlow Avro schema in Schema
Registry.

Requirements:

* use the configured Schema Registry URL and subject rather than copying the
  bundled initial Avro schema into `analytics-query`;
* match registered Avro fields to physical StarRocks columns by their normalized
  names and use each field's Avro `doc` as its column comment;
* define application-owned comments for Kafka metadata columns that are not part
  of the Avro record;
* include comments when creating new columns and idempotently reconcile missing
  or changed comments on existing columns as part of the writer's schema
  creation/evolution flow;
* keep compatibility and type-evolution policy in the writer rather than
  duplicating it in `analytics-query`, and do not allow comment-enrichment
  failure to stop ingestion;
* retry failed comment reconciliation on the next controlled schema refresh and
  expose a bounded warning or metric;
* verify initial comments, newly added fields and updated Avro documentation with
  integration tests.

StarRocks remains authoritative for physical column existence, type and
nullability. `analytics-query` may prefer these comments as semantic meanings,
but must retain its deterministic fallback for columns whose comments are absent
during deployment, migration or schema-evolution windows.

### Day 5 exit criteria

* functional flows are hardened;
* custom metrics are visible;
* Prometheus-compatible output works;
* Grafana dashboard is ready;
* tests include normal and adversarial behavior;
* dependency failures remain controlled;
* SQL rejection paths are verified not to execute anything.

---

# Phase 6 — Day 6: Packaging, Helm, Jenkins CI/CD and Deployment Verification

## Objective

Make `analytics-query` a first-class deployable Nimbo component.

No major application feature should be introduced on Day 6.

### Phase 6.1 — Jib packaging

Configure Jib consistently with existing application modules.

Requirements:

* project-standard base image;
* correct Java runtime;
* module-specific image name;
* Nexus repository conventions;
* image tagging compatible with existing CD;
* no secrets baked into the image.

---

### Phase 6.2 — Helm chart

Create the new module's Helm chart according to existing application-chart conventions.

Chart must configure:

* image repository/tag;
* replicas;
* service;
* ports;
* CPU/memory requests and limits;
* environment/configuration;
* LLM configuration;
* `rest-api` URL;
* read-only PostgreSQL connection;
* read-only StarRocks connection;
* query limits;
* timeouts;
* health probes;
* Prometheus scraping configuration;
* secret references.

Keep one Service, one load balancer and one IP in this phase. Do not change the
current `server.servlet.context-path: /api` while creating the chart, because it
also affects controllers, Actuator, OpenAPI and Spring-served static resources.
Resolve the browser root-path behavior deliberately with the static UI in Phase
6.10.

---

### Phase 6.3 — Kubernetes Secrets

Ensure sensitive properties are injected from Secrets:

* internal LLM key;
* database credentials;
* any future authentication token for `rest-api`.

Verify rendered Helm manifests never expose secret plaintext.

---

### Phase 6.4 — Liveness and readiness

Wire Spring Boot Actuator health into Kubernetes.

Readiness should represent whether the application can serve requests, without making temporary failure of every external dependency automatically kill the pod unnecessarily.

Liveness should answer whether the process itself is healthy.

Follow existing project conventions wherever already defined.

---

### Phase 6.5 — Prometheus deployment integration

Connect `analytics-query` to the existing Prometheus deployment using the mechanism currently used by Nimbo:

* ServiceMonitor if that is the current standard;
* otherwise existing scrape annotations/configuration.

Verify the target becomes healthy and custom metrics are ingested.

---

### Phase 6.6 — Grafana deployment integration

Provision the dashboard using the project's current Git/Helm/operator mechanism.

Verify panels actually receive data from Prometheus after a few test requests.

---

### Phase 6.7 — Platform/umbrella Helm integration

If Nimbo application modules are included through the umbrella/platform chart:

* add `analytics-query`;
* provide enabled/image/config values;
* maintain dependency/version conventions;
* verify upgrade behavior.

Run:

* Helm lint;
* template rendering;
* dry-run/upgrade validation.

---

### Phase 6.8 — Jenkins CI changes

Update existing CI so the module participates normally in:

```text
Maven verification
→ unit/integration tests
→ SonarQube
→ quality gate
→ JUnit/report publication
```

Ensure module/change detection recognizes modifications under `analytics-query`.

Do not create an unrelated CI architecture.

---

### Phase 6.9 — Jenkins CD changes

Add `analytics-query` as a deployable component to the existing CD model.

Required flow:

```text
detect changed module
→ Jib image build
→ Nexus push
→ Helm image/tag configuration
→ staging deployment
→ validation
→ existing approval gate
→ production deployment
```

Preserve existing Gerrit/Jenkins/Helm conventions.

---

### Phase 6.10 — Simple static UI

Serve the V1 interface directly from `analytics-query` using Spring Boot static resources:

```text
src/main/resources/static/index.html
src/main/resources/static/app.js
src/main/resources/static/style.css
```

The page should submit questions to `POST /api/analytics/query`, render structured `data` separately from the supplementary `explanation`, and provide clear loading and controlled error states. Use plain HTML, CSS and JavaScript; do not add a frontend framework for V1.

When the page first loads, show a short guidance block near the question input. It should ask users to include, when relevant:

* the time range, such as "last hour" or explicit UTC start/end times;
* what to analyze, such as an IP, protocol or all traffic;
* the metric, such as bytes, packets or connections;
* source, destination, either-side or traffic-direction meaning;
* ranking and limit for top/bottom questions.

Keep the guidance compact and include two or three representative NetFlow examples rather than a long instruction manual. When the API returns `NEEDS_CLARIFICATION`, display its message prominently beside the input so the user can revise and resubmit the question. Do not add conversation memory for V1.

Before finalizing the UI route, account for the current
`server.servlet.context-path: /api`, which also prefixes Spring static
resources. Choose and verify a deliberate routing arrangement that serves the
UI from the browser root while preserving the existing `/api/analytics/**`,
Actuator and OpenAPI contracts. Do not solve this by introducing another
Service or load balancer.

---

### Phase 6.11 — Swagger/OpenAPI finalization

Document all three public contracts:

```text
POST /api/analytics/query
POST /api/analytics/sql/generate
POST /api/analytics/sql/execute
```

Include representative:

* requests;
* successful responses;
* clarification;
* unsupported response;
* rejected request;
* rejected SQL;
* dependency failure.

---

### Phase 6.12 — Final deployed smoke test

Verify, in order:

1. pod starts successfully;
2. liveness succeeds;
3. readiness succeeds;
4. `/actuator/prometheus` exposes metrics;
5. supported tool-based analytics request succeeds;
6. PostgreSQL SQL-fallback request succeeds;
7. StarRocks SQL-fallback request succeeds;
8. `/sql/generate` returns safe SQL;
9. `/sql/execute` executes permitted SQL;
10. destructive SQL is rejected;
11. unknown tables are rejected;
12. explanation appears on successful request;
13. forced explanation failure still returns data;
14. Prometheus target is healthy;
15. metrics appear in Prometheus;
16. Grafana dashboard renders the new service's traffic;
17. the static UI loads and successfully renders a representative response;
18. Jenkins CI successfully builds the module;
19. Jenkins CD successfully packages/deploys it.

### Day 6 exit criteria

`analytics-query` is not merely code in the repository. It is a fully buildable, deployable, observable Nimbo service with all three API paths working safely.

---

# Final Definition of Done

At the end of Day 6, the feature should satisfy this complete flow:

```text
Natural-language question
          ↓
Company internal LLM
          ↓
Structured validated intent
          ↓
      Planner
       /   \
      /     \
approved     no approved
 tool           tool
  ↓               ↓
rest-api       SQL eligibility
  ↓               ↓
result        SemanticSchemaCatalog
                  ↓
              DataSourceRouter
                  ↓
         PostgreSQL OR StarRocks
                  ↓
              Internal LLM
                  ↓
              generated SQL
                  ↓
               AST parser
                  ↓
              policy validation
                  ↓
             EXPLAIN / safety
                  ↓
            read-only execution
                  ↓
                 result
                   │
        ┌──────────┘
        ▼
ExplanationService
        ↓
Internal LLM
        ↓
short explanation
        ↓
API response
```

Operationally, the release is complete only when:

```text
Maven build          ✓
tests                ✓
Sonar quality gate   ✓
Jib image            ✓
Nexus push           ✓
Helm chart           ✓
Kubernetes health    ✓
3 REST endpoints     ✓
tool-first path      ✓
NL→SQL fallback      ✓
SQL safety           ✓
read-only DB users   ✓
LLM explanation      ✓
Micrometer metrics   ✓
Prometheus scraping  ✓
Grafana dashboard    ✓
Jenkins CI           ✓
Jenkins CD           ✓
Swagger/OpenAPI      ✓
```

This is the version I would now use as the working implementation document for the six-day deadline.


Yes, those three clarifications fit the architecture cleanly.

* **Catalog:** the four PostgreSQL cube tables stay fixed. For StarRocks, the catalog should refresh its actual `nimbo.netflow` columns from StarRocks metadata (`INFORMATION_SCHEMA`) at startup and periodically/on demand, so NL→SQL always sees the current schema. We should still keep semantic/allowlist rules around those columns rather than blindly trusting every discovered field.
* **Tool-first path:** agreed — tools call **only the existing PostgreSQL/cube-backed REST endpoints**. We do not use existing StarRocks REST endpoints as tools. If the PostgreSQL tools cannot answer the request, then the guarded SQL fallback can route to either PostgreSQL or StarRocks.
* **UI:** add a simple HTML/CSS/JavaScript page, probably served directly by `analytics-query`, which calls `POST /api/analytics/query` and displays the structured data plus the `explanation`. No frontend framework is necessary for V1.

For the routing question, deterministic routing still works even when the StarRocks catalog changes. The LLM first converts the question into structured requirements, for example:

```text
dimensions:
    SOURCE_IP
    DESTINATION_PORT

metric:
    DISTINCT_COUNT

granularity:
    RAW

timeRange:
    last 30 minutes
```

Then ordinary Java code compares those requirements against the **current catalog**:

```text
QueryRequirements
        +
current SemanticSchemaCatalog
        ↓
Does PostgreSQL cube data satisfy all requirements?
        │
       yes → POSTGRESQL
        │
       no
        ↓
Does current StarRocks schema satisfy them?
        │
       yes → STARROCKS
        │
       no → UNSUPPORTED
```

So changing StarRocks columns doesn't require changing routing code; it changes what the catalog reports as available.

I would **not let the LLM make the final datasource decision**. We can let the LLM help extract the semantic requirements from the user's language, which is exactly where an LLM is useful. But the final choice should remain deterministic because it is easy to test, explain, and enforce.

A reasonable hybrid is:

```text
LLM:
"What does this question require?"

Java:
"Given those requirements and the current catalog,
which datasource can actually satisfy them?"
```

If later you want the LLM to propose `POSTGRESQL` or `STARROCKS` too, it can be treated as a suggestion, but Java should still validate that suggestion against the catalog before accepting it.

So I would update the plan with exactly these three changes.
