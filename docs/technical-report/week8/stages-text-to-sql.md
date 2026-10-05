## Analytics Query Execution Stages

- **INTENT** — Uses the LLM to convert the natural-language question into validated structured query requirements.
- **PLANNER** — Decides whether the request should use an approved REST tool or the generated-SQL fallback.
- **TOOL** — Executes a predefined read-only analytics operation through the existing REST API.
- **SQL_ELIGIBILITY** — Verifies that an unmatched request is safe and suitable for SQL generation.
- **ROUTING** — Deterministically selects PostgreSQL or StarRocks based on the validated requirements and semantic catalog.
- **SQL_GENERATION** — Uses the LLM to generate one read-only SQL query using only the selected datasource schema.
- **SQL_VALIDATION** — Parses the generated SQL and enforces table, function, join, time-range, row-limit, and read-only policies.
- **QUERY_PLAN** — Uses datasource-specific `EXPLAIN` checks to reject clearly unsafe or excessive query plans.
- **SQL_EXECUTION** — Executes approved SQL with read-only credentials, timeouts, row limits, and payload limits.
- **EXPLANATION** — Uses the LLM to produce a short factual explanation of the successful analytics result.