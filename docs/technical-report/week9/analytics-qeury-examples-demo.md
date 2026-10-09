# Analytics Query Demo Examples

Endpoint:

```text
POST http://10.10.61.78/api/analytics/query
```

These examples were validated against the running service on 2026-10-09. The successful examples returned HTTP `200`, non-empty data and a non-empty explanation. The controlled non-success examples returned their intended status.

## 1. Success — PostgreSQL tool execution

Status: `SUCCESS`  
Execution mode: `TOOL`  
Datasource: `POSTGRESQL`

The validated intent matches an approved REST tool. Analytics Query calls the PostgreSQL-backed REST API instead of generating SQL.

- **Connection activity for an IP**

  > On October 6, 2026, from 1 PM to 3 PM UTC, list each stored connection interval involving IP 85.214.251.214 with source IP, destination IP, inbound and outbound bytes and packets.

  Observed result: 1 row with an explanation.

## 2. Success — SQL fallback with PostgreSQL

Status: `SUCCESS`  
Execution mode: `GENERATED_SQL`  
Datasource: `POSTGRESQL`

No approved tool exactly matches the requested analysis. The service deterministically selects a PostgreSQL cube, validates the generated SQL and executes it through the read-only path.

- **Top connection pairs by total bytes**

  > On October 6, 2026, from 1 PM to 3 PM UTC, group connections involving 85.214.251.214 by source and destination IP, sum total bytes, and return the top 5 pairs.

  Observed result: 1 row with an explanation.

- **Top destinations by total packets**

  > On October 6, 2026, from 1 PM to 3 PM UTC, for connection intervals involving IP 85.214.251.214, group by destination IP, sum total packets, and return the top 5 destination IPs sorted by summed total packets descending.

  Observed result: 1 row with an explanation.

- **Traffic for a known source IP**

  > Show the inbound and outbound bytes and packets for source IP 223.191.114.202 on October 6, 2026, from 1 PM to 3 PM UTC.

  Observed result: 1 row with an explanation.

- **L7 protocol usage**

  > Show L7 protocol usage with inbound and outbound bytes, packets, and connection count on October 6, 2026, from 1 PM to 3 PM UTC.

  Observed result: 5 rows with an explanation.

- **Top L7 protocols by connection count**

  > On October 6, 2026, from 1 PM to 3 PM UTC, group protocol usage by L7 protocol, sum connection count, and return the top 5 protocols sorted by summed connection count descending.

  Observed result: 5 rows with an explanation.

- **Stakeholder-style connection question**

  > Who did 85.214.251.214 communicate with on October 6, 2026 between 1 PM and 3 PM UTC, and how much traffic was exchanged?

  Observed result: 1 row with an explanation.

- **Stakeholder-style protocol question**

  > Which application protocols handled the most connections on October 6, 2026 between 1 PM and 3 PM UTC? Show the top five.

  Observed result: 5 rows with an explanation.

## 3. Success — SQL fallback with StarRocks

Status: `SUCCESS`  
Execution mode: `GENERATED_SQL`  
Datasource: `STARROCKS`

The request needs raw-flow fields that the PostgreSQL cubes do not contain. The service deterministically selects StarRocks, validates the generated SQL and executes it through the read-only path.

- **Source ports by average flow duration**

  > On October 6, 2026, from 1 PM to 3 PM UTC, show the 10 source ports with the highest average flow duration in milliseconds.

  Observed result: 10 rows with an explanation.

- **Source ports by inbound bytes**

  > On October 6, 2026, from 1 PM to 3 PM UTC, show the top 10 source ports ranked by summed inbound bytes across raw flows.

  Observed result: 10 rows with an explanation.

- **Source ports by maximum flow duration**

  > On October 6, 2026, from 1 PM to 3 PM UTC, show the 10 source ports with the highest maximum flow duration in milliseconds.

  Observed result: 10 rows with an explanation.

- **Average bytes per raw flow for a known IP**

  > On October 6, 2026, from 1 PM to 3 PM UTC, calculate the average total bytes per raw flow involving IP 85.214.251.214.

  Observed result: 1 row with an explanation.

## 4. Controlled non-success responses

These questions demonstrate that invalid, unsafe, ambiguous or unsupported requests stop with a controlled status instead of reaching unchecked execution.

### Rejected harmful request

Status: `REJECTED`

The intent requests a destructive operation. It is rejected before tool selection or SQL generation.

- **Delete network records**

  > Delete all network records from 1 PM to 3 PM UTC on October 6, 2026.

### SQL rejected by the safety policy

Status: `QUERY_REJECTED`

The request reaches SQL fallback, but the generated query does not pass the SQL or query-plan safety policy and is not executed.

- **Expensive source-IP aggregation**

  > On October 6, 2026, from 1 PM to 3 PM UTC, group traffic by source IP, sum total bytes, and return the top 5 source IPs sorted by summed total bytes descending.

### Out-of-scope request

Status: `OUT_OF_SCOPE`

The question is unrelated to the supported NetFlow analytics domain.

- **Football result**

  > Who won the football World Cup in 2022?

### Request needing clarification

Status: `NEEDS_CLARIFICATION`

The analytics meaning is potentially supported, but a required time range is missing. The service asks the user for that information and does not execute a query.

- **Traffic without a time range**

  > Show the inbound and outbound bytes and packets for source IP 223.191.114.202.

### Unsupported analysis

Status: `UNSUPPORTED_ANALYSIS`

The request asks for a definitive security conclusion that the available NetFlow data cannot establish.

- **Definitive malware attribution**

  > Which source IP was definitely infected with malware on October 6, 2026, from 1 PM to 3 PM UTC?
