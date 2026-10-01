# REST API capability contract

## Purpose

This document records the REST capabilities verified for Phase 2.1 of
`analytics-query`. It is based on the current `rest-api` controllers, request and
response DTOs, services, repositories, validation, exception handling, and
production configuration.

Phase 2.1 is inspection and documentation only. It does not introduce tools,
HTTP adapters, tool matching, or planner behavior.

## Approved backend boundary

The REST API uses the `/api` context path. The PostgreSQL aggregate endpoint
families approved for the later tool-first path are:

```text
/api/traffic
/api/connections
/api/bandwidth
/api/protocols
```

These controllers call PostgreSQL/JPA query services. They do not route to
StarRocks.

StarRocks has a separate controller under `/api/StarRocks`. Those endpoints are
not approved tools for `analytics-query`; StarRocks remains available through
the later guarded generated-SQL path.

## Common PostgreSQL contract

### Time range

Every endpoint requires `startTime` and `endTime` as ISO-8601 instants.

The accepted interval is half-open:

```text
[startTime, endTime)
```

Repository predicates use:

```text
windowStart >= startTime
windowStart <  endTime
```

Rules:

- `endTime` must be after `startTime`;
- `endTime` must not be in the future;
- the range must not exceed seven days.

Although configuration declares `default-days: 1`, the inspected controller
contract does not apply a default. Both parameters are required.

### IP values

The controllers use `@ValidIpAddress`. Blank values are accepted so optional
query filters can be omitted. The current REST validator delegates to
`InetAddress.getByName`, so it may accept values beyond literal IPv4 addresses,
including resolvable hostnames.

`analytics-query` must not treat DNS resolution as a tool capability. Later
tools must send only IP literals already accepted by the `analytics-query`
semantic contract. V1 currently uses IPv4 semantics.

### Service behavior

All four PostgreSQL query services are read-only and use caching, the
`database` circuit breaker, and the `database` retry policy. The configured
retry policy makes up to three attempts for configured database exceptions.

No matching records currently produce HTTP `404`, not `200` with an empty
collection. A later `analytics-query` REST adapter must normalize only the
known no-data response to a successful empty result. It must not use that
response to trigger SQL fallback.

## Endpoint capability matrix

| Endpoint | Verified capability | Parameters | Result |
|---|---|---|---|
| `GET /api/traffic/{srcIp}` | Aggregate traffic windows for one source IP | source IP, time range, pagination and optional sorting | paginated window rows |
| `GET /api/traffic` | Aggregate traffic windows, optionally filtered by source IP | optional source IP, time range, pagination and optional sorting | paginated window rows |
| `GET /api/connections/{ip}` | Aggregate connections where the IP is source or destination | IP, time range, pagination and optional sorting | paginated window rows |
| `GET /api/connections` | Aggregate connections with source and/or destination filters | optional source IP, optional destination IP, time range, pagination and optional sorting | paginated window rows |
| `GET /api/bandwidth/top` | IPs ranked by total bandwidth over the complete range | time range and optional limit | whole-range totals grouped by IP and ordered by total bytes descending |
| `GET /api/bandwidth/{ip}` | Latest matching bandwidth window for one IP | IP and time range | one newest window, not a whole-range total |
| `GET /api/protocols` | Protocol aggregate windows | time range | all matching protocol/window rows |
| `GET /api/protocols/{l7Protocol}` | Latest matching window for one L7 protocol | numeric L7 protocol and time range | one newest window, not a whole-range total |

### Traffic limitations

- Only source-IP filtering exists.
- Destination-IP filtering is not available.
- Results are stored aggregate-window rows, not a whole-range regrouping.
- The repository contains a top-traffic query, but no controller/service exposes
  it; therefore it is not a REST capability.

### Connection limitations

- Source-only, destination-only, both, and source-or-destination IP filtering
  are supported.
- Ports and protocol filters are not supported.
- Response rows contain no port or protocol fields.
- Results are stored aggregate-window rows, not a whole-range regrouping.

### Bandwidth details

`/bandwidth/top` groups the complete requested range by IP and sums inbound and
outbound bytes and packets. Ranking is fixed to total bytes descending.

Its `limit` behavior is:

```text
omitted          -> 10
1 through 100    -> requested value
zero/negative    -> 10
greater than 100 -> 10
```

The invalid values are normalized by the controller rather than rejected.

`/bandwidth/{ip}` orders matching rows by `windowStart DESC` and returns the
first row. It must not be represented as bandwidth summed over the requested
range.

### Protocol details

`/protocols` returns per-window records ordered by protocol and newest window.

`/protocols/{l7Protocol}` returns the first row from results ordered by
`windowStart DESC`. It must not be represented as usage summed over the
requested range.

## Pagination and sorting

Traffic and connection endpoints require:

```text
page >= 0
1 <= size <= 100
```

Sorting defaults:

```text
sortBy        = windowStart
sortDirection = DESC
```

`sortDirection` accepts case-insensitive `ASC` or `DESC`.

The configured `default-size: 20` and `max-offset: 10000` are not enforced by
these controller/service paths. They must not be advertised as tool behavior.

### Traffic sort fields

```text
srcIp
windowStart
windowEnd
inBytes
outBytes
inPackets
outPackets
recordCount
ingestedAt
```

### Connection sort fields

```text
id
srcIp
dstIp
windowStart
windowEnd
inBytes
outBytes
inPackets
outPackets
ingestedAt
```

Calculated response fields such as `totalBytes` and `totalPackets` are not
valid server-side sort fields. Paginating a supported sort does not provide a
global ranking by a calculated field.

Pagination metadata contains:

```text
totalElements
totalPages
page
size
first
last
numberOfElements
```

## Response contract

Successful responses use:

```json
{
  "success": true,
  "data": {},
  "pagination": {},
  "timestamp": "2026-10-01T00:00:00Z"
}
```

`pagination` is absent for non-paginated endpoints. The response timestamp is
an `Instant` and is serialized in UTC.

### Traffic row

```text
srcIp
windowStart
windowEnd
inBytes
outBytes
totalBytes
inPackets
outPackets
totalPackets
recordCount
batchId
ingestedAt
```

### Connection row

```text
id
srcIp
dstIp
windowStart
windowEnd
inBytes
outBytes
totalBytes
inPackets
outPackets
totalPackets
batchId
ingestedAt
```

`id` is the lowercase hexadecimal representation of the stored binary
connection identifier.

### Bandwidth row

```text
ip
windowStart
windowEnd
inboundBytes
outboundBytes
totalBytes
inboundPackets
outboundPackets
totalPackets
batchId
ingestedAt
```

For `/bandwidth/top`, only IP and aggregated byte/packet totals are populated;
window, batch, and ingestion fields are null.

### Protocol row

```text
l7Protocol
windowStart
windowEnd
inBytes
outBytes
totalBytes
inPackets
outPackets
totalPackets
connectionCount
batchId
ingestedAt
```

The PostgreSQL endpoints expose persisted rows including `batchId`; traffic,
connection, bandwidth-by-IP, and protocol window endpoints do not merge equal
semantic windows across different batch IDs.

## Error behavior

The controller advice explicitly maps these conditions:

| Condition | HTTP status | Body |
|---|---:|---|
| no matching records | `404` | `ApiResponseDto`, `success=false` |
| invalid time range or illegal argument | `400` | `ApiResponseDto`, `success=false` |
| Bean Validation failure handled by the advice | `400` | `ApiResponseDto`, `success=false` |
| PostgreSQL data-access failure | `503` | `ApiResponseDto`, `success=false` |
| open database circuit breaker | `503` | `ApiResponseDto`, `success=false` |

The database-unavailable message is:

```text
PostgreSQL query service is temporarily unavailable
```

The advice is scoped only to the four PostgreSQL controllers. StarRocks errors
are outside this contract. Framework-level binding failures not listed in the
advice may use Spring Boot's default error response, so the later client must
primarily classify by HTTP status and only parse known bodies defensively.

## Capabilities safe for later tool registration

```text
TRAFFIC_WINDOWS
  optional source IP
  bounded time range
  pagination
  approved traffic sort fields

CONNECTION_WINDOWS
  optional source IP
  optional destination IP
  source-or-destination IP variant
  bounded time range
  pagination
  approved connection sort fields

TOP_BANDWIDTH_IPS
  bounded time range
  whole-range aggregation
  ranking by total bytes descending
  effective limit behavior documented above

PROTOCOL_USAGE_WINDOWS
  bounded time range
  all protocol/window rows
  no single-protocol filter

LATEST_PROTOCOL_WINDOW
  specific L7 protocol
  bounded time range
  latest matching window only
  not initially registered unless the user explicitly requests latest/current
```

Do not initially register `/bandwidth/{ip}` or
`/protocols/{l7Protocol}` as generic whole-range tools. They are safe only for
questions explicitly requesting the latest matching aggregate window.

Do not advertise unsupported capabilities such as top traffic, top
connections, destination filtering for traffic, connection port/protocol
filtering, or StarRocks REST access.

## Phase 2.1 completion boundary

This verified document is the complete Phase 2.1 artifact. The following work
belongs to later phases:

- centralized `RestApiClient` implementation;
- tool interfaces and concrete tools;
- tool registry and matching;
- normalization of downstream `404` no-data responses;
- planner integration;
- SQL fallback decisions.
