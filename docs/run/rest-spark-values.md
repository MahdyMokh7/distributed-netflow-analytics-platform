All current REST endpoints are `GET` requests, so Swagger shows path and query fields—not a JSON request body.

Open Swagger at:

```text
http://<rest-api-host>:8080/api/swagger-ui.html
```

Use this valid historical range in the examples:

```text
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

The end must be after the start, not in the future, and the range cannot exceed seven days.

## PostgreSQL batch-cube endpoints

### `GET /traffic/{srcIp}`

```text
srcIp        = 70.192.246.222
startTime    = 2026-09-07T00:00:00Z
endTime      = 2026-09-08T00:00:00Z
page         = 0
size         = 20
sortBy       = windowStart
sortDirection = DESC
```

### `GET /traffic`

```text
srcIp        = 70.192.246.222
startTime    = 2026-09-07T00:00:00Z
endTime      = 2026-09-08T00:00:00Z
page         = 0
size         = 20
sortBy       = inBytes
sortDirection = DESC
```

Leave `srcIp` empty to list traffic for every source IP.

### `GET /connections/{ip}`

```text
ip           = 70.192.246.222
startTime    = 2026-09-07T00:00:00Z
endTime      = 2026-09-08T00:00:00Z
page         = 0
size         = 20
sortBy       = windowStart
sortDirection = DESC
```

The IP can match either `src_ip` or `dst_ip`.

### `GET /connections`

```text
srcIp        = 10.0.0.1
dstIp        = 10.0.0.2
startTime    = 2026-09-07T00:00:00Z
endTime      = 2026-09-08T00:00:00Z
page         = 0
size         = 20
sortBy       = inBytes
sortDirection = DESC
```

Either IP filter can be left empty.

### `GET /bandwidth/top`

```text
limit     = 10
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

Valid limit: `1–100`.

### `GET /bandwidth/{ip}`

```text
ip        = 70.192.246.222
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

### `GET /protocols`

```text
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

### `GET /protocols/{l7Protocol}`

UDP example:

```text
l7Protocol = 17.0
startTime  = 2026-09-07T00:00:00Z
endTime    = 2026-09-08T00:00:00Z
```

TCP would be `6.0`.

## StarRocks realtime endpoints

### `GET /realtime/traffic/{ip}`

```text
ip        = 70.192.246.222
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

### `GET /realtime/connections/{ip}`

```text
ip        = 70.192.246.222
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
page      = 0
size      = 20
```

### `GET /realtime/bandwidth/top`

```text
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
limit     = 10
```

### `GET /realtime/protocols`

```text
startTime = 2026-09-07T00:00:00Z
endTime   = 2026-09-08T00:00:00Z
```

For pagination, use Java field names such as `windowStart` and `inBytes`, not database names such as `window_start` or `in_bytes`. If the example IP is absent from the database, the request format is still valid but the API will return not found.