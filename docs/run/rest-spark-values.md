http://10.10.61.75:18083/api/swagger-ui/index.html#

## PostgreSQL batch-cube endpoints


### `GET /bandwidth/top`

```text
limit = 10
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
```
Valid limit: `1–100`.


### `GET /bandwidth/{ip}`

```text
ip = 192.168.1.152
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
```


### `GET /connections/{ip}`

```text
ip = 172.31.69.23
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
{
  "page": 0,
  "size": 10,
  "sortBy": "inBytes",
  "sortDirection": "DESC"
}
```
The IP can match either `src_ip` or `dst_ip`.


### `GET /connections`

```text
srcIp = 172.31.69.23
dstIp = 18.219.211.138
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
{
  "page": 0,
  "size": 10,
  "sortBy": "inBytes",
  "sortDirection": "DESC"
}
```
Either IP filter can be left empty.


### `GET /protocols`

```text
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
```


### `GET /protocols/{l7Protocol}`

```text
l7Protocol = 5.0
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
```
TCP would be `6.0` and UDP woudl be `5.0`.


### `GET /traffic/{srcIp}`

```text
srcIp = 70.192.246.222
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
{
  "page": 0,
  "size": 10,
  "sortBy": "inBytes",
  "sortDirection": "DESC"
}
```


### `GET /traffic`

```text
srcIp = 70.192.246.222
{
  "startTime": "2026-09-07T04:47:15.772Z",
  "endTime": "2026-09-09T04:47:15.772Z"
}
{
  "page": 0,
  "size": 10,
  "sortBy": "inBytes",
  "sortDirection": "DESC"
}
```
Leave `srcIp` empty to list traffic for every source IP.





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