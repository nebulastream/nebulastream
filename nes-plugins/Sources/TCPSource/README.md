# TCPSource Plugin

Reads raw bytes from a TCP server and feeds them into the query engine as tuple buffers.

## Configuration Parameters

| Parameter | Key | Type | Default | Required | Description |
|---|---|---|---|---|---|
| Host | `socket_host` | string | — | yes | Hostname or IP address of the TCP server. |
| Port | `socket_port` | uint32 | — | yes | TCP port (0–65535). |
| Flush Interval | `flush_interval_ms` | float | `0` | no | Maximum time in milliseconds to accumulate data before flushing a partial buffer. When set to `0` (default), buffers are only flushed once completely full — the source will block indefinitely waiting for enough data to fill the buffer. |
| Connect Timeout | `connect_timeout_ms` | uint32 | `0` | no | Connection timeout in milliseconds. The source will fail if a connection cannot be established within this time. When set to `0` (default), there is no timeout and the source will wait indefinitely for a connection. |
| Auto Reconnect | `auto_reconnect` | bool | `false` | no | If `true`, automatically reconnects on disconnect or error. If `connect_timeout_ms` is set, the source will fail when a reconnection attempt exceeds the timeout. If `connect_timeout_ms` is `0`, reconnection attempts continue indefinitely. If `false`, the source terminates on disconnect. |

## Source Definition

```yaml
physical:
  - logical: MY_STREAM
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: TCP
    source_config:
      socket_host: "localhost"
      socket_port: "9999"
```

### With all options

```yaml
physical:
  - logical: MY_STREAM
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: TCP
    source_config:
      socket_host: "192.168.1.100"
      socket_port: "5000"
      flush_interval_ms: "100"
      connect_timeout_ms: "10000"
      auto_reconnect: "true"
```

## Full Query Example

```yaml
query: |
  SELECT * FROM SENSOR_DATA INTO PRINT_SINK

sinks:
  - name: PRINT_SINK
    type: Print
    schema:
      - name: SENSOR_DATA$id
        type: UINT64
      - name: SENSOR_DATA$name
        type: VARSIZED
      - name: SENSOR_DATA$value
        type: FLOAT64
      - name: SENSOR_DATA$timestamp
        type: UINT64
    config:
      input_format: CSV

logical:
  - name: SENSOR_DATA
    schema:
      - name: id
        type: UINT64
      - name: name
        type: VARSIZED
      - name: value
        type: FLOAT64
      - name: timestamp
        type: UINT64

physical:
  - logical: SENSOR_DATA
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: TCP
    source_config:
      socket_host: "localhost"
      socket_port: "9999"
      flush_interval_ms: "100"
      connect_timeout_ms: "10000"
      auto_reconnect: "true"
```
