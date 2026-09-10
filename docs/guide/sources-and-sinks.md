# Sources and Sinks

A source ingests external data into NebulaStream, creating a stream; a sink is the destination for query
results. Currently, a query must have exactly one sink.

## Logical Sources

NebulaStream separates **logical** and **physical** sources to provide flexible data ingestion. A logical
source is like a **table definition** in a traditional database. It provides an abstract description of a
data stream with a `name` and `schema`.

- The `name` references the stream in your query's `FROM` clause.
- The `schema` defines the structure of data records (tuples), listing each field's name and data type.

Operators automatically infer their schemas from the source, so you only define it once. Incoming data
must strictly match this schema, including field order; any mismatch terminates the query.

```sql
CREATE LOGICAL SOURCE lrb(
  creationTS UINT64,
  vehicle INT16,
  speed FLOAT32,
  highway INT16,
  lane INT16,
  direction INT16,
  position INT32
);
```

This defines the `lrb` source used in a query's `FROM` clause. The schema specifies seven fields per
record. Multiple logical sources can be defined and combined with `JOIN` or `UNION`.

## Physical Sources

A physical source specifies **how** and **where** to ingest data for a logical source. Each logical
source can have multiple physical sources, allowing a single stream to aggregate data from heterogeneous
endpoints.

Supported physical source types:
- `File`
- `TCP`
- `MQTT`
- `Generator`

### Source Parameters

All source types also accept `MAX_INFLIGHT_BUFFERS` (default: `0`, meaning the engine-wide default is
used).

#### File

| Parameter | Default | Description |
|---|---|---|
| `FILE_PATH` | *(required)* | Path to the file to read. |

#### TCP

| Parameter | Default | Description |
|---|---|---|
| `SOCKET_HOST` | *(required)* | Hostname/IP to connect to. |
| `SOCKET_PORT` | *(required)* | Port to connect to (0-65535). |
| `SOCKET_DOMAIN` | `AF_INET` | `AF_INET` or `AF_INET6`. |
| `SOCKET_TYPE` | `SOCK_STREAM` | `SOCK_STREAM`, `SOCK_DGRAM`, `SOCK_SEQPACKET`, `SOCK_RAW`, or `SOCK_RDM`. |
| `TUPLE_DELIMITER` | `\n` | Byte separating tuples in the stream. |
| `FLUSH_INTERVAL_MS` | `0` | Interval for flushing partially-filled buffers. |
| `SOCKET_BUFFER_SIZE` | `1024` | Size of the socket read buffer. |
| `CONNECT_TIMEOUT_SECONDS` | `10` | Connection timeout. |

#### MQTT

| Parameter | Default | Description |
|---|---|---|
| `SERVER_URI` | *(required)* | Broker URI. |
| `TOPIC` | *(required)* | Topic to subscribe to. |
| `CLIENT_ID` | auto-generated | MQTT client identifier. |
| `QOS` | `1` | Quality of service: `0`, `1`, or `2`. |
| `FLUSH_INTERVAL_MS` | `0` | Interval for flushing partially-filled buffers. |
| `IMPLICIT_MESSAGE_DELIMITER` | `\n` | Delimiter inserted between concatenated MQTT payloads. |
| `LOG_MESSAGES` | `false` | Logs every received message. |

#### Generator

| Parameter | Default | Description |
|---|---|---|
| `GENERATOR_SCHEMA` | *(required)* | DSL string describing how to generate each field. |
| `SEED` | current time | Seed for the random generator. |
| `GENERATOR_RATE_TYPE` | `FIXED` | `FIXED` or `SINUS` emission rate. |
| `GENERATOR_RATE_CONFIG` | `emit_rate 1000` | DSL string configuring the emission rate. |
| `MAX_RUNTIME_MS` | `-1` | Stops the generator after this many ms; `-1` runs until stopped externally. |
| `STOP_GENERATOR_WHEN_SEQUENCE_FINISHES` | *(required)* | `ALL`, `ONE`, or `NONE`. |
| `FLUSH_INTERVAL_MS` | `10` | Interval for flushing partially-filled buffers. |

The example below defines two physical sources that both feed the `lrb` logical source:

```sql
CREATE PHYSICAL SOURCE FOR lrb TYPE TCP SET(
  'localhost:8080' AS "SOURCE"."HOST",
  'localhost' as "SOURCE".SOCKET_HOST,
  50501 as "SOURCE".SOCKET_PORT,
  65536 as "SOURCE".SOCKET_BUFFER_SIZE,
  100 as "SOURCE".FLUSH_INTERVAL_MS,
  60 as "SOURCE".CONNECT_TIMEOUT_SECONDS,
  'CSV' as INPUT_FORMATTER."TYPE",
  '\n' as INPUT_FORMATTER.TUPLE_DELIMITER,
  ',' as INPUT_FORMATTER.FIELD_DELIMITER
);

CREATE PHYSICAL SOURCE FOR lrb TYPE File SET(
  'localhost:8080' AS "SOURCE"."HOST",
  'lrb.json' as "SOURCE".FILE_PATH,
  'JSON' as INPUT_FORMATTER."TYPE"
);
```

One source reads CSV-formatted data from a TCP socket, while the other reads JSON-formatted data from a
file. Both produce tuples that conform to the `lrb` schema. The CSV file might look like this (columns:
`creationTS, vehicle, speed, highway, lane, direction, position`; the CSV input formatter cannot skip a
header row, so the file itself must not contain one):

```
1234567890,101,65.5,1,2,0,15840
1234567891,102,70.2,1,3,0,21120
1234567892,103,55.8,2,1,1,10560
1234567893,101,68.3,1,2,0,16896
```

Each physical source requires configuration for:
- The specific connector (e.g. file path or TCP socket details) via `"SOURCE".*` parameters.
- The data's input format (e.g. `CSV` or `JSON`) and delimiters via `INPUT_FORMATTER.*` parameters.

The query itself remains completely decoupled from these physical details. You can add, remove, or
change physical sources without touching the query logic.

## Input Formatters

Tuples can arrive in a variety of formats. We distinguish two broad categories:
- Text-based formats (JSON, CSV, XML, YAML, etc.)
- Binary formats (Avro, Parquet, Protobuf, etc.)

Input formatters convert byte streams from source connectors into the native in-memory representation
used by query-compiled operators. The format is specified via `INPUT_FORMATTER.*` parameters in each
physical source:

```sql
CREATE PHYSICAL SOURCE FOR source_name TYPE TCP SET(
  'CSV' as INPUT_FORMATTER."TYPE",
  '\n' as INPUT_FORMATTER.TUPLE_DELIMITER,
  ',' as INPUT_FORMATTER.FIELD_DELIMITER,
  ...
);
```

NebulaStream supports CSV and JSON input formats.

### Input Formatter Parameters

#### CSV

| Parameter | Default | Description |
|---|---|---|
| `TUPLE_DELIMITER` | `\n` | Byte separating tuples. |
| `FIELD_DELIMITER` | `,` | Byte separating fields within a tuple. |
| `ALLOW_COMMAS_IN_STRINGS` | `true` | Allows the field delimiter to appear inside quoted string fields. |

#### JSON

| Parameter | Default | Description |
|---|---|---|
| `TUPLE_DELIMITER` | `\n` | Byte separating JSON objects. |

Nested JSON objects are supported: address a nested field by joining the path with `/` in the schema's
field name. JSON arrays are not supported. For example, given the JSON object:

```json
{"MILK": {"CYCLES": {"LEFT": 3, "RIGHT": 5}}, "FARM_ID": 42}
```

the following logical source reads `LEFT` and `RIGHT` out of the nested `MILK.CYCLES` object:

```sql
CREATE LOGICAL SOURCE dairy(
  "FARM_ID" UINT32,
  "MILK/CYCLES/LEFT" UINT32,
  "MILK/CYCLES/RIGHT" UINT32
);
```

Unquoted identifiers are uppercased before matching, so JSON keys must be uppercase unless the field name
is quoted (as above) for case-sensitive matching. For a nullable field, a missing field, a missing parent
object, or an explicit JSON `null` all map to `NULL`; for a `NOT NULL` field, a missing field or parent
raises an error.

## Defining a Sink

```sql
CREATE SINK csv_sink(
  start UINT64 NOT NULL,
  end UINT64 NOT NULL,
  highway INT16,
  direction INT16,
  positionDiv5280 INT32,
  avgSpeed FLOAT64
) TYPE File SET(
  'localhost:8080' AS "SINK"."HOST",
  '<path>' as "SINK".FILE_PATH,
  'CSV' as "SINK".OUTPUT_FORMAT,
  FALSE as "SINK".APPEND
);
```

The sink name (`csv_sink`) must match the name used in the query's `INTO` clause.

Available sink types:
- `File`: Writes results to a file, either overwriting or appending.
- `Print`: Writes results to standard output (stdout).
- `Void`
- `MQTT`

The `SET` clause specifies the output details. For a `File` sink, this includes the file path and the
data format for the output. The `HOST` configuration parameter specifies the worker node, identified by
its gRPC address, which hosts the physical source/sink.

- The sink itself is configured via `"SINK".*` parameters.
- The output formatter is configured via `OUTPUT_FORMATTER.*` parameters.

### Sink Parameters

All sink types also accept:

| Parameter | Default | Description |
|---|---|---|
| `OUTPUT_FORMAT` | *(required)* | Output format, e.g. `CSV` or `JSON`. |
| `ADD_TIMESTAMP` | `false` | Prefixes each output tuple with a timestamp. |
| `BACKPRESSURE_UPPER_THRESHOLD` | `1000` | Queue size at which backpressure engages. |
| `BACKPRESSURE_LOWER_THRESHOLD` | `200` | Queue size at which backpressure disengages. |

#### File

| Parameter | Default | Description |
|---|---|---|
| `FILE_PATH` | *(required)* | Path to the output file. |
| `APPEND` | `false` | Appends to the file instead of overwriting it. |

#### Print

| Parameter | Default | Description |
|---|---|---|
| `INGESTION` | `0` | Artificial delay (ms) added after writing each buffer. |

#### Void

Discards every tuple; accepts `FILE_PATH` and `OUTPUT_FORMAT` for interface compatibility, but ignores
both.

#### MQTT

| Parameter | Default | Description |
|---|---|---|
| `SERVER_URI` | *(required)* | Broker URI. |
| `TOPIC` | *(required)* | Topic to publish to. |
| `CLIENT_ID` | auto-generated | MQTT client identifier. |
| `QOS` | `1` | Quality of service: `0`, `1`, or `2`. |
| `RETAINED` | `false` | Marks published messages as retained. |
| `MAX_OUTSTANDING_MESSAGES` | `128` | Max unacknowledged QoS≥1 messages before backpressure engages. |

## Output Formatters

The output formatter component converts records with values in NebulaStream's native in-memory format
into the desired output format of a sink. It is used by sinks that set the `OUTPUT_FORMAT` parameter to
configure the format of the result tuples.

Out-of-the-box available output formats:
- CSV
- JSON

Some output formats are configurable via parameters. For instance, the bool parameter `QUOTE_STRINGS`
controls how the CSV output formatter represents strings. All required parameters are specified via
`OUTPUT_FORMATTER.*` in each sink.

```sql
CREATE SINK sink_name TYPE FILE SET(
       'CSV' as "SINK".OUTPUT_FORMAT,
       TRUE as "OUTPUT_FORMATTER".QUOTE_STRINGS,
       ...
);
```

### Output Formatter Parameters

#### CSV

| Parameter | Default | Description |
|---|---|---|
| `QUOTE_STRINGS` | `false` | Wraps string fields in quotes. |
| `FIELD_DELIMITER` | `,` | Byte separating fields within a tuple. |
| `TUPLE_DELIMITER` | `\n` | Byte separating tuples. |

#### JSON

No configurable parameters.
