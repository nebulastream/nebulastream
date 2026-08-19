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
file. Both produce tuples that conform to the `lrb` schema. The CSV file might look like this:

```
creationTS,vehicle,speed,highway,lane,direction,position
1234567890,101,65.5,1,2,0,15840
1234567891,102,70.2,1,3,0,21120
1234567892,103,55.8,2,1,1,10560
1234567893,101,68.3,1,2,0,16896
```

Each physical source requires configuration for:
- The specific connector (e.g. file path or TCP socket details) via `SOURCE.*` parameters.
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

- The sink itself is configured via `SINK.*` parameters.
- The output formatter is configured via `OUTPUT_FORMATTER.*` parameters.

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
