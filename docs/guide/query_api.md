# NebulaStream Query API Guide

NebulaStream provides stream processing through a declarative, SQL-like query language.
This guide explains core concepts and how to create queries.

The fundamental data flow is simple:
1.  **Sources** ingest data into the system.
2.  **Operators** process and transform data in-flight.
3.  **Sinks** emit results to external systems (databases, files) or to the console.

Let's start with key terminology.

---

### Core Concepts Glossary

| Term                 | Description                                                                                 | Usage                                                                            |
|:---------------------|:--------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------|
| **Stream**           | Unbounded sequence of data records (tuples).                                                | `FROM`, `INTO`                                                                   |
| **Tuple**            | A single record or event in a stream, composed of one or more fields.                       | Internal                                                                         |
| **Schema**           | Logical structure of a tuple, defining its fields and their data types.                     | [See Sources](#data-sources-logical-and-physical)                                |
| **Field**            | Atomic unit of data within a tuple, defined by a name and a data type.                      | Internal                                                                         |
| **Data Type**        | Specifies how to interpret a field's data and which operations are valid.                   | `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`, `CHAR`, `BOOL`, `VARSIZED` |
| **Source**           | Connector that **ingests** external data, creating a stream.                                | `FROM`, [See Sources](#data-sources-logical-and-physical)                        |
| **Input Formatter**  | Decodes raw data from a source into internal tuple format.                                  | [See Input Formatters](#input-formatters)                                        |
| **Operator**         | Transforms a stream of tuples (e.g., filtering, aggregating).                               | `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, [See Operators](#operators)               |
| **Function**         | Operation applied to one or more fields (or input functions) within an operator.            | `SUM`, `AVG`, `+`, `-`, `CONCAT`, [See Functions](#functions)                    |
| **Window**           | Partition an unbounded stream into finite chunks for stateful operations like aggregations. | `WINDOW (TUMBLING\|SLIDING) (timestamp, [duration][unit])`                       |
| **Output Formatter** | Encodes tuples into a specific format to prepare for a sink.                                | [See Input Formatters](#input-formatters)                                        |
| **Sink**             | Connector that **exports** query results out of NebulaStream.                               | `INTO`, [See Sinks](#data-sinks-defining-the-output)                             |

---

### A Complete Query Example

Queries can be submitted as YAML specifications or SQL statements. Below is a complete example from the Linear Road Benchmark, which we'll break down section by section.

```yaml
query: |
  SELECT start, end, highway, direction, positionDiv5280, AVG(speed) AS avgSpeed
  FROM (SELECT creationTS, highway, direction, position / INT32(5280) AS positionDiv5280, speed FROM lrb)
  GROUP BY (highway, direction, positionDiv5280)
  WINDOW SLIDING(creationTS, SIZE 5 MINUTES, ADVANCE BY 1 SEC)
  HAVING avgSpeed < FLOAT32(40)
  INTO csv_sink;

sinks:
  - name: csv_sink
    schema:
      - name: lrb$start
        type: UINT64
      - name: lrb$end
        type: UINT64
      - name: lrb$highway
        type: INT16
      - name: lrb$direction
        type: INT16
      - name: positionDiv5280
        type: INT32
      - name: lrb$avgSpeed
        type: FLOAT64
    type: File
    config:
      input_format: CSV
      file_path: "<path>"
      append: false

logical:
  - name: lrb
    schema:
      - name: creationTS
        type: UINT64
      - name: vehicle
        type: INT16
      - name: speed
        type: FLOAT32
      - name: highway
        type: INT16
      - name: lane
        type: INT16
      - name: direction
        type: INT16
      - name: position
        type: INT32

physical:
  - logical: lrb
    type: TCP
    parser_config:
      type: CSV
      tuple_delimiter: "\n"
      field_delimiter: ","
    source_config:
      socket_host: localhost
      socket_port: 50501
      socket_buffer_size: 65536
      flush_interval_ms: 100
      connect_timeout_seconds: 60
  - logical: lrb
    host: worker-1:9090
    type: File
    parser_config:
      type: JSON
    source_config:
      file_path: lrb.json
```
This YAML can be sent to `nes-cli` to register or run the query.

Here's the equivalent SQL syntax:

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

CREATE PHYSICAL SOURCE FOR lrb TYPE TCP SET(
  'localhost' as `SOURCE`.SOCKET_HOST,
  50501 as `SOURCE`.SOCKET_PORT,
  65536 as `SOURCE`.SOCKET_BUFFER_SIZE,
  100 as `SOURCE`.FLUSH_INTERVAL_MS,
  60 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
  'CSV' as PARSER.`TYPE`,
  '\n' as PARSER.TUPLE_DELIMITER,
  ',' as PARSER.FIELD_DELIMITER
);

CREATE PHYSICAL SOURCE FOR lrb TYPE File SET(
  'lrb.json' as `SOURCE`.FILE_PATH,
  'JSON' as PARSER.`TYPE`
);

CREATE SINK csv_sink(
  lrb.start UINT64,
  lrb.end UINT64,
  lrb.highway INT16,
  lrb.direction INT16,
  positionDiv5280 INT32,
  lrb.avgSpeed FLOAT64
) TYPE File SET(
  '<path>' as `SINK`.FILE_PATH,
  'CSV' as `SINK`.INPUT_FORMAT,
  FALSE as `SINK`.APPEND
);

SELECT start, end, highway, direction, positionDiv5280, AVG(speed) AS avgSpeed
FROM (SELECT creationTS, highway, direction, position / INT32(5280) AS positionDiv5280, speed FROM lrb)
GROUP BY (highway, direction, positionDiv5280)
WINDOW SLIDING(creationTS, SIZE 5 MINUTES, ADVANCE BY 1 SEC)
HAVING avgSpeed < FLOAT32(40)
INTO csv_sink;
```
---

### Anatomy of a Query

#### Data Sources: Logical and Physical

NebulaStream separates **logical** and **physical** sources to provide flexible data ingestion.

##### Logical Sources

A logical source is like a **table definition** in a traditional database.
It provides an abstract description of a data stream with a `name` and `schema`.

-   The `name` references the stream in your query's `FROM` clause.
-   The `schema` defines the structure of data records (tuples), listing each field's name and data type.

Operators automatically infer their schemas from the source, so you only define it once.
Incoming data must strictly match this schema, including field order.
Any mismatch terminates the query.

Here is the logical source definition from our example:
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
This defines the `lrb` source used in the query's `FROM` clause.
The schema specifies seven fields per record.
Multiple logical sources can be defined and combined with `JOIN` or `UNION`.

##### Physical Sources

A physical source specifies **how** and **where** to ingest data for a logical source.
Each logical source can have multiple physical sources, allowing a single stream to aggregate data from heterogeneous endpoints.

Supported physical source types:
- `File`
- `TCP`

In our example, we define two physical sources that both feed the `lrb` logical source:
```sql
CREATE PHYSICAL SOURCE FOR lrb TYPE TCP SET(
  'localhost' as `SOURCE`.SOCKET_HOST,
  50501 as `SOURCE`.SOCKET_PORT,
  65536 as `SOURCE`.SOCKET_BUFFER_SIZE,
  100 as `SOURCE`.FLUSH_INTERVAL_MS,
  60 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
  'CSV' as PARSER.`TYPE`,
  '\n' as PARSER.TUPLE_DELIMITER,
  ',' as PARSER.FIELD_DELIMITER
);

CREATE PHYSICAL SOURCE FOR lrb TYPE File SET(
  'lrb.json' as `SOURCE`.FILE_PATH,
  'JSON' as PARSER.`TYPE`
);
```
As you can see, one source reads CSV-formatted data from a TCP socket, while the other reads JSON-formatted data from a file.
Both produce tuples that conform to the `lrb` schema.

The CSV file might look like this:
```
creationTS,vehicle,speed,highway,lane,direction,position
1234567890,101,65.5,1,2,0,15840
1234567891,102,70.2,1,3,0,21120
1234567892,103,55.8,2,1,1,10560
1234567893,101,68.3,1,2,0,16896
```

Each physical source requires configuration for:
- The specific connector (e.g., file path or TCP socket details) via `SOURCE.*` parameters.
- The data's input format (e.g., `CSV` or `JSON`) and delimiters via `PARSER.*` parameters.

The query itself remains completely decoupled from these physical details.
You can add, remove, or change physical sources without touching the query logic.

---

#### Data Sinks: Defining the Output

Sinks represent the destination for query results.
Currently, a query must have exactly one sink.

```sql
CREATE SINK csv_sink(
  lrb.start UINT64,
  lrb.end UINT64,
  lrb.highway INT16,
  lrb.direction INT16,
  positionDiv5280 INT32,
  lrb.avgSpeed FLOAT64
) TYPE File SET(
  '<path>' as `SINK`.FILE_PATH,
  'CSV' as `SINK`.INPUT_FORMAT,
  FALSE as `SINK`.APPEND
);
```
The sink name (`csv_sink`) must match the name used in the query's `INTO` clause.

Available sink types include:
- `File`: Writes results to a file, either overwriting or appending.
- `Print`: Writes results to standard output (stdout).

The `SET` clause specifies the output details.
For a `File` sink, this includes the file path and the data format for the output.

---
## Input Formatters
Tuples can arrive in a variety of formats.
We distinguish two broad categories:
- Text-based formats (JSON, CSV, XML, YAML, etc.)
- Binary formats (Avro, Parquet, Protobuf, etc.)

Input formatters convert byte streams from source connectors into the native in-memory representation used by query-compiled operators.
The format is specified via `PARSER.*` parameters in each physical source:
```sql
CREATE PHYSICAL SOURCE FOR source_name TYPE TCP SET(
  'CSV' as PARSER.`TYPE`,
  '\n' as PARSER.TUPLE_DELIMITER,
  ',' as PARSER.FIELD_DELIMITER,
  ...
);
```

Currently, we support the text-based CSV format, with JSON following in an upcoming release.

---
## Data Types
In NebulaStream, each field is associated with exactly one data type.
This data type specifies the physical memory layout and valid operations on the field.

Supported data types:
- `INT8`
- `UINT8`
- `INT16`
- `UINT16`
- `INT32`
- `UINT32`
- `INT64`
- `UINT64`
- `FLOAT32`
- `FLOAT64`
- `CHAR`
- `BOOL`
- `VARSIZED`

These types match primitive C++ data types.
The numeric suffix denotes the bit width.
`VARSIZED` supports arbitrary-length data like strings.
For output types of arithmetical operations, we stick to the C++ standard, c.f.[Integer Promotions](https://en.cppreference.com/w/cpp/language/implicit_conversion.html#Integer_promotions) and [Conversion Ranks](https://en.cppreference.com/w/cpp/language/usual_arithmetic_conversions.html#Integer_conversion_rank).

---

## Operators

An operator consumes an input stream and produces an output stream.
We differentiate between **stateless** and **stateful** operators.
Stateless operators produce output tuples without buffering the stream.

Operators are either unary (one input stream) or binary (two input streams).
All operators produce a single output stream.
Data flows from sources to a single sink via unary operators (selection, projection) or binary operators (join, union).

### Stateless Operators

| Operator   | Description                                         |
|------------|-----------------------------------------------------|
| Projection | Enumerate fields, functions, and subqueries         |
| Selection  | Filter tuples based on a predicate                  |
| Union      | Combine two streams with the same underlying schema |

#### Projection

Projections are compositions of functions that are enumerated after the `SELECT` keyword.

```sql
SELECT a, b, c FROM s INTO sink
```

```sql
SELECT speed * FLOAT32(3.6) AS speed_m_sec FROM s INTO sink
```

```sql
SELECT CONCAT(firstName, lastName) AS firstNameLastName FROM nameStream INTO firstNameLastNameSink;
```

💡 Constants must be wrapped in an explicit cast to specify their type.

```sql
SELECT FLOAT64(3.141) * r FROM stream INTO sink
```

#### Selection

Selections use the `WHERE` keyword to filter the input stream.

```sql
SELECT * FROM s WHERE t == VARSIZED("sometext") INTO sink
```

Predicates can be arbitrary compositions of functions:

```sql
SELECT * FROM s WHERE CEIL(speed) != UINT64(0) OR altitude == 0 INTO sink
```

```sql
SELECT * FROM transactions WHERE amount > FLOAT64(1000.0) AND status == VARSIZED("completed") INTO sink
```

#### Union

Union combines two input streams with identical schema into one.

```sql
SELECT * FROM s UNION (SELECT * FROM t) INTO sink
```

```sql
SELECT user_id, action, timestamp FROM web_events 
UNION (SELECT user_id, action, timestamp FROM mobile_events) INTO sink
```

💡 Union does not deduplicate values as in classical relational algebra.

### Stateful/Windowed Operators

| Operator    | Description                                         |
|-------------|-----------------------------------------------------|
| Aggregation | Accumulate windows of a single stream               |
| Join        | Combine two streams in windows based on a predicate |

Stateful operators require more context than a single tuple to produce an output.
In batch systems, these operations would require all input data to be seen before emitting results.
This is not feasible in stream processing systems that deal with unbounded datasets.
Therefore, we chunk the stream up into **windows**.

#### Window Types

Two window types are supported:

**Tumbling Windows**

Tumbling windows chunk the stream into disjoint subsets, for example for timestamps `(1...6)` and a window size of 3 `[1 2 3][4 5 6]`.

Syntax: `WINDOW TUMBLING(<timestamp_field>, <size><unit>)`

```sql
WINDOW TUMBLING(ts, SIZE 1 SEC) INTO sink
```

**Sliding Windows**

Sliding windows chunk the stream into overlapping subsets, for example: `[1s 2s][2s 3s][3s 4s]`

Syntax: `WINDOW SLIDING(<timestamp_field>, SIZE <size><unit>, ADVANCE BY <size><unit>)`

💡 The timestamp field needs to be of type UINT64, with a millisecond resolution.

```sql
WINDOW SLIDING(ts, SIZE 1 SEC, ADVANCE BY 100 MS) INTO sink
```

#### Window Measures

Two window measures are supported:

**Event Time**

Event time uses timestamps defined in the tuples themselves to assign them to the correct windows.

💡 For binary windowed operators like joins, the timestamp field must have the same name for both input streams.

**Ingestion Time**

Ingestion time assigns tuples to windows based on the timestamp when the tuple was first ingested into the system.
We omit the timestamp field specifier in the window definition:

```sql
WINDOW TUMBLING(SIZE 1 MIN) INTO sink
```

#### Aggregation

Aggregations allow you to compute summary statistics over windows of data.
Common aggregation functions include mathematical operations like `MAX`, `MIN`, `SUM`, `AVG`, and statistical functions like `MEDIAN`.

```sql
SELECT MAX(price) FROM bid GROUP BY ticker WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 1 SEC) INTO sink
```

```sql
SELECT MEDIAN(oxygen_level) FROM health_sensor WINDOW TUMBLING(ts, SIZE 100 MS) INTO sink
```

```sql
SELECT COUNT(*) AS event_count, AVG(response_time) AS avg_response 
FROM api_requests 
GROUP BY endpoint 
WINDOW TUMBLING(ts, SIZE 5 MIN) INTO sink
```

Windowed aggregations support an optional `GROUP BY` clause to specify grouping keys.
A `HAVING` clause applies filters to aggregated results.

```sql
SELECT ticker, MAX(price) AS max_price, MIN(price) AS min_price
FROM stock_quotes 
GROUP BY ticker 
WINDOW TUMBLING(ts, SIZE 1 MIN)
HAVING MAX(price) > FLOAT64(100.0) AND COUNT(*) >= UINT64(10) INTO sink
```

#### Join

Joins combine tuples from two input streams based on a condition within a window.
Only tuples that satisfy the join predicate are included in the output.

```sql
SELECT * FROM s INNER JOIN (SELECT * FROM t) ON sid = tid WINDOW TUMBLING(ts, SIZE 1 MIN) INTO sink
```

```sql
SELECT order_id, customer_id, amount 
FROM orders
INNER JOIN (SELECT * FROM payments p) ON order_id = payments_order_id 
WINDOW SLIDING(ts, SIZE 30 SEC, ADVANCE BY 5 SEC) INTO sink
```

💡 Currently, the timestamp field is required to have the same name in both input streams.

---
## Functions

A function (also known as scalar expression) specifies an operation on one or more fields.
For example, `SELECT a + b FROM stream INTO sink` uses the `ADD` function.
Every expression is a function, including field access and constants.
We refer to input parameters as *input functions*.

Functions are either unary (one input) or binary (two inputs).
`ABS` is a unary function, while `+` is a binary function.

### Supported Functions

#### **Base**

| Function                 | Example                             |
|--------------------------|-------------------------------------|
| Access a field           | `SELECT x FROM s INTO sink`         |
| Define a constant        | `SELECT INT32(42) FROM s INTO sink` |
| Rename an input function | `SELECT x AS x1 FROM s INTO sink`   |
| Cast an input function   | `SELECT x FROM s INTO sink`         |

#### **Arithmetical**

| Function                                        | Example                                 |
|-------------------------------------------------|-----------------------------------------|
| Addition                                        | `SELECT x + INT32(10) FROM s INTO sink` |
| Subtraction                                     | `SELECT x - y FROM s INTO sink`         |
| Division                                        | `SELECT x / y FROM s INTO sink`         |
| Multiplication                                  | `SELECT x * y FROM s INTO sink`         |
| Exponentiation                                  | `SELECT EXP(x, y) FROM s INTO sink`     |
| Power                                           | `SELECT POW(x, 2) FROM s INTO sink`     |
| Square Root                                     | `SELECT SQRT(x) FROM s INTO sink`       |
| Modulo                                          | `SELECT x % y FROM s INTO sink`         |
| Round to the nearest integer larger than `x`    | `SELECT CEIL(x) FROM s INTO sink`       |
| Round to the nearest integer smaller than `x`   | `SELECT FLOOR(x) FROM s INTO sink`      |
| Round a float to the specified number of digits | `SELECT ROUND(x, 4) FROM s INTO sink`   |
| Absolute Value                                  | `SELECT ABS(x) FROM s INTO sink`        |

#### **Boolean/Comparison**

| Function           | Example                                          |
|--------------------|--------------------------------------------------|
| Logical `AND`      | `SELECT * FROM s WHERE a AND b INTO sink`        |
| Logical `OR`       | `SELECT * FROM s WHERE a OR b INTO sink`         |
| Equal              | `SELECT * FROM s WHERE a == INT32(42) INTO sink` |
| Not Equal          | `SELECT * FROM s WHERE a != INT32(42) INTO sink` |
| Greater            | `SELECT * FROM s WHERE a > b INTO sink`          |
| Greater or Equal   | `SELECT * FROM s WHERE a >= b INTO sink`         |
| Less Than          | `SELECT * FROM s WHERE a < b INTO sink`          |
| Less Than or Equal | `SELECT * FROM s WHERE a <= b INTO sink`         |

#### **Other**

| Description                     | Example                                        |
|---------------------------------|------------------------------------------------|
| Concatenate variable-sized data | `SELECT CONCAT(text1, text2) FROM s INTO sink` |

We can combine functions into nested structures:
```sql
SELECT POW((x AS actual) - (y AS predicted), 2) FROM s INTO sink
```
This calculates the squared error between a prediction and ground truth.
Query compilation traces expression trees at compile time, producing efficient machine code instead of runtime evaluation. 

#### **Aggregation**
| Function | Example                                                                              |
|----------|--------------------------------------------------------------------------------------|
| Sum      | `SELECT SUM(x) FROM s WINDOW TUMBLING(ts, SIZE 30 SEC) INTO sink`                    |
| Min      | `SELECT MIN(x) FROM s WINDOW TUMBLING(ts, SIZE 10 MIN) INTO sink`                    |
| Max      | `SELECT MAX(x) FROM s WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 2 SEC) INTO sink`   |
| Count    | `SELECT COUNT(x) FROM s WINDOW SLIDING(ts, SIZE 1 SEC, ADVANCE BY 100 MS) INTO sink` |
| Average  | `SELECT AVG(x) FROM s WINDOW SLIDING(ts, SIZE 1 MIN, ADVANCE BY 15 SEC) INTO sink`   |
| Median   | `SELECT MEDIAN(x) FROM s WINDOW TUMBLING(ts, SIZE 1 SEC) INTO sink`                  |

