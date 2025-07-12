# NebulaStream Query API Guide

NebulaStream offers rich stream processing functionality through a declarative, SQL-like query language. 
This guide explains the core concepts of the API and walks you through creating a query.

The fundamental data flow in NebulaStream is simple:
1.  **Sources** ingest data into the system.
2.  **Operators** process and transform the data in-flight.
3.  **Sinks** emit the final results to an external system or the user.

To get started, let's define some key terminology.

---

### Core Concepts Glossary

| Term                 | Description                                                                                                | Usage                                                                            |
|:---------------------|:-----------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------|
| **Stream**           | An unbounded sequence of data records (tuples) processed by NebulaStream.                                  | `FROM`, `INTO`                                                                   |
| **Tuple**            | A single record or event in a stream, composed of one or more fields.                                      | Internal                                                                         |
| **Schema**           | The structure of a tuple, defining its fields and their data types.                                        | [See Sources](#data-sources-logical-and-physical)                                |
| **Field**            | An atomic unit of data within a tuple, defined by a name and a data type.                                  | Internal                                                                         |
| **Data Type**        | Specifies how to interpret a field's data and which operations are valid.                                  | `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`, `CHAR`, `BOOL`, `VARSIZED` |
| **Source**           | A connector that **ingests** external data, creating a stream.                                             | `FROM`, [See Sources](#data-sources-logical-and-physical)                        |
| **Input Formatter**  | A component that decodes raw data from a source into NebulaStream's internal tuple format.                 | [See Input Formatters](#input-formatters)                                        |
| **Operator**         | A function that transforms a stream of tuples (e.g., filtering, aggregating).                              | `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, [See Operators](#operators)               |
| **Function**         | An operation applied to one or more fields within an operator.                                             | `SUM`, `AVG`, `+`, `-`, `CONCAT`, [See Functions](#functions)                    |
| **Window**           | A mechanism to partition an unbounded stream into finite chunks for stateful operations like aggregations. | `WINDOW (TUMBLING\|SLIDING) (timestamp, [duration][unit])`                       |
| **Output Formatter** | A component that encodes tuples into a specific format for a sink.                                         | [See Input Formatters](#input-formatters)                                        |
| **Sink**             | A connector that **exports** query results out of NebulaStream.                                            | `INTO`, [See Sinks](#data-sinks-defining-the-output)                             |

---

### A Complete Query Example

Queries are submitted to NebulaStream as self-contained YAML specifications. 
Let's look at a complete example from the Linear Road Benchmark. 
We will then break down each section of this file.

```yaml
query: |
  SELECT start, end, highway, direction, positionDiv5280, AVG(speed) AS avgSpeed
  FROM (SELECT creationTS, highway, direction, position / INT32(5280) AS positionDiv5280, speed FROM lrb)
  GROUP BY (highway, direction, positionDiv5280)
  WINDOW SLIDING(creationTS, SIZE 5 MINUTES, ADVANCE BY 1 SEC)
  HAVING avgSpeed < FLOAT32(40)
  INTO csv_sink;
  
sink:
  name: csv_sink
  type: File
  config:
    inputFormat: CSV
    filePath: "<path>"
    append: false

logicalSources:
  - name: lrb
    schema:
      - name: creationTS
        dataType: UINT64
      - name: vehicle 
        dataType: INT16
      - name: speed
        dataType: FLOAT32
      - name: highway
        dataType: INT16
      - name: lane
        dataType: INT16
      - name: direction
        dataType: INT16
      - name: position
        dataType: INT32
        
physicalSources:
  - logicalSource: lrb
    parserConfig:
      type: CSV
      tupleDelimiter: "\n"
      fieldDelimiter: ","
    sourceConfig:
      type: TCP
      socketHost: localhost
      socketPort: 50501
      socketBufferSize: 65536
      flushIntervalMS: 100
      connectTimeoutSeconds: 60
  - logicalSource: lrb
    parserConfig:
      type: JSON
    sourceConfig:
      type: File
      filePath: LRB.json
```
This YAML specification can be sent to the NebulaStream coordinator (`nebuli`) to register the query or run it directly on a worker.

---

### Anatomy of a Query YAML

#### Data Sources: Logical and Physical

A core concept in NebulaStream is the separation between **logical** and **physical** sources. This provides flexibility in how data is ingested.

##### Logical Sources

Think of a logical source as a **table definition** in a traditional database. 
It provides an abstract description of a data stream, giving it a `name` and a `schema`.

-   The `name` is used to reference the stream in your query's `FROM` clause.
-   The `schema` defines the structure of the data records (tuples), listing each field's name and data type.

All operators after the source will automatically infer their schemas, so you only need to define it once. 
It is crucial that incoming data strictly adheres to this schema, including the order of fields. 
Any mismatch will cause the query to terminate.

Here is the logical source definition from our example:
```yaml
logicalSources:
  - name: lrb
    schema:
      - name: creationTS
        dataType: UINT64
      - name: vehicle
        dataType: INT16
      - name: speed
        dataType: FLOAT32
      - name: highway
        dataType: INT16
      - name: lane
        dataType: INT16
      - name: direction
        dataType: INT16
      - name: position
        dataType: INT32
```
This defines a source named `lrb`, which is then used in the query (`FROM lrb`). 
The schema describes the seven fields that make up each record in the `lrb` stream. 
You could define multiple logical sources here and combine them using `JOIN` or `UNION` operators in the query.

##### Physical Sources

A physical source provides the concrete details for **how and from where** to ingest data for a specific logical source. 
There can be a one-to-many relationship between a logical source and its physical sources. 
This means a single logical stream can be fed by data from multiple, heterogeneous physical endpoints.

Currently, NebulaStream supports the following physical source types:
- `File`
- `TCP`

An `MQTT` source is planned for a future release.

In our example, we define two physical sources that both feed the `lrb` logical source:
```yaml
physicalSources:
  - logicalSource: lrb
    parserConfig:
      type: CSV
      tupleDelimiter: "\n"
      fieldDelimiter: ","
    sourceConfig:
      type: TCP
      socketHost: localhost
      socketPort: 50501
      socketBufferSize: 65536
      flushIntervalMS: 100
      connectTimeoutSeconds: 60
  - logicalSource: lrb
    parserConfig:
      type: JSON
    sourceConfig:
      type: File
      filePath: LRB.json
```
As you can see, one source reads CSV-formatted data from a TCP socket, while the other reads JSON-formatted data from a file. 
Both produce tuples that conform to the `lrb` schema.

Each physical source requires two configuration blocks:
- `sourceConfig`: Configures the specific connector (e.g., the file path or TCP socket details).
- `parserConfig`: Informs NebulaStream about the data's input format (e.g., `CSV` or `JSON`) and its specific delimiters.

The query itself remains completely decoupled from these physical details.
You could add, remove, or change physical sources without touching the query logic.

---

#### Data Sinks: Defining the Output

Sinks are the counterpart to sources: they represent the destination for your query's final results. 
They are simpler to configure because their schema is automatically inferred from the final operator in the query plan. 
Currently, a query is constrained to have exactly one sink.

```yaml
sink:
  name: csv_sink
  type: File
  config:
    inputFormat: CSV
    filePath: "<path>"
    append: false
```
The `name` of the sink (`csv_sink`) must match the name used in the query's `INTO` clause.

Available sink `type`s include:
- `FileSink`: Writes results to a file, either overwriting or appending.
- `PrintSink`: Writes results to standard output (stdout), which is useful for debugging.

The `config` block specifies the output details. 
For a `FileSink`, this includes the `filePath` and the data format for the output, as specified here with `inputFormat: CSV`.

---
## Input Formatters
Tuples can arrive in a variety of different formats.
At a high level, we distinguish two broad categories:
- Text-based formats (JSON, CSV, XML, YAML, etc.)
- Binary formats (like Avro, Parquet, Protobuf, etc.)

To convert the byte stream in one of the mentioned formats that is emitted by source connectors into native in-memory representation understood by our query-compiled operators, we use *input formatters*.
The format is specified in the `parserConfig`-block of each physical source:
```yaml
parserConfig:
  type: CSV
  tupleDelimiter: "\n"
  fieldDelimiter: ","
```

Currently, we support the text-based CSV format, with JSON following in an upcoming release.

---
## Operators

Conceptually, an operator consumes an input stream and produces an output stream.
We differentiate between **stateless** and **stateful** operators.
A stateless operator can produce an output tuple without buffering the stream.
Currently, an operator is either unary (consumes a single input stream) or binary (consumes two input streams).
All operators produce a single output stream, meaning data flows from the leaves (sources) to a single common root (sink) via unary operators (selection, projection) or binary operators (join, union).

### Stateless Operators

| Operator   | Description                                         |
|------------|-----------------------------------------------------|
| Projection | Enumerate fields, functions, and subqueries         |
| Selection  | Filter tuples based on a predicate                  |
| Union      | Combine two streams with the same underlying schema |

#### Projection

Projections are compositions of functions that are enumerated after the `SELECT` keyword.

```sql
SELECT a, b, c FROM s
```

```sql
SELECT speed * FLOAT32(3.6) AS speed_m_sec FROM s
```

```sql
SELECT user_id, UPPER(username) AS username_upper, created_at FROM users
```

💡 Whenever you use a constant function, you currently need to wrap it in an explicit cast to guide the system on what type it should use.

```sql
SELECT FLOAT64(3.141) * r FROM stream
```

#### Selection

Selections follow a `WHERE` keyword and specify a filter condition to select a subset of the input stream.

```sql
SELECT * FROM s WHERE t == VARSIZED("sometext")
```

Similar to projections, these predicates can also come in the form of arbitrary compositions of input functions:

```sql
SELECT * FROM s WHERE CEIL(speed) != UINT64(0) OR altitude == 0
```

```sql
SELECT * FROM transactions WHERE amount > FLOAT64(1000.0) AND status == VARSIZED("completed")
```

#### Union

Union combines two input streams with identical schema into one.

```sql
SELECT * FROM s UNION (SELECT * FROM t)
```

```sql
SELECT user_id, action, timestamp FROM web_events 
UNION (SELECT user_id, action, timestamp FROM mobile_events)
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

Currently, you can choose one of the following two window types:

**Tumbling Windows**

Tumbling windows chunk the stream into disjoint subsets, for example: `[1s 2s 3s][4s 5s 6s]`

Syntax: `WINDOW TUMBLING(<timestamp_field>, <size><unit>)`

```sql
WINDOW TUMBLING(ts, SIZE 1 SEC)
```

**Sliding Windows**

Sliding windows chunk the stream into overlapping subsets, for example: `[1s 2s][2s 3s][3s 4s]`

Syntax: `WINDOW SLIDING(<timestamp_field>, SIZE <size><unit>, ADVANCE BY <size><unit>)`

```sql
WINDOW SLIDING(ts, SIZE 1 SEC, ADVANCE BY 100 MS)
```

#### Window Measures

Additionally, we support two window measures:

**Event Time**

Event time uses timestamps defined in the tuples themselves to assign them to the correct windows.

💡 For binary windowed operators like joins, the timestamp field must have the same name for both input streams.

**Ingestion Time**

Ingestion time assigns tuples to windows based on the timestamp when the tuple was first ingested into the system.
We omit the timestamp field specifier in the window definition:

```sql
WINDOW TUMBLING(SIZE 1 MIN)
```

#### Aggregation

Aggregations allow you to compute summary statistics over windows of data.
Common aggregation functions include mathematical operations like `MAX`, `MIN`, `SUM`, `AVG`, and statistical functions like `MEDIAN`.

```sql
SELECT MAX(price) FROM bid GROUP BY ticker WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 1 SEC)
```

```sql
SELECT MEDIAN(oxygen_level) FROM health_sensor WINDOW TUMBLING(ts, SIZE 100 MS)
```

```sql
SELECT COUNT(*) AS event_count, AVG(response_time) AS avg_response 
FROM api_requests 
GROUP BY endpoint 
WINDOW TUMBLING(ts, SIZE 5 MIN)
```

In windowed aggregations, you can optionally specify a `GROUP BY` clause and enumerate all functions that will be used as grouping keys.
Moreover, a `HAVING` clause followed by a predicate allows you to apply a selection on the aggregated results.

```sql
SELECT ticker, MAX(price) AS max_price, MIN(price) AS min_price
FROM stock_quotes 
GROUP BY ticker 
WINDOW TUMBLING(ts, SIZE 1 MIN)
HAVING MAX(price) > FLOAT64(100.0) AND COUNT(*) >= UINT64(10)
```

#### Join

Joins combine tuples from two input streams based on a specified condition within a time window.
The join condition is evaluated for each pair of tuples from the two streams that fall within the same window.
Only tuples that satisfy the join predicate are included in the output stream.

```sql
SELECT * FROM s INNER JOIN (SELECT * FROM t) ON sid = tid WINDOW TUMBLING(ts, SIZE 1 MIN)
```

```sql
SELECT order_id, customer_id, amount 
FROM orders
INNER JOIN (SELECT * FROM payments p) ON order_id = payments_order_id 
WINDOW SLIDING(ts, SIZE 30 SEC, ADVANCE BY 5 SEC)
```
---
## Functions

A function (also known as expression) specifies an operation on one or more fields.
For example, `SELECT a + b FROM stream` would uses `ADD` function that adds the two fields together.
Every expression that you use in your query is a function, even a field access or a constant.
Therefore, we'll refer to any input parameters to a function as its *input function*.
As with operators, most functions are either unary (single input function) or binary (two input functions).
`ABS` is an example for a unary function, whereas `+` is an example for a binary function.

### Supported Functions

#### **Base**

| Function                 | Example                   |
|--------------------------|---------------------------|
| Access a field           | `SELECT x FROM s`         |
| Define a constant        | `SELECT INT32(42) FROM s` |
| Rename an input function | `SELECT x AS x1 FROM s`   |
| Cast an input function   | `SELECT x FROM s`         |

#### **Arithmetical**

| Function                                        | Example                       |
|-------------------------------------------------|-------------------------------|
| Addition                                        | `SELECT x + INT32(10) FROM s` |
| Subtraction                                     | `SELECT x - y FROM s`         |
| Division                                        | `SELECT x / y FROM s`         |
| Multiplication                                  | `SELECT x * y FROM s`         |
| Exponentiation                                  | `SELECT EXP(x, y) FROM s`     |
| Power                                           | `SELECT POW(x, 2) FROM s`     |
| Square Root                                     | `SELECT SQRT(x) FROM s`       |
| Modulo                                          | `SELECT x % y FROM s`         |
| Round to the nearest integer larger than `x`    | `SELECT CEIL(x) FROM s`       |
| Round to the nearest integer smaller than `x`   | `SELECT FLOOR(x) FROM s`      |
| Round a float to the specified number of digits | `SELECT ROUND(x, 4) FROM s`   |
| Absolute Value                                  | `SELECT ABS(x) FROM s`        |

#### **Boolean/Comparison**

| Function           | Example                                |
|--------------------|----------------------------------------|
| Logical `AND`      | `SELECT * FROM s WHERE a AND b`        |
| Logical `OR`       | `SELECT * FROM s WHERE a OR b`         |
| Equal              | `SELECT * FROM s WHERE a == INT32(42)` |
| Not Equal          | `SELECT * FROM s WHERE a != INT32(42)` |
| Greater            | `SELECT * FROM s WHERE a > b`          |
| Greater or Equal   | `SELECT * FROM s WHERE a >= b`         |
| Less Than          | `SELECT * FROM s WHERE a < b`          |
| Less Than or Equal | `SELECT * FROM s WHERE a <= b`         |

#### **Other**

| Description                     | Example                              |
|---------------------------------|--------------------------------------|
| Concatenate variable-sized data | `SELECT CONCAT(text1, text2) FROM s` |

We can combine functions into nested structures:
```sql
SELECT POW((x AS actual) - (y AS predicted), 2) FROM s
```
In this example, we calculate the squared error between a prediction and the ground truth.
Due to our use of query compilation, complex expression trees don't need to be evaluated at runtime for every tuple.
Instead, at compile time of the query, it will be traced by the query compiler, producing efficient machine code dedicated to the query at hand. 

#### **Aggregation**
| Function | Example                                                                    |
|----------|----------------------------------------------------------------------------|
| Sum      | `SELECT SUM(x) FROM s WINDOW TUMBLING(ts, SIZE 30 SEC)`                    |
| Min      | `SELECT MIN(x) FROM s WINDOW TUMBLING(ts, SIZE 10 MIN)`                    |
| Max      | `SELECT MAX(x) FROM s WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 2 SEC)`   |
| Count    | `SELECT COUNT(x) FROM s WINDOW SLIDING(ts, SIZE 1 SEC, ADVANCE BY 100 MS)` |
| Average  | `SELECT AVG(x) FROM s WINDOW SLIDING(ts, SIZE 1 MIN, ADVANCE BY 15 SEC)`   |
| Median   | `SELECT MEDIAN(x) FROM s WINDOW TUMBLING(ts, SIZE 1 SEC)`                  |

---
## Data Types

Currently, the following data types are usable:
- `INT8`
- `UINT8`
- `INT16`
- `UINT16`
- `INT32`
- `UNT32`
- `INT64`
- `UINT64`
- `FLOAT32`
- `FLOAT64`
- `CHAR`
- `BOOL`
- `VARSIZED`
