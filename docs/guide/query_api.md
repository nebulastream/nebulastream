# NebulaStream Query API Guide

NebulaStream provides stream processing through a declarative, SQL-like query language. This page defines
the core terminology and syntax rules shared across the query language. For a hands-on walkthrough of
submitting a query, see [Quick Start](get_started.md).

The fundamental data flow is simple:
1.  **Sources** ingest data into the system.
2.  **Operators** process and transform data in-flight.
3.  **Sinks** emit results to external systems (databases, files) or to the console.

Let's start with key terminology.

---

### Core Concepts Glossary

| Term | Description | Usage |
|:---|:---|:---|
| **Stream** | Unbounded sequence of data records (tuples). | `FROM`, `INTO` |
| **Tuple** | A single record or event in a stream, composed of one or more fields. | Internal |
| **Schema** | Logical structure of a tuple, defining its fields and their data types. | [See Sources and Sinks](sources-and-sinks.md) |
| **Field** | Atomic unit of data within a tuple, defined by a name and a data type. | Internal |
| **Data Type** | Specifies how to interpret a field's data and which operations are valid. | `INT8`, `UINT8`, `INT16`, `UINT16`, `INT32`, `UINT32`, `INT64`, `UINT64`, `FLOAT32`, `FLOAT64`, `CHAR`, `BOOLEAN`, `VARSIZED` |
| **Source** | Connector that **ingests** external data, creating a stream. | `FROM`, [See Sources and Sinks](sources-and-sinks.md) |
| **Input Formatter** | Decodes raw data from a source into internal tuple format. | [See Sources and Sinks](sources-and-sinks.md#input-formatters) |
| **Operator** | Transforms a stream of tuples (e.g., filtering, aggregating). | `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, [See Operators](operators.md) |
| **Function** | Operation applied to one or more fields (or input functions) within an operator. | `SUM`, `AVG`, `+`, `-`, `CONCAT`, [See Functions](functions.md) |
| **Window** | Partition an unbounded stream into finite chunks for stateful operations like aggregations. | `WINDOW TUMBLING(timestamp, SIZE size unit)`, [See Operators](operators.md#windows) |
| **Output Formatter** | Encodes tuples into a specific format to prepare for a sink. | [See Sources and Sinks](sources-and-sinks.md#output-formatters) |
| **Sink** | Connector that **exports** query results out of NebulaStream. | `INTO`, [See Sources and Sinks](sources-and-sinks.md) |

---

## Identifiers and Quotation Marks

Identifiers are the names of SQL objects, including sources, sinks, fields, aliases, and configuration keys. Simple identifiers can be written without quotes. Double quotes allow identifiers to contain spaces or preserve their exact spelling:

```sql
SELECT "event type" AS "Event Type" FROM "input stream" INTO sink;
```

Double-quoted text refers to an identifier, while single-quoted text represents a string value:

```sql
SELECT "status" AS field_value, 'status' AS literal_value FROM stream INTO sink;
```

Here, `"status"` refers to a field named `status`, whereas `'status'` is the literal string `status`.

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
- `BOOLEAN`
- `VARSIZED`

These types match primitive C++ data types.
The numeric suffix denotes the bit width.
`VARSIZED` supports arbitrary-length data like strings.
For output types of arithmetical operations, we stick to the C++ standard, c.f.[Integer Promotions](https://en.cppreference.com/w/cpp/language/implicit_conversion.html#Integer_promotions) and [Conversion Ranks](https://en.cppreference.com/w/cpp/language/usual_arithmetic_conversions.html#Integer_conversion_rank).

### Numeric literals

Numeric constants can be written directly in query expressions:

```sql
SELECT speed * 3.6 AS speed_m_sec FROM s INTO sink
SELECT * FROM s WHERE count >= 10 AND delta > -5 INTO sink
```

NebulaStream infers a raw numeric literal's data type from its value:
- Integer literals without a leading minus sign use the smallest unsigned integer type that can represent the value: `UINT8`, `UINT16`, `UINT32`, or `UINT64`.
- Negative integer literals use the smallest signed integer type that can represent the value: `INT8`, `INT16`, `INT32`, or `INT64`.
- Floating-point literals, including fractional and exponent notation such as `0.1`, `42.0`, `.5`, and `1E3`, use `FLOAT64`.

Use an explicit type constructor when a query depends on an exact literal type:

```sql
SELECT UINT64(1) AS id, FLOAT32(3.6) AS scale FROM s INTO sink
```

### String and boolean literals

String and boolean constants can also be written directly in query expressions:

```sql
SELECT 'hello' AS message, TRUE AS enabled FROM s INTO sink
SELECT * FROM s WHERE status == 'completed' AND enabled == false INTO sink
```

Raw string literals infer `VARSIZED`, including numeric-looking strings such as `'123'`. Raw `TRUE` and `FALSE` literals infer `BOOLEAN`; lowercase `true` and `false` are also supported. Explicit constructors remain available when desired:

```sql
SELECT VARSIZED('hello') AS message, BOOLEAN(TRUE) AS enabled FROM s INTO sink
```
