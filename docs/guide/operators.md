# Operators

This page is the reference for every query operator NebulaStream supports: the operations a `SELECT`
statement is built out of, and the parameters each one takes. For the scalar/aggregate functions used
inside these operators (`ABS`, `SUM`, `CONCAT`, …), see [Functions](functions.md).

An operator consumes an input stream and produces an output stream. Operators are either unary (one
input stream) or binary (two input streams); all operators produce a single output stream. We also
differentiate stateless operators, which produce output tuples without buffering the stream, from
stateful operators, which accumulate a window of tuples before they can produce output.

| Operator | Kind | Purpose |
|---|---|---|
| [Projection](#projection) | Stateless | Enumerate fields, functions, and subqueries |
| [Selection](#selection) | Stateless | Filter tuples based on a predicate |
| [Union](#union) | Stateless | Combine two streams with the same underlying schema |
| [Aggregation](#aggregation) | Stateful | Accumulate windows of a single stream |
| [Join](#join) | Stateful | Combine two streams in windows based on a predicate |
| [Model Inference](#model-inference) | Stateless | Run an ONNX ML model on each tuple in a stream |

---

## Projection

**Syntax:** `SELECT <field | function | subquery> [AS alias], ... FROM <source> INTO <sink>`

A projection is a composition of [functions](functions.md) enumerated after `SELECT`. It is a stateless,
unary operator.

```sql
SELECT a, b, c FROM s INTO sink
```

```sql
SELECT speed * 3.6 AS speed_m_sec FROM s INTO sink
```

```sql
SELECT CONCAT(firstName, lastName) AS firstNameLastName FROM nameStream INTO firstNameLastNameSink;
```

💡 Use an explicit type constructor when a constant needs an exact type:

```sql
SELECT FLOAT32(3.141) * r FROM stream INTO sink
```

## Selection

**Syntax:** `SELECT ... FROM <source> WHERE <predicate> INTO <sink>`

Selection is a stateless, unary operator that filters the input stream using the `WHERE` keyword. The
predicate is built from the [boolean and comparison functions](functions.md#boolean-and-comparison-functions),
which lists everything `WHERE` accepts, including `BETWEEN`, `IN (...)`, `IS [NOT] NULL`, and
`IS [NOT] NaN`.

```sql
SELECT * FROM s WHERE t == 'sometext' INTO sink
```

```sql
SELECT * FROM s WHERE CEIL(speed) != 0 OR altitude == 0 INTO sink
```

```sql
SELECT * FROM transactions WHERE amount BETWEEN 100.0 AND 1000.0 AND status == 'completed' INTO sink
```

## Union

**Syntax:** `<query> UNION (<query>) INTO <sink>`

Union is a stateless, binary operator that combines two input streams with an identical schema into one.
Union does not deduplicate values, unlike classical relational-algebra union.

```sql
SELECT * FROM s UNION (SELECT * FROM t) INTO sink
```

```sql
SELECT user_id, action, timestamp FROM web_events
UNION (SELECT user_id, action, timestamp FROM mobile_events) INTO sink
```

---

## Windows

Stateful operators ([Aggregation](#aggregation), [Join](#join)) need more context than a single tuple to
produce output. The stream is chunked into windows to give them that context. Both stateful operators
share the same window syntax.

### Window types

| Type | Syntax | Description |
|---|---|---|
| Tumbling | `WINDOW TUMBLING(<timestamp_field>, SIZE <size> <unit>)` | Chunks the stream into disjoint, fixed-size subsets. For timestamps `1..6` and a window size of 3: `[1 2 3][4 5 6]`. |
| Sliding | `WINDOW SLIDING(<timestamp_field>, SIZE <size> <unit>, ADVANCE BY <size> <unit>)` | Chunks the stream into overlapping subsets. For example: `[1s 2s][2s 3s][3s 4s]`. |

```sql
WINDOW TUMBLING(ts, SIZE 1 SEC) INTO sink
```

```sql
WINDOW SLIDING(ts, SIZE 1 SEC, ADVANCE BY 100 MS) INTO sink
```

💡 A sliding window's timestamp field must be `UINT64`, with millisecond resolution.

### Time units

The `<unit>` in `SIZE` and `ADVANCE BY` is one of:

| Unit | Keyword |
|---|---|
| Millisecond | `MS` |
| Second | `SEC` |
| Minute | `MINUTE` |
| Hour | `HOUR` |
| Day | `DAY` |

### Window measures

| Measure | Description |
|---|---|
| Event time | Uses a timestamp field defined in the tuples themselves to assign them to windows. Supply the field as the first `WINDOW` argument, e.g. `WINDOW TUMBLING(ts, SIZE 1 SEC)`. |
| Ingestion time | Assigns tuples to windows based on when they were ingested into the system. Omit the timestamp field: `WINDOW TUMBLING(SIZE 1 MINUTE)`. |

💡 For binary windowed operators (joins), the event-time timestamp field must have the same name in both
input streams.

## Aggregation

**Syntax:** `SELECT <aggregate function>, ... FROM <source> [GROUP BY <field>, ...] <window> [HAVING <predicate>] INTO <sink>`

Aggregation is a stateful, unary operator that computes summary statistics over windows of data. See
[Aggregation functions](functions.md#aggregation-functions) for the full list of `SUM`, `AVG`, `MIN`,
`MAX`, `MEDIAN`, and `COUNT`.

- One or more aggregate functions appear in the `SELECT` list.
- `WINDOW` is required, see [Windows](#windows).
- `GROUP BY` is optional; when present, it produces one output row per distinct key combination per
  window.
- `HAVING` is optional; it filters the aggregated results, evaluated after windowing.

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
WINDOW TUMBLING(ts, SIZE 5 MINUTE) INTO sink
```

```sql
SELECT ticker, MAX(price) AS max_price, MIN(price) AS min_price
FROM stock_quotes
GROUP BY ticker
WINDOW TUMBLING(ts, SIZE 1 MINUTE)
HAVING MAX(price) > 100.0 AND COUNT(*) >= 10 INTO sink
```

## Join

**Syntax:** `SELECT ... FROM <left> [INNER] JOIN (<right>) ON <predicate> <window> INTO <sink>`

Join is a stateful, binary operator that combines tuples from two input streams within the same window
that satisfy the join predicate. The join's timestamp field currently must have the same name in both
input streams.

- `ON <predicate>` is required; an equality predicate over fields from both sides.
- `WINDOW` is required, see [Windows](#windows).

```sql
SELECT * FROM s INNER JOIN (SELECT * FROM t) ON sid = tid WINDOW TUMBLING(ts, SIZE 1 MINUTE) INTO sink
```

```sql
SELECT order_id, customer_id, amount
FROM orders
INNER JOIN (SELECT * FROM payments p) ON order_id = payments_order_id
WINDOW SLIDING(ts, SIZE 30 SEC, ADVANCE BY 5 SEC) INTO sink
```

---

## Model Inference

**Syntax:** `SELECT ... FROM MODEL_INFERENCE(<model_name>, <source|subquery>) INTO <sink>`

Model Inference runs a registered ML model on each input tuple and appends the model's output fields to
the result. It is a stateless operator that appears in the `FROM` clause instead of being called within a
projection or predicate. Register a model with `CREATE MODEL` before use, as shown below.

- `<model_name>` is the name a model was registered under via `CREATE MODEL`.
- The second argument is either a source/stream name, or a subquery producing the model's input fields.

```sql
CREATE MODEL iris ('/path/to/iris.onnx')
INPUT (p1 FLOAT32, p2 FLOAT32, p3 FLOAT32, p4 FLOAT32)
OUTPUT (setosa FLOAT32, versicolor FLOAT32, virginica FLOAT32);
```

- `INPUT` fields must match the model's input tensor shape and types; each field maps to one tensor
  element.
- `OUTPUT` fields must match the model's output tensor shape and types.
- Only `.onnx` model files are supported. Models are compiled to IREE bytecode at first use.
- Only `FLOAT32` tensor element types are currently supported.
- Model Inference requires the IREE runtime library and the IREE compiler tools (`iree-import-onnx`,
  `iree-compile`).

```sql
-- Direct stream input: each tuple's fields are fed to the model
SELECT * FROM MODEL_INFERENCE(iris, stream) INTO result;
```

The output schema contains all fields from the input stream followed by the model's output fields.

```sql
-- Decode base64-encoded image data before feeding it to a model
SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9
FROM MODEL_INFERENCE(mnist, (SELECT FROM_BASE64(pixels) AS pixels FROM stream))
INTO result;
```

When the model input is defined as `VARSIZED`, a single binary blob (e.g. raw tensor bytes) is passed
directly to the model runtime, useful for image data or pre-packed tensors.

```sql
-- Feed the output of one model into another
SELECT * FROM MODEL_INFERENCE(model_b, MODEL_INFERENCE(model_a, stream)) INTO result;
```
