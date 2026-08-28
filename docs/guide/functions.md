# Functions

This page is the reference for every scalar and aggregate function NebulaStream exposes in SQL. Functions
can be applied to [fields](sources-and-sinks.md#logical-sources) within the `SELECT` list, `WHERE`
clause, or aggregation. See [Operators](operators.md) for the operators these clauses belong to.

A function is either unary (one argument) or binary (two arguments). Passing the wrong number of
arguments throws `InvalidQuerySyntax` during query submission. A function name is matched
case-insensitively, so `Abs(x)`, `ABS(x)`, and `abs(x)` are equivalent.

## Field access, constants, and casts

| Signature | Description | Example |
|---|---|---|
| `<field>` | References a record attribute by name. Every bare identifier in an expression is this. | `SELECT x FROM s INTO sink` |
| `<literal>` | An inline literal value (number, string, bool). | `SELECT 42 FROM s INTO sink` |
| `<expr> AS <alias>` | Binds the result of `<expr>` to a new field name in the output schema. | `SELECT x AS x1 FROM s INTO sink` |
| `CASTTOTYPE(x AS <type>)` | Converts `x` to `<type>` using standard SQL cast syntax. Takes exactly one argument (`x`); the target type is a type annotation, not a second function argument. See [Casting rules](#casting-rules). | `SELECT CASTTOTYPE(x AS FLOAT64) FROM s INTO sink` |
| `CASTTOUNIXTS(x)` | Parses a string field as a timestamp and converts it to a Unix timestamp. | `SELECT CASTTOUNIXTS(ts) FROM s INTO sink` |
| `CASTFROMUNIXTS(x)` | Converts a Unix timestamp field to its string representation. | `SELECT CASTFROMUNIXTS(ts) FROM s INTO sink` |

### Casting rules

`CASTTOTYPE` converts a field from one type to another:

- For numeric-to-numeric conversions (e.g. `INT32` to `FLOAT64`), a direct type cast is performed.
- When casting from `VARSIZED` (string) to a numeric type, digits (`0-9`), `.`, `+`, `-`, `e`, and `E`
  (scientific notation) are kept and everything else is stripped before parsing. For example,
  `"1 234.56"` becomes `1234.56` and `"12?3"` becomes `123`.

## Arithmetic functions

| Signature | Description | Example |
|---|---|---|
| `x + y` | Adds two numeric fields. | `SELECT x + 10 FROM s INTO sink` |
| `x - y` | Subtracts `y` from `x`. | `SELECT x - y FROM s INTO sink` |
| `x * y` | Multiplies two numeric fields. | `SELECT x * y FROM s INTO sink` |
| `x / y` | Divides `x` by `y`. | `SELECT x / y FROM s INTO sink` |
| `x % y` | Remainder of `x / y`. | `SELECT x % y FROM s INTO sink` |
| `ABS(x)` | Absolute value of `x`. Takes exactly one argument. | `SELECT ABS(x) FROM s INTO sink` |
| `CEIL(x)` | Rounds `x` up to the nearest integer. Takes exactly one argument. | `SELECT CEIL(x) FROM s INTO sink` |
| `FLOOR(x)` | Rounds `x` down to the nearest integer. Takes exactly one argument. | `SELECT FLOOR(x) FROM s INTO sink` |
| `ROUND(x)` | Rounds `x` to the nearest integer. Takes exactly one argument. | `SELECT ROUND(x) FROM s INTO sink` |
| `EXP(x)` | Computes `e^x`. Takes exactly one argument. | `SELECT EXP(x) FROM s INTO sink` |
| `POW(x, y)` | Computes `x^y`. Takes exactly two arguments. | `SELECT POW(x, 2) FROM s INTO sink` |
| `SQRT(x)` | Square root of `x`. Takes exactly one argument. | `SELECT SQRT(x) FROM s INTO sink` |

## Boolean and comparison functions

| Signature | Description | Example |
|---|---|---|
| `a AND b` | Logical AND. | `SELECT * FROM s WHERE a AND b INTO sink` |
| `a OR b` | Logical OR. | `SELECT * FROM s WHERE a OR b INTO sink` |
| `NOT a` | Logical negation. | `SELECT * FROM s WHERE NOT a INTO sink` |
| `a == b` | Equality comparison. | `SELECT * FROM s WHERE a == 42 INTO sink` |
| `a != b` | Inequality comparison. | `SELECT * FROM s WHERE a != 42 INTO sink` |
| `a > b` | Greater-than comparison. | `SELECT * FROM s WHERE a > b INTO sink` |
| `a >= b` | Greater-than-or-equal comparison. | `SELECT * FROM s WHERE a >= b INTO sink` |
| `a < b` | Less-than comparison. | `SELECT * FROM s WHERE a < b INTO sink` |
| `a <= b` | Less-than-or-equal comparison. | `SELECT * FROM s WHERE a <= b INTO sink` |
| `a BETWEEN lo AND hi` | Inclusive range check. Supports `NOT BETWEEN`. | `SELECT * FROM t WHERE amount BETWEEN 100.0 AND 1000.0 INTO sink` |
| `a IN (v1, v2, ...)` | Membership in an explicit, non-empty value list. Supports `NOT IN`. | `SELECT * FROM s WHERE status IN ('queued', 'running') INTO sink` |
| `a IS NULL` | Tests whether `a` is null. Supports `IS NOT NULL`. | `SELECT * FROM s WHERE optional_value IS NULL INTO sink` |
| `IsNull(a)` | Function-call spelling of `IS NULL`, usable in a projection (not just `WHERE`). | `SELECT IsNull(optional_value) AS is_missing FROM s INTO sink` |
| `a IS NaN` | Tests whether a numeric value is floating-point *not a number*. Supports `IS NOT NaN`. The operand must be numeric; integers are accepted but are never NaN. | `SELECT * FROM measurements WHERE reading IS NaN INTO sink` |
| `ISNAN(x)` | Function-call spelling of `IS NaN`, usable in a projection. | `SELECT ISNAN(reading) AS invalid FROM measurements INTO sink` |

<!-- `IS NaN` follows SQL three-valued logic, see [null handling](../technical/null_handling.md) for details. -->


## String and encoding functions

| Signature | Description | Example |
|---|---|---|
| `CONCAT(a, b)` | Concatenates two `VARSIZED` (string) fields. | `SELECT CONCAT(text1, text2) FROM s INTO sink` |
| `CHAR_LENGTH(x)` | Number of characters in a `VARSIZED` field. | `SELECT CHAR_LENGTH(x) FROM s INTO sink` |
| `OCTET_LENGTH(x)` | Number of bytes in a `VARSIZED` field. | `SELECT OCTET_LENGTH(x) FROM s INTO sink` |
| `TO_BASE64(x)` | Encodes raw binary/`VARSIZED` data to base64 text, using OpenSSL's EVP base64 implementation. | `SELECT TO_BASE64(data) FROM s INTO sink` |
| `FROM_BASE64(x)` | Decodes base64 text back to raw binary/`VARSIZED` data. | `SELECT FROM_BASE64(encoded) FROM s INTO sink` |

`TO_BASE64` and `FROM_BASE64` are particularly useful for passing binary tensor data through SQL queries
into [Model Inference](operators.md#model-inference).

## Timestamp component functions

Extracting a component out of a timestamp is done with one of three dedicated functions.

| Signature | Description | Example |
|---|---|---|
| `DAY_OF(ts)` | Day-of-month component of a timestamp field. | `SELECT DAY_OF(ts) FROM s INTO sink` |
| `MONTH_OF(ts)` | Month component of a timestamp field. | `SELECT MONTH_OF(ts) FROM s INTO sink` |
| `YEAR_OF(ts)` | Year component of a timestamp field. | `SELECT YEAR_OF(ts) FROM s INTO sink` |

## Aggregation functions

Aggregation functions are only valid inside a windowed query, see
[Aggregation](operators.md#aggregation) for `WINDOW`/`GROUP BY`/`HAVING` syntax. Each one takes a single
field argument, except `COUNT`, which also accepts `*`.

| Signature | Description | Example |
|---|---|---|
| `SUM(x)` | Sum of `x` over the window. | `SELECT SUM(x) FROM s WINDOW TUMBLING(ts, SIZE 30 SEC) INTO sink` |
| `AVG(x)` | Arithmetic mean of `x` over the window. | `SELECT AVG(x) FROM s WINDOW SLIDING(ts, SIZE 1 MINUTE, ADVANCE BY 15 SEC) INTO sink` |
| `MIN(x)` | Minimum value of `x` over the window. | `SELECT MIN(price) FROM bid GROUP BY ticker WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 1 SEC) INTO sink` |
| `MAX(x)` | Maximum value of `x` over the window. | `SELECT MAX(price) FROM bid GROUP BY ticker WINDOW SLIDING(ts, SIZE 10 SEC, ADVANCE BY 1 SEC) INTO sink` |
| `MEDIAN(x)` | Median value of `x` over the window. | `SELECT MEDIAN(oxygen_level) FROM health_sensor WINDOW TUMBLING(ts, SIZE 100 MS) INTO sink` |
| `COUNT(x)` / `COUNT(*)` | Number of tuples in the window. `COUNT(*)` counts all tuples including those with a null `x`; `COUNT(x)` counts only non-null `x` values. | `SELECT COUNT(*) AS event_count FROM api_requests WINDOW TUMBLING(ts, SIZE 5 MINUTE) INTO sink` |
