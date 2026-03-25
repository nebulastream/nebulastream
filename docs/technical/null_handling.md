# NULL Handling in NebulaStream

NebulaStream supports nullable fields with SQL-compatible three-valued logic.
A NULL value represents a missing or unknown value.
Fields are **nullable by default** — to make a field non-nullable, it must be explicitly annotated with `NOT NULL` in the schema definition.
We follow the rational of SQL 2023.

```sql
CREATE LOGICAL SOURCE sensor(
    ts UINT64 NOT NULL,   -- non-nullable
    temperature INT32,    -- nullable (default)
    location VARSIZED     -- nullable (default)
);
```

## Input Parsing

**CSV**: Only empty fields are interpreted as NULL for nullable columns.

```csv
1,,           -- ts=1, temperature=NULL, location=NULL
2,25,berlin   -- ts=2, temperature=25, location="berlin"
3,30,         -- ts=3, temperature=30, location=NULL
```

**JSON**: `null` and missing keys produce NULL for nullable columns.

```json
{"ts": 1, "temperature": null, "location": "berlin"}   -- temperature=NULL
{"ts": 2}                                              -- temperature=NULL, location=NULL
```

## Output Formatting

NULL values are rendered as the string `NULL` in both CSV and JSON output.

```csv
1,NULL,berlin
2,25,NULL
```
```json
{"ts": 1, "temperature": "NULL", "location" :  "berlin"}
{"ts": 2, "temperature": "25", "location" :  "NULL"}
```

---

## Operations

### Filter (WHERE)

A filter predicate that evaluates to NULL is treated as **FALSE** — the row is excluded.

```sql
-- Source data:
-- ts=1, value=NULL
-- ts=2, value=1
-- ts=3, value=NULL
-- ts=4, value=2

SELECT * FROM stream WHERE value == INT32(1);
-- Result: only ts=2 (NULL == 1 evaluates to NULL, treated as false)

SELECT * FROM stream WHERE value < INT32(1);
-- Result: empty (NULL < 1 evaluates to NULL, treated as false)
```

Use `ISNULL()` to explicitly filter on null values:

```sql
SELECT * FROM stream WHERE ISNULL(value);
-- Result: ts=1, ts=3 (only rows where value IS NULL)

SELECT * FROM stream WHERE NOT ISNULL(value);
-- Result: ts=2, ts=4 (only rows where value is NOT NULL)
```

### Projection (SELECT)

Projection passes NULL values as is.

```sql
-- Source: (1, NULL, NULL), (2, 100, "hello"), (3, NULL, NULL)
SELECT value, text FROM stream;
-- Output:
-- NULL, NULL
-- 100, hello
-- NULL, NULL
```

### Map (Expressions)

Arithmetic and string expressions propagate NULL: if **any operand is NULL**, the result is **NULL**.

```sql
-- Source: id=0 value1=0 value2=4 | id=4 value1=NULL value2=NULL | id=5 value1=7 value2=NULL

SELECT id, value1 + INT32(1) AS result FROM stream;
-- id=0: 1
-- id=4: NULL     (NULL + 1 = NULL)
-- id=5: 8

SELECT id, value1 + value2 AS result FROM stream;
-- id=0: 4        (0 + 4)
-- id=4: NULL     (NULL + NULL = NULL)
-- id=5: NULL     (7 + NULL = NULL)
```

String concatenation follows the same rule:

```sql
SELECT id, CONCAT(text, VARSIZED("_x")) AS result FROM stream;
-- id=1: "alpha_x"
-- id=3: NULL      (NULL concat "_x" = NULL)
```

### Comparisons

All comparisons involving NULL yield **NULL** — including equality of a value with itself.

```sql
-- Source: id=1 value1=1 value2=2 | id=2 value1=NULL value2=3 | id=5 value1=NULL value2=NULL

SELECT id,
       value1 == INT32(1) AS v1_eq_1,
       value1 == value2   AS v1_eq_v2,
       value1 == value1   AS v1_self_eq,
       value2 == value2   AS v2_self_eq
FROM stream;
```

| id | v1_eq_1 | v1_eq_v2 | v1_self_eq | v2_self_eq |
|----|---------|----------|------------|------------|
| 1  | 1       | 0        | 1          | 1          |
| 2  | NULL    | NULL     | NULL       | 1          |
| 5  | NULL    | NULL     | NULL       | NULL       |

Key takeaway: **`NULL == NULL` is NULL, not TRUE**. Use `ISNULL()` to check for NULL.

The same applies to `<`, `>`, `<=`, `>=`, and `!=`:

```sql
-- Source: id=1 a=1 b=2 | id=2 a=NULL b=2 | id=3 a=3 b=NULL

SELECT id, a < INT32(3) AS a_lt_3, b > INT32(0) AS b_gt_0, a < b AS a_lt_b FROM stream;
```

| id | a_lt_3 | b_gt_0 | a_lt_b |
|----|--------|--------|--------|
| 1  | 1      | 1      | 1      |
| 2  | NULL   | 1      | NULL   |
| 3  | 0      | NULL   | NULL   |

### Logical Operators (AND, OR, NOT)

NebulaStream implements SQL three-valued logic. The key short-circuit rules are:

- **`FALSE AND NULL ==> FALSE`** (false dominates AND)
- **`TRUE OR NULL ==> TRUE`** (true dominates OR)

Full truth tables:

**AND**

| Left \ Right | TRUE  | FALSE | NULL  |
|-------------|-------|-------|-------|
| **TRUE**    | TRUE  | FALSE | NULL  |
| **FALSE**   | FALSE | FALSE | FALSE |
| **NULL**    | NULL  | FALSE | NULL  |

**OR**

| Left \ Right | TRUE | FALSE | NULL |
|-------------|------|-------|------|
| **TRUE**    | TRUE | TRUE  | TRUE |
| **FALSE**   | FALSE| FALSE | NULL |
| **NULL**    | TRUE | NULL  | NULL |

**NOT**

| Input | Result |
|-------|--------|
| TRUE  | FALSE  |
| FALSE | TRUE   |
| NULL  | NULL   |

Example with concrete data:

```sql
-- Source: id=5 flag=NULL flag2=TRUE | id=8 flag=FALSE flag2=NULL | id=9 flag=NULL flag2=FALSE

SELECT id, flag AND flag2, flag OR flag2 FROM stream;
```

| id | AND  | OR   |
|----|------|------|
| 5  | NULL | TRUE |
| 8  | FALSE| NULL |
| 9  | FALSE| NULL |

When used in a WHERE clause, these rules compose with the "NULL is treated as FALSE" rule:

```sql
SELECT id FROM stream WHERE flag OR flag2;
-- id=5 included (NULL OR TRUE = TRUE)
-- id=8 excluded (FALSE OR NULL = NULL, treated as false)
-- id=9 excluded (NULL OR FALSE = NULL, treated as false)

SELECT id FROM stream WHERE flag AND flag2;
-- id=5 excluded (NULL AND TRUE = NULL, treated as false)
-- id=8 excluded (FALSE AND NULL = FALSE)
```

### Division and Modulo

Division and modulo follow NULL propagation, with an additional safety check:

- If either operand is NULL, the result is **NULL**.
- Dividing by a non-null zero throws an `ArithmeticalError`.
- If the divisor is NULL (and therefore has an underlying value of 0), a safe denominator of 1 is used internally to avoid a crash. The result is still NULL due to the null operand.

### Join (INNER JOIN)

**NULL join keys are excluded**: any tuple with a NULL value in a join key field is **never inserted into the hash table**, so it can never match. This follows SQL semantics where `NULL != NULL`.

```sql
-- Left:  id=0 value=NULL ts=0   | id=0 value=5 ts=200  | id=0 value=7 ts=1000
-- Right: id=0 value2=0 ts=100   | id=0 value2=7 ts=1300

SELECT * FROM left INNER JOIN right ON (value = value2) WINDOW TUMBLING(ts, size 1 sec);
-- Result: only (value=7) matches (value2=7)
-- value=NULL never matches anything, not even another NULL
```

**Non-key NULL values pass through** unchanged when the join matches on other (non-null) keys:

```sql
-- Left:  id=0 value=NULL ts=0  | id=1 value=3 ts=700
-- Right: id=0 value2=0 ts=100  | id=1 value2=6 ts=900

SELECT * FROM left INNER JOIN right ON (id = id2) WINDOW TUMBLING(ts, size 1 sec);
-- Result includes: (id=0, value=NULL, ..., id2=0, value2=0, ...)
-- NULL in non-key field 'value' is preserved in the output
```

### Aggregation Functions

#### SUM, AVG, MIN, MAX, MEDIAN

If **any input value is NULL**, the aggregation result is **NULL**.

```sql
-- Source: ts=0 value=0 | ts=20 value=3 | ts=50 value=1 | ts=100 value=2 | ts=150 value=NULL

SELECT SUM(value), MAX(value), MIN(value), AVG(value), MEDIAN(value)
FROM stream WINDOW TUMBLING(ts, size 100 ms);
```

| Window      | SUM  | MAX  | MIN  | AVG  | MEDIAN |
|-------------|------|------|------|------|--------|
| [0, 100)    | 4    | 3    | 0    | 1.33 | 1      |
| [100, 200)  | NULL | NULL | NULL | NULL | NULL   |

Window [0, 100) contains only non-null values (0, 3, 1), so aggregations work normally.
Window [100, 200) contains value=2 and value=NULL — the presence of NULL makes the result NULL.

To aggregate only over non-null values, pre-filter with `NOT ISNULL()`:

```sql
SELECT SUM(value) FROM stream WHERE NOT ISNULL(value) WINDOW TUMBLING(ts, size 100 ms);
```

#### COUNT

`COUNT(field)` **skips NULL values** and always returns a **NOT NULL UINT64**.

```sql
-- Source: ts=0 id=NULL value=NULL | ts=10 id=NULL value=1 | ts=20 id=1 value=2 | ts=30 id=1 value=3

SELECT id, COUNT(value) AS valueCount, COUNT(ts) AS rowCount
FROM stream GROUP BY id WINDOW TUMBLING(ts, size 1 sec);
```

| id   | valueCount | rowCount |
|------|-----------|----------|
| NULL | 1         | 2        |
| 1    | 2         | 2        |

- `COUNT(value)` for `id=NULL`: only 1 of the 2 values is non-null, so count is 1.
- `COUNT(ts)` counts all rows because `ts` is `NOT NULL`.

#### GROUP BY with NULL Keys

NULL keys form **their own group** — all rows with a NULL key are aggregated together.

```sql
-- Source: (ts=0, id=NULL, value=NULL), (ts=10, id=NULL, value=1), (ts=20, id=1, value=2), (ts=40, id=3, value=NULL)

SELECT id, SUM(value), COUNT(value), COUNT(ts)
FROM stream GROUP BY id WINDOW TUMBLING(ts, size 1 sec);
```

| id   | SUM  | COUNT(value) | COUNT(ts) |
|------|------|-------------|-----------|
| NULL | NULL | 1           | 2         |
| 1    | 5    | 2           | 2         |
| 3    | NULL | 0           | 2         |

### ISNULL Function

`ISNULL(expr)` returns a **non-nullable BOOLEAN** indicating whether the expression is NULL.

```sql
SELECT ISNULL(value) FROM stream;              -- true if value is NULL
SELECT NOT ISNULL(value) FROM stream;          -- true if value is NOT NULL
SELECT ISNULL(value1 == INT32(1)) FROM stream; -- true if the comparison result is NULL
SELECT ISNULL(CONCAT(a, b)) FROM stream;       -- true if the concat result is NULL
```

`ISNULL()` is the **only way to reliably test for NULL**, since `value == NULL` would itself evaluate to NULL.

---

## Summary

| Operation | NULL Behavior | Result Nullable? |
|-----------|--------------|-----------------|
| Arithmetic (`+`, `-`, `*`) | Either operand NULL → NULL | Yes, if any operand nullable |
| Division (`/`), Modulo (`%`) | Either operand NULL → NULL | Yes, if any operand nullable |
| Comparison (`==`, `<`, `>`, ...) | Either operand NULL → NULL | Yes, if any operand nullable |
| `AND` | FALSE AND NULL = FALSE; else NULL | Yes, if any operand nullable |
| `OR` | TRUE OR NULL = TRUE; else NULL | Yes, if any operand nullable |
| `NOT` | NULL → NULL | Yes, if operand nullable |
| `CONCAT` | Either operand NULL → NULL | Yes, if any operand nullable |
| `ISNULL(x)` | Returns TRUE/FALSE | **Always NOT NULL** |
| `SUM`, `AVG`, `MIN`, `MAX`, `MEDIAN` | Any NULL input → NULL result | Yes, if input nullable |
| `COUNT(field)` | Skips NULLs | **Always NOT NULL** |
| Filter (WHERE) | NULL predicate → row excluded | N/A |
| Projection | Pass-through | Preserves nullability |
| Join key | NULL key → row excluded from join | N/A |
| GROUP BY key | NULL keys → own group | Preserves nullability |

### Implementation Note

Internally, each nullable field occupies one extra byte in the tuple buffer to store the null flag.
The `DataType::getSizeInBytesWithNull()` method returns the type's size plus 1 byte for nullable fields.
Nullability is propagated at the logical level during type inference: if any child expression is nullable, the parent expression's result type is also nullable.
The exception is `ISNULL()` and `COUNT()`, which always produce non-nullable results.
