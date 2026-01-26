# JSONInputFormatter

The JSONInputFormatter parses JSON-encoded data from sources (files, TCP streams, etc.) into NebulaStream's internal TupleBuffer representation. It is built on top of [SIMDJSON](https://github.com/simdjson/simdjson)'s `ondemand` parser for high-performance, lazy evaluation of JSON documents.

## Supported Data Types

All NebulaStream fixed-size and variable-sized types are supported:

| Type | JSON representation |
|------|---------------------|
| `INT8`, `INT16`, `INT32`, `INT64` | Integer literal |
| `UINT8`, `UINT16`, `UINT32`, `UINT64` | Integer literal |
| `FLOAT32`, `FLOAT64` | Floating-point literal |
| `BOOLEAN` | `true` / `false` |
| `CHAR` | Single-character string |
| `VARSIZED` | String |

Every type can be declared nullable (omit `NOT NULL`) or non-nullable (`NOT NULL`).

## Basic Usage

Each JSON tuple is a single JSON object. Fields are matched **case-insensitively** by name. Field order in the JSON object does not matter.

```sql
CREATE LOGICAL SOURCE mySource(key UINT64 NOT NULL, value UINT64 NOT NULL, name VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR mySource TYPE File SET("JSON" AS PARSER.`TYPE`);
```

```json
{"KEY": 1, "VALUE": 2, "NAME": "john"}
```

Extra fields in the JSON that are not in the schema are silently ignored. Whitespace around keys and values is tolerated.

## Nested Field Access

Nested JSON objects are accessed by encoding the path in the schema field name using `/` as the separator.

### Syntax

Use backtick-quoted field names with `/` separating each nesting level:

```sql
CREATE LOGICAL SOURCE nested(
    key UINT64,
    `EXTRA_KEY/NAME` VARSIZED,
    `milk/cycles/left` FLOAT64
);
```

This maps to the following JSON structure:

```json
{
    "KEY": 1,
    "EXTRA_KEY": { "NAME": "max" },
    "milk": { "cycles": { "left": 0.5 } }
}
```

### Nesting Depth

There is no practical limit on nesting depth. Fields at 1, 2, 3, or more levels deep are all supported:

```sql
`info/city`              -- 1 level:  {"info": {"city": "berlin"}}
`milk/cycles/left`       -- 2 levels: {"milk": {"cycles": {"left": 0}}}
`org/team/lead`          -- 2 levels: {"org": {"team": {"lead": "carol"}}}
```

### Multiple Fields from Same Parent

Multiple fields can be extracted from the same nested parent or from sibling parents:

```sql
CREATE LOGICAL SOURCE sensors(
    `source/name` VARSIZED NOT NULL,
    `source/location` VARSIZED NOT NULL,
    `target/name` VARSIZED NOT NULL,
    `target/location` VARSIZED NOT NULL
);
```

```json
{"source": {"name": "sensor-1", "location": "room-a"}, "target": {"name": "actuator-1", "location": "room-b"}}
```

## Null Handling

Nullable fields handle three cases gracefully:

| Scenario | Result |
|----------|--------|
| Explicit `null` value: `{"KEY": null}` | `NULL` |
| Missing field: `{"OTHER_KEY": 1}` | `NULL` |
| Missing parent object (nested): `{"id": 1}` when reading `info/city` | `NULL` |

For **non-nullable** fields (`NOT NULL`), any of these cases produces an error (`FieldNotFound`, error code 2004).

## What Is NOT Supported

### JSON Arrays

Arrays are not supported. Array fields in the JSON are ignored/skipped. There is no syntax for array indexing (`field[0]`) or array flattening/unnesting.

### Type Mismatches

The JSON value type must match the schema type exactly. Mismatches produce a `FormattingError` (error code 4003):

- `{"KEY": 1.0}` when the schema expects `UINT64` -- float cannot be read as integer
- `{"KEY": {"nested": 1}}` when the schema expects a scalar type -- object cannot be read as scalar

### Buffer Size Limit

SIMDJSON's ondemand parser has a default batch size of **100,000 bytes**. If a single TupleBuffer contains more JSON data than this limit, parsing fails with a `CannotFormatSourceData` error (error code 4000). This is a per-buffer constraint, not per-field.

## Error Reference

| Error Code | Name | Cause |
|------------|------|-------|
| 2004 | `FieldNotFound` | A `NOT NULL` field is missing from the JSON or has an explicit `null` value |
| 4000 | `CannotFormatSourceData` | JSON buffer exceeds the SIMDJSON batch size limit (~100 KB) |
| 4003 | `FormattingError` | JSON value type does not match the expected schema type |

## Implementation Notes

- Uses SIMDJSON's `ondemand` parser in single-threaded mode for lazy, streaming evaluation.
- Field lookup uses `find_field_unordered()` rather than `at_pointer()` to preserve the internal string buffer. This ensures that `string_view` references to variable-sized data remain valid across multiple field accesses within the same tuple.
- Nested paths are navigated step-by-step (splitting on `/` and calling `find_field_unordered()` at each level) rather than using JSON Pointer, again to preserve buffer lifetime.
