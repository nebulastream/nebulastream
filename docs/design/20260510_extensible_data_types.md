# General Information
The PoC branch attached to this DD is not a feature branch in disguise.
Its sole purpose is to give you a concrete, interactive vision of what I am aiming for with a first version of extensible data types.
As a result there are at least four major simplifications that the PoC makes use of:
1. builds on top of current main and not on top of the schema inference branch
2. keeps simplistic datatype frontend/logical implementation
3. does not dive into a memorylayout-/bufferref-based implementation for custom/new datatypes, which is probably the clean way, but requires a significant refactor of bufferrefs
4. hardcodes parsing logic into new 'NestedJSON' parser

# The Problem
Adding a new logical type means editing core code at every switch site: parsers, schema printer, sinks, JSON/CSV formatters, Nautilus runtime, generator source, etc.
There is no way to register a new type from `nes-plugins/`, although sources, sinks, and input/output formatters already follow that pattern.

In particular, real-world ingest scenarios need *composite* types whose layout is fixed but bigger than a scalar, e.g. a thermal-camera reading `Image{timestamp, cameraId, ThermalFrame{pixels: UINT16[N]}}`.
Today the only way to model this is to flatten everything into top-level columns, which loses the type identity that makes domain-specific functions (`to_celsius`, `is_fever`, `to_rgb`) safe to register. Such flattened approach also quickly blows up the schema and does not really work with containers such as arrays and vectors.

- P1: `DataType` cannot be extended without modifying core code paths spread across ~20 files.
- P2: There is no first-class composite type — no way to represent a named struct of fields and refer to it by name in SQL or in physical functions.
- P3: There is no way to add SQL constructor syntax (`Image(...)`) for a new type without per-type parser changes. The casting/construction surface is hard-coded.
- P4: There is no vector- or array-like container type that is type-specific. VarSized is essentially a string/byte-vector.

# Goals
- G1: A new `DataType` can be added from `nes-plugins/` using the same registration pattern that sources, sinks, and formatters already use, addressing P1.
- G2: Support a composite `STRUCT` variant with a name and a field list that participates in schemas, parsers, formatters, and Nautilus values, addressing P2.
- G3: Provide a generic SQL constructor syntax `T(arg1, arg2, ...)` for any registered type, dispatched in the parser without per-type code, addressing P3.
- G4: Demonstrate G1–G3 end-to-end with example plugins that fit on top of the core without modifying it (`Image/ThermalFrame`, `Reading`).
- G5: Create a fixed-sized (is an array, but name contrasts 'varsized') datatype that is type-specific, i.e., represents an array of type T. 

# Non-Goals
- NG1: Refactoring `DataType` into a polymorphic class hierarchy or `std::variant`.
  `DataType` stays a flat struct with optional fields per variant; this is a deliberate PoC scope decision (smaller blast radius, follows the existing `FIXEDSIZED` precedent).
- NG2: Structural typing.
  Two structs with the same physical layout but different `structName` are distinct types and do not `join()`; this preserves the safety of name-keyed domain functions.
- NG3: Full coverage of every downstream switch site.
  Nautilus, codegen, serializer, and a handful of other switch sites currently throw `NotImplemented` for `STRUCT`; wiring those is follow-up work.
- NG4: Create a dedicated string datatype.
- NG5: Create a vector datatype, i.e., an 'unbounded' array of a specific type. 

# Alternatives
- A1: Polymorphic `DataType` hierarchy (each variant a subclass).
  Cleaner long term, but a much larger refactor that touches every consumer of `DataType`.
  Rejected because it is orthogonal to demonstrating extensibility and would block this PoC indefinitely. I mainly want to build on top of the schema inference branch and then add complexity to the 'frontend' of the datatypes if required.
- A2: Structural typing for composite types (compare by layout).
  Rejected because it makes domain-specific functions unsafe (an arbitrary `(uint64, uint32, uint16[N])` struct would match `Image`).

# Solution Background
- `FIXEDSIZED` was added on this same branch as a precedent for an "extensible-but-flat" variant that participates in formatters, parsers, and Nautilus (G5).
  `STRUCT` follows the same shape (extra fields on `DataType`, new Nautilus value type, switch-site handling) and reuses the same shared `JsonValueParser`.
- `DataTypeRegistry` and `DataTypeProvider` already existed for the registry pattern; this PoC adds the first composite plugins that actually use them.

# Proposed Solution
1. Extend `DataType` with two new variants and their per-variant fields:
   - `FIXEDSIZED` carries `elementType` + `count`.
   - `STRUCT` carries `structName` + `vector<pair<string, DataType>> fields`.
2. Add matching Nautilus values (`FixedSizedData`, `StructData`) with `VarVal` integration so physical functions can read and write composite fields.
3. Wire the new variants through the necessary switch sites: schema printer, sinks, JSON/CSV output formatters, JSON input formatter (via shared `JsonValueParser` in `nes-plugins/InputFormatters/Common/`), generator source.
4. Add three generic, type-driven logical/physical functions that replace the per-type pattern:
   - `ConstructStructLogicalFunction` / `ConstructStructPhysicalFunction`: for any registered `STRUCT`, build a value from N child expressions matching the field list.
   - `CastToTypeLogicalFunction`: for any registered non-`STRUCT` type, cast an expression to it.
   - `ConstantValueLogicalFunction` (existing): used when the argument list is a single literal.
5. Dispatch SQL `T(...)` calls in the parser's `exitFunctionCall` default case based on `DataTypeProvider::tryProvideDataType(funcName)`:
   - registered `STRUCT` → `ConstructStructLogicalFunction`
   - registered non-`STRUCT`, single literal arg → `ConstantValueLogicalFunction`
   - registered non-`STRUCT`, expression arg → `CastToTypeLogicalFunction`
   - otherwise → existing function-registry lookup
6. Domain functions stay per-struct (e.g. `to_celsius`, `is_fever`, `to_rgb`) and gate themselves on `dt.type == STRUCT && dt.structName == "Image"` inside the registered `LogicalFunctionRegistry` entry.

A new plugin therefore consists of:
- one `*DataType.cpp` that registers the type via `RegisterXxxDataType(DataTypeRegistryArguments)` in namespace `NES::DataTypeGeneratedRegistrar`;
- one `CMakeLists.txt` line `add_plugin_as_library(<Name> DataType nes-data-types-registry ...)`;
- zero or more domain-specific logical/physical functions, registered the same way as any other function.

No edits to core code paths are required beyond the registration entry point.

## Solution vs. Goals
- G1: New plugins live entirely under `nes-plugins/DataTypes/<Name>/` and register through the existing `DataTypeRegistry`.
- G2: `STRUCT` participates in `DataType`, schemas, formatters, parsers, and Nautilus values; composite values round-trip through the system end-to-end.
- G3: Any registered type works with the generic `T(...)` syntax — the parser does not need per-type code.
- G4: Two end-to-end plugins (`Image`/`ThermalFrame`, `Reading`) and a Python demo demonstrate the full path. A new `NestedJSON` input formatter plugin demonstrates ingesting nested input that maps onto composite fields.
- G5: Implemented working FIXEDSIZED datatype .


# Proof of Concept
## Branch
https://github.com/nebulastream/nebulastream/tree/extensible-data-types
## Code Examples
All snippets below are condensed from the systests under `nes-systests/` (`function/casting/Casting.test`,
`formatter/JSON/NestedJSON.test`, `formatter/JSON_OUTPUT/*`). They are grouped by the query-API capability
they exercise — each group is something that was impossible before this PoC.

### 1. Generic `T(...)` over an expression: cast any expression to a registered type (P3)
Before, an explicit type annotation could only wrap a literal (`UINT64(5)`); the parser had hard-coded keyword
cases. Now `exitFunctionCall` resolves the callee via `DataTypeProvider`, so `T(<expr>)` works for any registered
non-`STRUCT` type and lowers to `CastToTypeLogicalFunction` (or `ConstantValueLogicalFunction` for a lone literal).
```sql
CREATE LOGICAL SOURCE stream(id UINT32 NOT NULL, value UINT32 NOT NULL, timestamp UINT32 NOT NULL);
-- inputs: (1,1) (2,1) (3,1) (4,2) (5,19) (6,20) (7,21)

SELECT UINT64(id + value) AS u64id FROM stream INTO sinkId;        -- cast of an arithmetic expression
----
2
3
4
6
24
26
28
```

### 2. A plugin `DataType` as a schema column type (P1, P2)
`ThermalFrame` is a `STRUCT` registered entirely from `nes-plugins/DataTypes/Image/`; the ANTLR grammar now
accepts an identifier wherever a type name is expected, so the plugin type is usable in `CREATE LOGICAL SOURCE`
with no core change. The composite value round-trips: JSON in (via the `NestedJSON` parser) → inline `STRUCT`
layout in the tuple buffer → JSON out.
```sql
CREATE LOGICAL SOURCE thermalRecords(`timestamp` UINT64 NOT NULL, `camera_id` UINT32 NOT NULL, `frame` ThermalFrame NOT NULL);
CREATE PHYSICAL SOURCE FOR thermalRecords TYPE File SET('NestedJSON' AS `PARSER`.`TYPE`);
ATTACH INLINE
{"timestamp": 1714060800000, "camera_id": 1, "frame": {"pixels": [30100, 30180, ... , 30130]}}

SELECT * FROM thermalRecords INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"THERMALRECORDS$timestamp":1714060800000,"THERMALRECORDS$camera_id":1,"THERMALRECORDS$frame":{"pixels":[30100,30180, ... ,30130]}}
```

### 3. A fixed-sized array as a schema column type (P4, G5)
`T ARRAY[N]` is the new `FIXEDSIZED` variant — a type-specific array, distinct from `VARSIZED` (string/bytes).
Per-`elementType` dispatch covers floats, signed/unsigned ints, etc.
```sql
CREATE LOGICAL SOURCE mixedArrays(`id` UINT64 NOT NULL, `floats` FLOAT64 ARRAY[3] NOT NULL, `signed` INT16 ARRAY[2] NOT NULL);
CREATE PHYSICAL SOURCE FOR mixedArrays TYPE File SET('NestedJSON' AS `PARSER`.`TYPE`);
ATTACH INLINE
{"id": 1, "floats": [1.5, -2.25, 0.0], "signed": [-1, 32767]}
{"id": 2, "floats": [0.5, 0.0, -0.0], "signed": [-32768, 0]}

SELECT * FROM mixedArrays INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"MIXEDARRAYS$id":1,"MIXEDARRAYS$floats":[1.5,-2.25,0.0],"MIXEDARRAYS$signed":[-1,32767]}
{"MIXEDARRAYS$id":2,"MIXEDARRAYS$floats":[0.5,0.0,-0.0],"MIXEDARRAYS$signed":[-32768,0]}
```

### 4. Constructing a `STRUCT` value in SQL with `StructName(...)` (P2, P3)
The same generic `exitFunctionCall` dispatch routes a registered `STRUCT` name to `ConstructStructLogicalFunction`,
which expects N child expressions matching the field list. Arguments can be literals, columns, or arbitrary
expressions — and may be projected out of *different* (e.g. joined) streams. Note the constructed value is a
first-class column that flows through windows, joins and the JSON formatter as a nested object.
```sql
CREATE LOGICAL SOURCE thermometer(sensor_id UINT64 NOT NULL, celsius FLOAT64 NOT NULL, ts UINT64 NOT NULL);
CREATE LOGICAL SOURCE hygrometer(device_id UINT64 NOT NULL, humidity_pct FLOAT64 NOT NULL, battery_pct UINT8 NOT NULL, ts UINT64 NOT NULL);

-- from a literal and a per-row expression
SELECT Reading(celsius + FLOAT64(10.0), FLOAT64(60.5)) AS reading FROM thermometer INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"READING":{"temperature":33.5,"humidity":60.5}}
{"READING":{"temperature":34.0,"humidity":60.5}}
{"READING":{"temperature":35.5,"humidity":60.5}}

-- pull each struct field from a different side of a windowed join
SELECT Reading(celsius, humidity_pct) AS reading
FROM (SELECT * FROM thermometer) JOIN (SELECT * FROM hygrometer) ON sensor_id = device_id
  WINDOW TUMBLING (ts, size 1 sec) INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"READING":{"temperature":23.5,"humidity":65.0}}
{"READING":{"temperature":24.0,"humidity":70.0}}
{"READING":{"temperature":25.5,"humidity":55.0}}
```

### 5. Per-type operator and comparison semantics over a `STRUCT` (P2, NG2)
Because `STRUCT`s are nominally typed, `FunctionProvider` can dispatch `+`, `=`, `>`, … by struct name. The
`Reading` plugin registers pairwise field add; comparisons are decided likewise. An arbitrary `(FLOAT64, FLOAT64)`
struct with a different name would *not* match these — that is the point of NG2.
```sql
-- pairwise field add provided by the Reading plugin: temperature += 1, humidity += 5
SELECT Reading(celsius, FLOAT64(50.0)) + Reading(FLOAT64(1.0), FLOAT64(5.0)) AS combined FROM thermometer INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"COMBINED":{"temperature":24.5,"humidity":55.0}}
{"COMBINED":{"temperature":25.0,"humidity":55.0}}
{"COMBINED":{"temperature":26.5,"humidity":55.0}}

-- STRUCT-valued predicate in WHERE
SELECT Reading(celsius, FLOAT64(60.5)) AS reading FROM thermometer
  WHERE Reading(celsius, FLOAT64(60.5)) > Reading(FLOAT64(23.5), FLOAT64(60.5)) INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"READING":{"temperature":24.0,"humidity":60.5}}
{"READING":{"temperature":25.5,"humidity":60.5}}
```

### 6. Domain-specific functions over a plugin `STRUCT` (P2, G4)
`to_celsius`, `is_fever`, `to_rgb` are ordinary `LogicalFunctionRegistry` entries shipped by the Image plugin;
they gate on `dt.type == STRUCT && dt.structName == "ThermalFrame"`, take/return `STRUCT` and `FIXEDSIZED`
values, and are dispatched through SQL → logical/physical lowering → Nautilus like any other function. The
two-arg `to_rgb` takes a `VARSIZED` colormap name and returns an `RGBFrame` `STRUCT` of three planar `UINT8[N]`
channels — a struct whose fields are themselves fixed-sized arrays.
```sql
CREATE LOGICAL SOURCE thermalRecords(`timestamp` UINT64 NOT NULL, `frame` ThermalFrame NOT NULL);
-- frame.pixels are raw UINT16 sensor counts (16-px 4x4 demo frame)

SELECT `timestamp`, to_celsius(`frame`) AS celsius, is_fever(`frame`) AS fever FROM thermalRecords INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"THERMALRECORDS$timestamp":1,"CELSIUS":[26.85,26.85, ... ,26.85],"FEVER":false}
{"THERMALRECORDS$timestamp":2,"CELSIUS":[26.85,26.85,26.85,26.85,26.85,38.849998,26.85, ... ,26.85],"FEVER":true}

SELECT `timestamp`, to_rgb(`frame`, VARSIZED('iron')) AS rgb FROM thermalRecords INTO File('JSON' as `SINK`.OUTPUT_FORMAT);
----
{"THERMALRECORDS$timestamp":1,"RGB":{"r":[44, ... ],"g":[0, ... ],"b":[175, ... ]}}
{"THERMALRECORDS$timestamp":2,"RGB":{"r":[44,44,44,44,44,255,44, ... ],"g":[0,0,0,0,0,213,0, ... ],"b":[175,175,175,175,175,63,175, ... ]}}
```
The `nes-plugins/DataTypes/Image/demo/run_demo.py` script wires this end to end: it feeds a real thermal frame
in, runs `to_rgb`, and writes out the colorized image — all of the type-specific behaviour living in the plugin.


## Locations of Changes
- `nes-plugins/DataTypes/Image/`: registers `Image` and `ThermalFrame`, plus `to_celsius`, `is_fever`, `to_rgb`. Demo at `nes-plugins/DataTypes/Image/demo/run_demo.py` consumes a thermal image and emits a colorized RGB image.
- `nes-plugins/DataTypes/Reading/`: registers `Reading{celsius, humidity_pct}` and an addition function over `Reading`s, demonstrating a second independent struct with arithmetic semantics.
- `nes-plugins/InputFormatters/NestedJSONInputFormatter/`: separate plugin, demonstrates that ingesting nested JSON into composite columns is itself an opt-in plugin.
- Systests under `nes-systests/formatter/JSON_OUTPUT/` (`StructConstruction`, `StructAdd`, `StructWHERE`, `ThermalFrameStructJSON`, `ThermalFrameFunctions`, `ThermalFrameToRGB`, `FixedSizedArrayJSON`) exercise the end-to-end paths.

# Implementation Plan
- branch off from Leonhards schema inference PR
- fix hardcoded antlr parser types (allow identifier)
    - currently antlr only expects specific keywords, e.g., UINT8, which is an anti-pattern for extensible data types
- allow data type construction from literals (casting), e.g., UINT64(value + 2) as valPlusTwo
- introduce descriptor for input formatters (make invididual input formatters configurable)
- introduce shared/central input/output value-parser registries for input/output formatters
- introduce nested json parser (Nils PR)
- (optional: data type refactoring in front end)
- (optional: memory layout refactoring)
- introduce struct type (frontend+backend)
    - either like PoC or via MemoryLayout/BufferRef
- introduce fixed-sized with type
    - either like PoC or via MemoryLayout/BufferRef
- (potentially) introduce Image type with Mono16 and functions for SIGMOD demo use case

# Summary
The PoC adds two extensible variants (`FIXEDSIZED`, `STRUCT`) to `DataType`, a generic SQL `T(...)` constructor pipeline, and three plugins that demonstrate the full path from registration to physical execution.
P1 is addressed by routing all type-specific logic through the existing registry pattern and through generic `Construct`/`Cast` functions instead of per-type switch cases.
P2 is addressed by `STRUCT` with nominal typing.
P3 is addressed by the generic `exitFunctionCall` parser dispatch.

# Open Questions
- OQ1: Do we want to evolve `DataType` itself to a polymorphic representation (NG1)? Or do we wait until there is a clear need? In my opinion, such a refactor would warrant its own DD that then also covers a full-blown type system.
- OQ2: Do we want to refactor the memory layout / buffer ref before implementing fixed-sized and struct as variants? While this is the better solution, a memory layout / buffer ref refactor that can handle compound/struct data types is a major undertaking. Additionally, I'd rather work on that refactor with already existing compound/struct data types that can demonstrate the effectiveness. 
