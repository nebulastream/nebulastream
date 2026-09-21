# General Information
The PoC branch attached to this DD is not a feature branch in disguise.
Its sole purpose is to give you a concrete, interactive vision of what we are aiming for with a first version of extensible data types.
As a result there are at least three major simplifications that the PoC makes use of:
1. keeps simplistic datatype frontend/logical implementation
2. does not dive into a memorylayout-/bufferref-based implementation for custom/new datatypes, which is probably the clean way, but requires a significant refactor of bufferrefs
3. ignores nullability of struct fields and array / vector elements

Furthermore, the PoC builds on top of the value SerDe implementations introduced on the parser-registry branch, which enables the addition of SerDes for new datatypes without modifying other formatter files.

# The Problem
Adding a new logical type means editing core code at every switch site: parsers, schema printer, sinks, JSON/CSV formatters, Nautilus runtime, generator source, etc.
There is no way to register a new type from `nes-plugins/`, although sources, sinks, and input/output formatters already follow that pattern.

In particular, real-world ingest scenarios need *composite* types and / or array types, whose layout is fixed but bigger than a scalar, e.g. a thermal-camera reading `Image{timestamp, cameraId, ThermalFrame{pixels: UINT16[N]}}` or a trajectory `Trajectory{points: Point{lon: FLOAT64, lat: FLOAT64, ts: INT64}[N]}`
Today the only way to model this is to flatten everything into top-level columns, which loses the type identity that makes domain-specific functions (`to_celsius`, `is_fever`, `to_rgb`) safe to register. Such flattened approach also quickly blows up the schema and does not really work with containers such as arrays and vectors.
Many formats like JSON, XML, and Avro support composite and array values, so we are currently unable to fully support these formats.

- P1: `DataType` cannot be extended without modifying core code paths spread across ~20 files.
- P2: There is no first-class composite type — no way to represent a named struct of fields and refer to it by name in SQL or in physical functions.
- P3: There is no way to add SQL constructor syntax (`Image(...)`) for a new type without per-type parser changes. The casting/construction surface is hard-coded.
- P4: There is no vector- or array-like container type that is type-specific. VarSized is essentially a string/byte-vector, but does not support indexing.

# Goals
- G1: A new `DataType` can be added from `nes-plugins/` using the same registration pattern that sources, sinks, and formatters already use, addressing P1.
- G2: Support a composite `STRUCT` variant with a name and a field list that participates in schemas, parsers, formatters, and Nautilus values, addressing P2.
- G3: Provide a generic SQL constructor syntax `T(arg1, arg2, ...)` for any registered type, dispatched in the parser without per-type code, addressing P3.
- G4: Support the definition of default SerDes for each individual DataType plugin (in contrast to only offering default SerDes for the "Struct" type), addressing P1.
- G5: Demonstrate G1–G4 end-to-end with example plugins that fit on top of the core without modifying it (`Image/ThermalFrame`, `Reading`, `(Moving)Point`, `MovingPolygon`, `Timestamp`).
- G6: Create a fixed-sized (is an array, but name contrasts 'varsized') datatype that is type-specific, i.e., represents an array of type T.
- G7: Create a vector datatype, which functions similar to the fixed-sized / array type but does not declare the number of elements per value in the schema definition.

# Non-Goals
- NG1: Refactoring `DataType` into a polymorphic class hierarchy or `std::variant`.
  `DataType` stays a flat struct with optional members per variant; this is a deliberate PoC scope decision (smaller blast radius, follows the existing `FIXEDSIZED` precedent).
- NG2: Structural typing.
  Two structs with the same physical layout but different `structName` are distinct types and do not `join()`; this preserves the safety of name-keyed domain functions.
- NG3: Full coverage of every downstream switch site.
  Nautilus, codegen, serializer, and a handful of other switch sites currently throw `NotImplemented` for `STRUCT`; wiring those is follow-up work.
- NG4: Create a dedicated string datatype. Varsized could become the dedicated string datatype after we introduced arrays and vectors.
- NG5: Dynamic resizing of vector values. In this PoC, the number of elements of a vector value is set as soon as the value arrives into the system.
- NG6: Nesting of variable-sized types. A vector is not allowed to contain any values that are variable-sized as well. We avoid storing child buffer addresses within child buffers via this constraint.

# Alternatives
- A1: Polymorphic `DataType` hierarchy (each variant a subclass).
  Cleaner long term, but a much larger refactor that touches every consumer of `DataType`.
  Rejected because it is orthogonal to demonstrating extensibility and would block this PoC indefinitely. I mainly want to build on top of the schema inference branch and then add complexity to the 'frontend' of the datatypes if required.
- A2: Structural typing for composite types (compare by layout).
  Rejected because it makes domain-specific functions unsafe (an arbitrary `(uint64, uint32, uint16[N])` struct would match `Image`).
<!Comment: Mention Lukas' solution>

# Solution Background
- `FIXEDSIZED` was added on this same branch as a precedent for an "extensible-but-flat" variant that participates in formatters, parsers, and Nautilus (G5).
  `STRUCT` follows the same shape (extra fields on `DataType`, new Nautilus value type, switch-site handling) and reuses the same shared `JsonValueParser`.
- `DataTypeRegistry` and `DataTypeProvider` already existed for the registry pattern; this PoC adds the first composite plugins that actually use them.
- We build on top of the `parser-registry` branch, which provides registries for `value SerDes` and allows to define default (de)serialization functions for each datatype.
  In this PoC, we will additionally allow the definition of default (de)serialization methods per datatype plugin.

# Proposed Solution
1. Extend `DataType` with three new variants and their per-variant fields:
   - `FIXEDSIZED` (Array) carries `elementType` + `count`.
   - `VECTOR` carries `elementType`
   - `STRUCT` carries `structName` + `vector<pair<string, DataType>> fields`.
2. Add matching Nautilus values (`FixedSizedData`, `StructData`, `VectorData`) with `VarVal` integration so physical functions can read and write composite fields.
3. Wire the new variants through the necessary switch sites: schema printer, sinks, JSON/CSV input / output formatters (via SerDe registry entries), generator source.
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
- Optional default SerDe for the plugin type. Formatters will resort to their default struct-SerDes if no default for the plugin was given.
  For example, to be able to receive values of the Decimal{integer: INT64, fraction: INT64} type in the form of "42.125", a default deserializer for Decimal must be implemented, as these values do not follow the standard struct syntax of {"integer": 42, "fraction": 125}.
- Zero or more non-default SerDes (see `parser-registry` branch).

No edits to core code paths are required beyond the registration entry point.

## Solution vs. Goals
- G1: New plugins live entirely under `nes-plugins/DataTypes/<Name>/` and register through the existing `DataTypeRegistry`.
- G2: `STRUCT` participates in `DataType`, schemas, formatters, parsers, and Nautilus values; composite values round-trip through the system end-to-end.
- G3: Any registered type works with the generic `T(...)` syntax — the parser does not need per-type code.
- G4: Allows registration of default and alternative SerDe plugins for a datatype plugin.
- G5: Multiple end-to-end plugins (`Image`/`ThermalFrame`, `Reading`, `Timestamp` etc.) and a Python demo demonstrate the full path. A new `NestedJSON` input formatter plugin demonstrates ingesting nested input that maps onto composite fields.
- G6: Implemented working FIXEDSIZED datatype.
- G7: Implemented working VECTOR datatype. Each individual field value may have a different number of elements, but a value cannot add or remove any elements after being received by the system.

# Proof of Concept
## Branch
https://github.com/nebulastream/nebulastream/tree/extensible-data-types-timestamp (Implemented PoC solution and plugins. Additionally, allows EventTimeWatermarks to use the "UnsignedTimestamp" datatype plugin as ts).

https://github.com/nebulastream/nebulastream/tree/extensible-data-types-geotemporal (Geospatial and geotemporal type plugins + functions backed by MEOS library. Builds upon MobilityStream.).

## Code Examples
All snippets below are condensed from the systests under `nes-systests/` (`function/casting/Casting.test`,
`formatter/JSON/NestedJSON.test`, `formatter/JSON_OUTPUT/*`). They are grouped by the query-API capability
they exercise — each group is **something** that was impossible before this PoC.

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

CREATE LOGICAL SOURCE thermalRecords(timestamp UINT64 NOT NULL, camera_id UINT32 NOT NULL, frame ThermalFrame NOT NULL);
CREATE PHYSICAL SOURCE FOR thermalRecords TYPE File SET('NestedJSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"TIMESTAMP": 1714060800000, "CAMERA_ID": 1, "FRAME": {"pixels": [30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130]}}
{"TIMESTAMP": 1714060800040, "CAMERA_ID": 1, "FRAME": {"pixels": [30120, 30200, 30240, 30160, 30230, 42150, 52900, 30270, 30250, 53600, 65535, 30310, 30190, 30260, 30225, 30150]}}

SELECT * FROM thermalRecords INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"TIMESTAMP":1714060800000,"CAMERA_ID":1,"FRAME":{"pixels":[30100,30180,30220,30150,30210,41900,52400,30260,30230,53100,65535,30290,30170,30240,30205,30130]}}
{"TIMESTAMP":1714060800040,"CAMERA_ID":1,"FRAME":{"pixels":[30120,30200,30240,30160,30230,42150,52900,30270,30250,53600,65535,30310,30190,30260,30225,30150]}}
```

### 3. A fixed-sized array as a schema column type (P4, G6)
`T ARRAY[N]` is the new `FIXEDSIZED` variant — a type-specific array, distinct from `VARSIZED` (string/bytes).
Per-`elementType` dispatch covers floats, signed/unsigned ints, etc.
```sql
CREATE LOGICAL SOURCE mixedArrays(id UINT64 NOT NULL, floats FLOAT64 ARRAY[3] NOT NULL, signed INT16 ARRAY[2] NOT NULL);
CREATE PHYSICAL SOURCE FOR mixedArrays TYPE File SET('NestedJSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"ID": 1, "FLOATS": [1.5, -2.25, 0.0], "SIGNED": [-1, 32767]}
{"ID": 2, "FLOATS": [0.5, 0.0, -0.0], "SIGNED": [-32768, 0]}

SELECT * FROM mixedArrays INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"ID":1,"FLOATS":[1.5,-2.25,0.0],"SIGNED":[-1,32767]}
{"ID":2,"FLOATS":[0.5,0.0,-0.0],"SIGNED":[-32768,0]}
```

### 4. A variable-sized vector as schema column type (P4, G7)
`T VECTOR` is the new `VECTOR` variant - a type-specific vector, distinct from `FIXEDSIZED` as each value may have an arbitrary amount of elements.
Per-`elementType` dispatch covers floats, signed/unsigned its, etc.
```sql
CREATE LOGICAL SOURCE polygons(timestamp UINT64 NOT NULL, lon UINT64 VECTOR NOT NULL, lat UINT64 VECTOR NOT NULL);
CREATE PHYSICAL SOURCE FOR polygons TYPE File SET('NestedJSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"TIMESTAMP": 1714060800000, "LON": [1, 2, 4, 3, 2], "LAT": [50, 60, 80, 60, 30]}
{"TIMESTAMP": 1714060800040, "LON": [1, 2, 3], "LAT": [30, 40, 90]}

SELECT * FROM polygons INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"TIMESTAMP":1714060800000,"LON":[1,2,4,3,2],"LAT":[50,60,80,60,30]}
{"TIMESTAMP":1714060800040,"LON":[1,2,3],"LAT":[30,40,90]}
```

### 5. Constructing a `STRUCT` value in SQL with `StructName(...)` (P2, P3)
The same generic `exitFunctionCall` dispatch routes a registered `STRUCT` name to `ConstructStructLogicalFunction`,
which expects N child expressions matching the field list. Arguments can be literals, columns, or arbitrary
expressions — and may be projected out of *different* (e.g. joined) streams. Note the constructed value is a
first-class column that flows through windows, joins and the JSON formatter as a nested object.
```sql
CREATE LOGICAL SOURCE thermometer(sensor_id UINT64 NOT NULL, celsius FLOAT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR thermometer TYPE File;
ATTACH INLINE
1,23.5,100
2,24.0,200
3,25.5,300

CREATE LOGICAL SOURCE hygrometer(device_id UINT64 NOT NULL, humidity_pct FLOAT64 NOT NULL, battery_pct UINT8 NOT NULL, ts2 UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR hygrometer TYPE File;
ATTACH INLINE
1,65.0,87,100
2,70.0,80,200
3,55.0,72,300

# Construct struct from constant and literal-addition
SELECT Reading(celsius + FLOAT64(10.0), FLOAT64(60.5)) AS reading FROM thermometer INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"READING":{"temperature":33.5,"humidity":60.5}}
{"READING":{"temperature":34.0,"humidity":60.5}}
{"READING":{"temperature":35.5,"humidity":60.5}}

# Each stream contributes a distinct subset of its fields to the constructed
# Reading: `celsius` from the thermometer side, `humidity_pct` from the
# hygrometer side. battery_pct, uptime_seconds, and the per-stream ids are
# all dropped on the floor by the projection.
SELECT Reading(celsius, humidity_pct) AS reading
FROM (SELECT * FROM thermometer) JOIN (SELECT * FROM hygrometer) ON sensor_id = device_id
  WINDOW TUMBLING (ts, ts2, size 1 sec) INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"READING":{"temperature":23.5,"humidity":65.0}}
{"READING":{"temperature":24.0,"humidity":70.0}}
{"READING":{"temperature":25.5,"humidity":55.0}}
```

### 6. Per-type operator and comparison semantics over a `STRUCT` (P2, NG2)
Because `STRUCT`s are nominally typed, `FunctionProvider` can dispatch `+`, `=`, `>`, … by struct name. The
`Reading` plugin registers pairwise field add; comparisons are decided likewise. An arbitrary `(FLOAT64, FLOAT64)`
struct with a different name would *not* match these — that is the point of NG2.
```sql
CREATE LOGICAL SOURCE thermometer(sensor_id UINT64 NOT NULL, celsius FLOAT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR thermometer TYPE File;
ATTACH INLINE
1,23.5,100
2,24.0,200
3,25.5,300

SELECT Reading(celsius, FLOAT64(50.0)) + Reading(FLOAT64(1.0), FLOAT64(5.0)) AS combined FROM thermometer INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"COMBINED":{"temperature":24.5,"humidity":55.0}}
{"COMBINED":{"temperature":25.0,"humidity":55.0}}
{"COMBINED":{"temperature":26.5,"humidity":55.0}}

SELECT Reading(celsius, FLOAT64(60.5)) AS reading FROM thermometer WHERE Reading(celsius, FLOAT64(60.5)) > Reading(FLOAT64(23.5), FLOAT64(60.5)) INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"READING":{"temperature":24.0,"humidity":60.5}}
{"READING":{"temperature":25.5,"humidity":60.5}}
```

### 7. Domain-specific functions over a plugin `STRUCT` (P2, G5)
`to_celsius`, `is_fever`, `to_rgb` are ordinary `LogicalFunctionRegistry` entries shipped by the Image plugin;
they gate on `dt.type == STRUCT && dt.structName == "ThermalFrame"`, take/return `STRUCT` and `FIXEDSIZED`
values, and are dispatched through SQL → logical/physical lowering → Nautilus like any other function. The
two-arg `to_rgb` takes a `VARSIZED` colormap name and returns an `RGBFrame` `STRUCT` of three planar `UINT8[N]`
channels — a struct whose fields are themselves fixed-sized arrays.
```sql
CREATE LOGICAL SOURCE thermalRecords(timestamp UINT64 NOT NULL, frame ThermalFrame NOT NULL);
CREATE PHYSICAL SOURCE FOR thermalRecords TYPE File SET('NestedJSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"TIMESTAMP": 1, "FRAME": {"pixels": [30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000]}}
{"TIMESTAMP": 2, "FRAME": {"pixels": [30000, 30000, 30000, 30000, 30000, 31200, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000]}}

SELECT timestamp, to_celsius(frame) AS celsius, is_fever(frame) AS fever FROM thermalRecords INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"TIMESTAMP":1,"CELSIUS":[26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85],"FEVER":false}
{"TIMESTAMP":2,"CELSIUS":[26.85,26.85,26.85,26.85,26.85,38.849998,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85,26.85],"FEVER":true}
```
The `nes-plugins/DataTypes/Image/demo/run_demo.py` script wires this end to end: it feeds a real thermal frame
in, runs `to_rgb`, and writes out the colorized image — all of the type-specific behaviour living in the plugin.

### 8. DataType-plugin-specific SerDes (P1, G4)
Since every datatype-plugin is treated as a struct-variant, the current approach of registering SerDes for datatype-variants, as introduced by `parser-registry`, does not allow us to register SerDes for individual datatype plugins.
This PoC allows to register SerDes for datatype plugins, by combining the name of the SerDe type with the name of the plugin (for example, `"DefaultTimestampValueDeserializer"`).
As an effect, this allows formats like CSV, which usually do not support composite values, to receive values of datatype plugins.
If not specifically configured, the system will resort to the `Default<Plugin Name>` SerDe, if it exists. Otherwise, the system will use the default SerDe for the struct variant, configured by the format.
In the following example, the `Date`, `Time`, and `Timestamp` plugins all have default SerDe implement, which accept ISO 8601 formatted data.
```sql
CREATE LOGICAL SOURCE timeSource(date Date NOT NULL, time Time NOT NULL, timestamp Timestamp NOT NULL);
CREATE PHYSICAL SOURCE FOR timeSource TYPE FILE;
ATTACH INLINE
2004-08-28,03:52:42,2008-07-11 12:32:07.278912

SELECT * FROM timeSource INTO FILE('CSV' as "SINK".OUTPUT_FORMAT);
----
2004-08-28,03:52:42,2008-07-11 12:32:07.278912
```

### 9. Timestamp Plugin as EventTimeWatermark
As the internal representation of the `Timestamp` plugin is an integer, this plugin can be used as field of the `TimeFunction` providing the timestamp to the EventTimeWatermark operator.
NES currently expects timestamps to be unsigned. To use the `Timestamp` plugin as EventTimeWatermark, we created an additional `UnsignedTimestamp` plugin that ignores time before unix epoch.
The following query demonstrates aggregations using `UnsignedTimestamp`-typed fields as ts.

```sql
CREATE LOGICAL SOURCE countStream(value INT32, ts UnsignedTimestamp NOT NULL, id INT32 NOT NULL);
CREATE PHYSICAL SOURCE FOR countStream TYPE File;
ATTACH INLINE
10,1970-01-01 00:00:00.010,1
30,1970-01-01 00:00:00.020,2
,1970-01-01 00:00:00.060,1
40,1970-01-01 00:00:00.110,2
50,1970-01-01 00:00:00.160,1
,1970-01-01 00:00:00.210,2
       
CREATE SINK sinkCountStar(start UINT64 NOT NULL, end UINT64 NOT NULL, rowCount UINT64 NOT NULL) TYPE File;

SELECT start, end, COUNT(*) AS rowCount
FROM countStream WINDOW TUMBLING(ts, size 100 ms)
INTO sinkCountStar;
----
0,100,3
100,200,2
200,300,1
```

## Locations of Changes
- `nes-plugins/DataTypes/Image/`: registers `Image` and `ThermalFrame`, plus `to_celsius`, `is_fever`, `to_rgb`. Demo at `nes-plugins/DataTypes/Image/demo/run_demo.py` consumes a thermal image and emits a colorized RGB image.
- `nes-plugins/DataTypes/Reading/`: registers `Reading{celsius, humidity_pct}` and an addition function over `Reading`s, demonstrating a second independent struct with arithmetic semantics.
- `nes-plugins/InputFormatters/NestedJSONInputFormatter/`: separate plugin, demonstrates that ingesting nested JSON into composite columns is itself an opt-in plugin.
- `nes-plugins/DataTypes/Time`: registers `Time`, `Date`, `(Unsigned)Timestamp` plugins and their SerDes.
- Systests under `nes-systests/formatter/JSON_OUTPUT/` (`StructConstruction`, `StructAdd`, `StructWHERE`, `ThermalFrameStructJSON`, `ThermalFrameFunctions`, `ThermalFrameToRGB`, `FixedSizedArrayJSON` ...) exercise the end-to-end paths.
- Systests under `nes-systests/formatter/datatype_plugins` for all `Timestamp` related tests.
- (Branch extensible-datatypes-geotemporal) `nes-plugins/MEOS`: registers all geospatial / geotemporal datatypes and the functions they can be used with. Includes MobilityStream's MEOS wrapper that acts as interface between NebulaStream and the MEOS library.

# Implementation Plan
- Rebase with main to employ the refactored plugin registration approach
- fix hardcoded antlr parser types (allow identifier)
    - currently antlr only expects specific keywords, e.g., UINT8, which is an anti-pattern for extensible data types
- allow data type construction from literals (casting), e.g., UINT64(value + 2) as valPlusTwo
- (optional: data type refactoring in front end)
- (optional: memory layout refactoring)
- introduce struct type (frontend+backend)
    - either like PoC or via MemoryLayout/BufferRef
- introduce fixed-sized with type
    - either like PoC or via MemoryLayout/BufferRef
- introduce vector with type
    - either like PoC or via MemoryLayout/BufferRef
- (potentially) timestamp refactor that introduces the `Timestamp` datatype plugin alongside multiple SerDes as the only valid timestamp type for watermarks.
- (potentially) introduce Image type with Mono16 and functions for SIGMOD demo use case

# Extensible Data Types in other Systems
Allowing users to extend the datatypes, a system offers, with optional plugins can have several meanings. 
As simplification, we can differentiate between two types of data type extensions.

### User-defined logical data types
This type of extension is usually based on composite / struct types.
The system offers a set amount of fully implemented basic / leaf types (`INT`, `CHAR`, `BOOL` ...) and container types (`Array`, `Vector`, `Map`...) and allows users to create compositions out of basic types, container types, and further composed types.
The user might also be able to define `functions` on their custom type composition, which perform operations using the fields of the composite type.

Our PoC follows this approach: As a first step, we introduced `FIXEDSIZED` as array type (and later on `VECTOR`). As a second step, we introduced the `STRUCT` type, which consists of typed and named fields.
We allow users to register a struct with a specific combination of fields under a name, which can be used like a type for fields of a schema and during ad-hoc construction inside a SQL query.
However, `FIXEDSIZED`, `VECTOR`, and `STRUCT` all have fixed physical representations within the records and the buffer, that the user cannot alter within their plugin definition: 
`STRUCT` and `FIXEDSIZED` byte-align the physical representation of their fields / elements inside the buffer.
`VECTOR` stores an 8 byte child buffer address and an 8 byte size of the location of the byte-aligned vector elements.
For the in-record representation, we implemented a nautilus data type for each of the variants.
Therefore, users may define functions and SerDes plugins using their datatype plugins, but need to use the functions provided by the `StructData` class to access the fields of their composite type.

### User-defined physical data types
This type of extension allows users, additionally to the logical structure of the type, to define the physical representation of the type in-memory.
Therefore, entirely new non-composite types are possible this way.

This type of user-extension is currently not supported in our PoC. Writing and reading a value from memory is hard-coded into the corresponding `VarVal` functions.
The only flexibility in this regard that we offer is the option to create plugins for datatype-plugin specific SerDes, which are naturally only employed for incoming / outgoing buffers.
Customizable physical layout of values and tuples within the buffer could be the next logical step of this extension, since they offer room for optimizations, but are of higher complexity, as they impact `BufferRefs` and `VarVals`.

### Other Systems
In the following overview, we briefly describe user-extensibility of data types in other stream processing engines and database systems.
#### Apache Flink


# Summary
The PoC adds three extensible variants (`FIXEDSIZED`, `VECTOR`, `STRUCT`) to `DataType`, a generic SQL `T(...)` constructor pipeline, and multiple plugins for several real-life use cases that demonstrate the full path from registration to physical execution.
P1 is addressed by routing all type-specific logic through the existing registry pattern and through generic `Construct`/`Cast` functions instead of per-type switch cases.
P2 is addressed by `STRUCT` with nominal typing.
P3 is addressed by the generic `exitFunctionCall` parser dispatch.
P4 is addressed by the addition of the `FIXEDSIZED` and `VECTOR` types. 
Current constraint: `VECTOR` and `FIXEDSIZED` currently cannot be constructed within a SQL query like values of registered datatype plugins can.

# Open Questions
- OQ1: Do we want to evolve `DataType` itself to a polymorphic representation (NG1)? Or do we wait until there is a clear need? In our opinion, such a refactor would warrant its own DD that then also covers a full-blown type system.
- OQ2: Do we want to refactor the memory layout / buffer ref before implementing fixed-sized and struct as variants? While this is the better solution, a memory layout / buffer ref refactor that can handle compound/struct data types is a major undertaking. Additionally, I'd rather work on that refactor with already existing compound/struct data types that can demonstrate the effectiveness. 
- OQ3: (Possibly worth a separate design doc): In the case that we replace UINT64 with the `Timestamp` datatype plugin as input type for our watermarks: Do we want to switch to signed integers to represent the timestamps internally? This allows us to capture points int time before `1970-01-01 00:00:00`. The change would impact multiple files responsible for window slices and aggregation/join build.
