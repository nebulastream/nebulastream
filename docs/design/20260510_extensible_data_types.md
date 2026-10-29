# General Information
The PoC branch attached to this DD is not a feature branch in disguise.
Its sole purpose is to give you a concrete, interactive vision of what we are aiming for with a first version of extensible data types.
As a result there are at least three major simplifications that the PoC makes use of:
1. keeps simplistic datatype frontend/logical implementation
2. does not dive into a memorylayout-/bufferref-based implementation for custom/new datatypes, which is probably the clean way, but requires a significant refactor of bufferrefs
3. ignores nullability of struct fields and array / vector elements

# The Problem
Adding a new logical type means editing core code at every switch site: parsers, schema printer, sinks, JSON/CSV formatters, Nautilus runtime, generator source, etc.
There is no way to register a new type from `nes-plugins/`, as reflected in the DataType runtime registry definition, although sources, sinks, and input/output formatters already follow that pattern.

In particular, real-world ingest scenarios need *composite* types and / or array types, whose layout is fixed but bigger than a scalar, e.g. a thermal-camera reading `Image{timestamp, cameraId, ThermalFrame{pixels: UINT16[N]}}` or a trajectory `Trajectory{points: TemporalPoint{lon: FLOAT64, lat: FLOAT64, ts: INT64}[N]}`
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
- G4: Support the definition of format-independent default Serde for each individual DataType plugin (in contrast to only offering format-dependent default Serde for the "Struct" type), addressing P1.
- G5: Demonstrate G1–G4 end-to-end with example plugins that fit on top of the core without modifying it (`Image/ThermalFrame`, `Reading`, `(Temporal)Point(Sequence)`, `(Temporal)Polygon`, `SpatioTemporalBox`,  `Date`, `Time`, `Timestamp`).
- G6: Create a fixed-sized (is an array, but name contrasts 'varsized') datatype that is type-specific, i.e., represents an array of type T with a fixed number of elements.
- G7: Create a vector datatype, which functions similar to the fixed-sized / array type but does not declare the number of elements per value in the schema definition, making it variable-sized.
- G8: Provide a generic SQL constructor syntax for Arrays and Vectors `Array(args1, arg2, ...)`.

# Non-Goals
- NG1: Refactoring `DataType` into a polymorphic class hierarchy or `std::variant`.
  `DataType` stays a flat struct with optional members per variant; this is a deliberate PoC scope decision (smaller blast radius, follows the existing `FIXEDSIZED` precedent).
- NG2: Structural typing.
  Two structs with the same physical layout but different `structName` are distinct types and do not `join()`; this preserves the safety of name-keyed domain functions.
- NG3: Full coverage of every downstream switch site.
  Nautilus, codegen, serializer, and a handful of other switch sites currently throw `NotImplemented` for `STRUCT`, `FIXEDSIZED`, and `VECTOR`; wiring those is follow-up work.
- NG4: Create a dedicated string datatype. Varsized could become the dedicated string datatype after we introduced arrays and vectors, representing a contiguous sequence of bytes.
- NG5: Dynamic resizing of vector values. In this PoC, the number of elements of a vector value is set as soon as the value arrives into the system. However, the addition of this feature should be possible without any conflicts.
- NG6: Nesting of variable-sized types. A vector is not allowed to contain any values that are variable-sized as well. We avoid storing child buffer addresses within child buffers via this constraint.

# Alternatives
- A1: Polymorphic `DataType` hierarchy (each variant a subclass).
  Cleaner long term, but a much larger refactor that touches every consumer of `DataType`.
  Rejected because it is orthogonal to demonstrating extensibility and would block this PoC indefinitely. We mainly want to build on top of the schema inference branch and then add complexity to the 'frontend' of the datatypes if required.
- A2: Structural typing for composite types (compare by layout).
  Rejected because it makes domain-specific functions unsafe (an arbitrary `(uint64, uint32, uint16[N])` struct would match `Image`).
- A3 [Proposal by Lukas](https://gist.github.com/ls-1801/2dc6c07fe07ad81a26c5a84715a0e9e7): Remove "logical" data type enum from `DataType.hpp`. Replace with simple string containing the name of the data type.
  User provides a mapping of a logical type to a composition of named, non-nested fundamental nautilus types (which replace the type-enum). E.g. `Point → {x: INT64, y: INT64}`.
More complex types use `VARSIZED` as escape hatch. `VarVal` is replaced by `Value`, holding the named nautilus fundamental values.
Instead of `T()` dispatching to construct struct calls, the user may register the constructors of the data type plugins via the function registry interface, using the name of the data type as function name.
`Value` is stored inline in memory, since it only consist of fixed-sized nautilus fundamental values (varsized is the only exception of course).
Furthermore, the PoC suggests storing nullability and array size info as part of the logical data type, which is carried by `Value` and can be used as metadata for memory layouts and `well-known` functions.
This alternative proposes many concepts that are not covered by our design document yet due to simplicity reasons. Well-known functions that signal whether a type is eligible for sorting or specific aggregations seems like a convenient feature to have.
Pattern matching for function argument definition is useful as well. However, both of these features can be implemented on top of our PoC as well.
I am not sure whether `FromJson` / `ToCSV` well-known function dispatches make sense for most extensible data types. In some cases like `Timestamps` or `Decimals` the external / textual representation does not depend on the format.
As long as we provide default serde for a data type plugin, the values can be transmitted via flat values of any textual format. As default for structs, arrays, and vectors that do not implement plugin-specific serde,
we can fall back to the format-specific defaults for struct, array, and vector, which are only implemented for each `DataType::Type` variant, not for every plugin.
We agree that arbitrary nesting of data types is too complex for now. This is why we forbid the creation of types that nest two or more variable-sized datatypes. 
However, even nested, fixed-sized structs can be represented inlined in memory, which our PoC demonstrates. Therefore, what `Value` is trying to achieve is possible in our PoC as well.
The `VARSIZED` escape hatch in this alternative is similar to our solution for the Vector type, which is represented like `Varsized` in memory. However, we show
that it is possible to use this kind of escape hatch on a field granularity rather than representing a whole data type plugin as `varsized`. We inline sub-fields byte-aligned if possible and resort to the variable-sized content variant only for `VARSIZED` and `VECTOR`.
Additionally, constructing a plugin like `Circle` out of a `Point` center and a `FLOAT64` radius is easy to understand for the user and still maintains the flat layout in memory, since `Point` and `FLOAT64` are flat as well.

# Solution Background
- `FIXEDSIZED` was added on this same branch as a precedent for an "extensible-but-flat" variant that participates in formatters, parsers, and Nautilus (G5).
  `STRUCT` follows the same shape (extra fields on `DataType`, new Nautilus value type, switch-site handling) and receives Serde implementations for the JSON format.
- `DataTypeRegistry` and `DataTypeProvider` already existed for the registry pattern; this PoC changes the registry's entry template from the `DataType` constructor to `&provide<KEY>DataType`, enabling data type plugins implementing this function.
- We build on top of the `main` branch. Recent major changes like the refactored schema, refactored registry concept, and the Serde registries are therefore already covered.
  This PoC extends the recently added Serde registry feature by allowing the registration of plugin-specific default serde, which take precedence over the format-specific defaults.

# Proposed Solution
1. Extend `DataType` with three new variants and their per-variant fields:
   - `FIXEDSIZED` (Fixed-sized Array) carries `elementType` + `count`.
   - `VECTOR` (Variable-sized Array) carries `elementType`
   - `STRUCT` carries `structName` + `vector<pair<string, DataType>> fields`. Similar to `STRUCTURE` type in `Flink` and `Composite` type in `PostgreSQL`
2. Add matching Nautilus values (`FixedSizedData`, `StructData`, `VectorData`) with `VarVal` integration so physical functions can read and write composite and array fields.
3. Implement `VarVal::writeToMemory` and `VarVal::readVarValFromMemory` for every nautilus datatype (including `VarsizedData`, `FixedsizedData`, `StructData`, `VectorData`) to support nested values within a pipeline:
   - Fixed-sized data types (basic types, `fixed-sized arrays`, `structs`) inline their values byte-aligned in order into memory.
   - Variable-sized data types (varsized, vector) are represented by 16 bytes, the first 8 being a pointer to the location of the actual contents in memory, the last 8 being the size of the contents in bytes.
   - This allows `arrays` and `structs` to hold variable-sized values while maintaining their fixed size.
   The write / read functions for a specific index in `FixedsizedData` and `StructData` can thus calculate the address of the value in memory and leave the write / read to the VarVal functions.
   A proper physical layout refactor will likely replace these hard-coded representations with customizable layouts.
4. Wire the new struct, fixed-sized array, and vector types through the `TupleBufferRef` read and write functions for the representation of the types within a buffer:
   - Fixed-sized data types (basic types, `fixed-sized arrays`, `structs`) will be inlined, analogue to their representation within the pipeline.
   - As a variable-sized type, `Vector` shares the 16 bytes `VariableSizedAccess` representation of the `Varsized` type and stores its contents in a child buffer.
   This imposes a significant `constraint` regarding nested data types: Our current implementation strictly `prohibits` the creation of data types which `nest multiple variable-sized types`.
   Such types would need to store `VariableSizedAccess` representations within a child buffer, pointing to content in another child buffer. This behavior is currently not implemented into `TupleBufferRef`.
   The attempt to use such a data type in a query will result in an error during query compile time.
5. Wire the new variants through the necessary switch sites: schema printer, sinks, JSON/CSV input / output formatters (via Serde registry entries), generator source.
6. Add five generic, type-driven logical/physical functions that replace the per-type pattern:
   - `ConstructArrayLogicalFunct` / `ConstructArrayPhysicalFunction`: For `Array(expr1, expr2...)` declarations, builds a fixed-sized array value from N child expressions. All child expression types need to be joined to a collective data type.
   - `ConstructVectorLogicalFunct` / `ConstructVectorPhysicalFunction`: For `Vector(expr1, expr2...)` declarations, builds a variable-sized vector value from N child expressions. All child expression types need to be joined to a collective data type.
   - `ConstructStructLogicalFunction` / `ConstructStructPhysicalFunction`: for any registered `STRUCT`, build a value from N child expressions matching the field list.
   - `CastToTypeLogicalFunction`: for any registered non-`STRUCT` type, cast an expression to it.
   - `ConstantValueLogicalFunction` (existing): used when the argument list is a single literal.
7. Dispatch SQL `T(...)` calls in the parser's `exitFunctionCall` default case based on `DataTypeProvider::tryProvideDataType(funcName)`:
   - `Array` -> `ConstructArrayLogicalFunction`
   - `Vector`-> `ConstructVectorLogicalFunction`
   - registered `STRUCT` → `ConstructStructLogicalFunction`
   - registered non-`STRUCT`, single literal arg → `ConstantValueLogicalFunction`
   - registered non-`STRUCT`, expression arg → `CastToTypeLogicalFunction`
   - otherwise → existing function-registry lookup
8. Domain functions stay per-struct (e.g. `to_celsius`, `is_fever`, `to_rgb`) and gate themselves on `dt.type == STRUCT && dt.structName == "Image"` inside the registered `LogicalFunctionRegistry` entry.
9. Registered data type plugins may define `Default<Key>ValueDeserializer` and `Default<Key>ValueSerializer`, in case a specific plugin does not follow the generic struct form of the input / output format.
This can be useful for types like `Decimal`, which might arrive as a single consecutive textual value, but is stored as a composite value out of the whole number and the fraction.
This functionality avoids that the user has to manually define the correct serializer for each field with the plugin type in the schema definition.

A new plugin therefore consists of:
- one `*DataType.hpp/cpp` that registers the type via `provideXxxDataType(DataTypeRegistryArguments)`;
- one `CMakeLists.txt` line `add_registry_entry(DataType <name>)`;
- zero or more domain-specific logical/physical functions, registered the same way as any other function.
- Optional default serde for the plugin type. Formatters will resort to their default struct-serde if no default for the plugin exists.
  For example, to be able to receive values of the Decimal{integer: INT64, fraction: INT64} type in the form of "42.125", a default deserializer for Decimal must be implemented, as these values do not follow the standard struct syntax of {"integer": 42, "fraction": 125}.
- (optionally) more non-default Serde for this plugin type.

No edits to core code paths are required beyond the registration entry point.

## Solution vs. Goals
- G1: New plugins live entirely under `nes-plugins/DataTypes/<Name>/` and register through the existing `DataTypeRegistry`.
- G2: `STRUCT` participates in `DataType`, schemas, formatters, parsers, and Nautilus values; composite values round-trip through the system end-to-end.
- G3: Any registered type works with the generic `T(...)` syntax — the parser does not need per-type code.
- G4: Allows registration of default and alternative Serde plugins for a datatype plugin.
- G5: Multiple end-to-end plugins (`Image`/`ThermalFrame`, `Reading`, `Timestamp`, `TemporalPoint` etc.) and a Python demo demonstrate the full path.
- G6: Implemented working FIXEDSIZED datatype.
- G7: Implemented working VECTOR datatype. Each individual field value may have a different number of elements, but a value cannot add or remove any elements after being received by the system.
- G8: `Array(...)` and `Vector(...)` constant expressions in the query are dispatched in `exitFunctionCall` and enable the creation of potentially nested arrays.

# Proof of Concept
## Branch
https://github.com/nebulastream/nebulastream/tree/extensible-data-types-poc (Implemented PoC solution and exemplary plugins. Additionally, allows EventTimeWatermarks to use the "UnsignedTimestamp" datatype plugin as ts).

## Code Examples
All snippets below are condensed from the systests under `nes-systests/` (`function/casting/Casting.test`,
`formatter/JSON/NestedJSON.test`, `formatter/JSON_OUTPUT/*`, `formatter/datatype_plugins`). They are grouped by the query-API capability
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
Per-`elementType` dispatch covers floats, signed/unsigned ints, structs / plugins, varsized etc.
This syntax is similar to the array type of `PostgreSQL`.
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
Similar to variable-sized arrays in `PostgreSQL`, `T ARRAY[]` will also dispatch to `VECTOR`.
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

CREATE LOGICAL SOURCE altVec(vec INT32 ARRAY[] NOT NULL);
CREATE PHYSICAL SOURCE FOR altVec TYPE File SET('JSON' as INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"VEC":[1,2,3,4,5]}
{"VEC":[2,0,7]}
{"VEC":[1,1,0,1,0,1,1]}

SELECT * FROM altVec INTO FILE(SCHEMA(vec INT32 VECTOR NOT NULL) AS "SINK"."SCHEMA", 'JSON' AS "SINK".OUTPUT_FORMAT);
----
{"VEC":[1,2,3,4,5]}
{"VEC":[2,0,7]}
{"VEC":[1,1,0,1,0,1,1]}
```

### 5. Definition of nested arrays (P4, G6, G7)
To nest multiple arrays, additional brackets can be appended to the type, like: `T ARRAY[3][3][2]`. 
The rightmost bracket corresponds to the innermost array and holds elements of the base type `T`.
Empty brackets dispatch to `VECTOR`. 
This syntax to define nested arrays is similar to the syntax in `PostgreSQL`.
```sql
# Vector of fixed-sized arrays: INT32 ARRAY[][2] is a variable number of INT32 pairs
CREATE LOGICAL SOURCE pairLists(id UINT64 NOT NULL, pairs INT32 ARRAY[][2] NOT NULL);
CREATE PHYSICAL SOURCE FOR pairLists TYPE File SET('JSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"ID": 1, "PAIRS": [[1, 2], [3, 4], [5, 6]]}
{"ID": 2, "PAIRS": [[7, 8]]}
{"ID": 3, "PAIRS": [[9, 10], [11, 12], [13, 14], [15, 16], [17, 18]]}

SELECT * FROM pairLists INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"ID":1,"PAIRS":[[1,2],[3,4],[5,6]]}
{"ID":2,"PAIRS":[[7,8]]}
{"ID":3,"PAIRS":[[9,10],[11,12],[13,14],[15,16],[17,18]]}

# Fixed-sized array of vectors: INT32 ARRAY[2][] is exactly 2 INT32 vectors of arbitrary length
CREATE LOGICAL SOURCE vectorPairs(id UINT64 NOT NULL, vecs INT32 ARRAY[2][] NOT NULL);
CREATE PHYSICAL SOURCE FOR vectorPairs TYPE File SET('JSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"ID": 1, "VECS": [[1, 2, 3], [4]]}
{"ID": 2, "VECS": [[5], [6, 7, 8, 9]]}

SELECT * FROM vectorPairs INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"ID":1,"VECS":[[1,2,3],[4]]}
{"ID":2,"VECS":[[5],[6,7,8,9]]}
```

### 6. Constructing a `STRUCT` value in SQL with `StructName(...)` (P2, P3)
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

### 7. Constructing fixed-sized arrays and vectors in SQL via `ARRAY(...)` and `VECTOR(...)` (P4, G8)
Similar to the struct construction, `ARRAY` and `VECTOR` expressions are dispatched in `exitFunctionCall` and enable the construction of arrays and vectors out of any child expressions.
All child expression must be able to be cast to a collective data type.
The construction of nested vectors or vectors of `VARSIZED` is prohibited and will result in an error.
```sql
CREATE LOGICAL SOURCE pairs(id UINT64 NOT NULL, p1 INT32 ARRAY[2] NOT NULL, p2 INT32 ARRAY[2] NOT NULL, v INT32 VECTOR NOT NULL, w INT32 VECTOR NOT NULL);
CREATE PHYSICAL SOURCE FOR pairs TYPE File SET('JSON' AS INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"ID": 1, "P1": [1, 2], "P2": [3, 4], "V": [1, 2, 3], "W": [4]}
{"ID": 2, "P1": [5, 6], "P2": [7, 8], "V": [9], "W": [10, 11]}
       
# Mixing existing array columns with constructed arrays
SELECT id, VECTOR(p1, ARRAY(INT32(0), INT32(0)), p2) AS matrix FROM pairs INTO File('JSON' as "SINK".OUTPUT_FORMAT);
----
{"ID":1,"MATRIX":[[1,2],[0,0],[3,4]]}
{"ID":2,"MATRIX":[[5,6],[0,0],[7,8]]}
```

### 8. Per-type operator and comparison semantics over a `STRUCT` (P2, NG2)
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

### 9. Domain-specific functions over a plugin `STRUCT` (P2, G5)
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

### 10. DataType-plugin-specific Serde (P1, G4)
Since every datatype-plugin is treated as a struct-variant, the current approach of registering Serde for datatype-variants, as introduced by `parser-registry`, does not allow us to register Serde for individual datatype plugins.
This PoC allows to register Serde for datatype plugins, by combining the name of the Serde type with the name of the plugin (for example, `"DefaultTimestampValueDeserializer"`).
As an effect, this allows formats like CSV, which usually do not support composite values, to receive values of datatype plugins.
If not specifically configured, the system will resort to the `Default<Plugin Name>` Serde, if it exists. Otherwise, the system will use the default Serde for the struct variant, configured by the format.
In the following example, the `Date`, `Time`, and `Timestamp` plugins all have default Serde implement, which accept ISO 8601 formatted data.
```sql
CREATE LOGICAL SOURCE timeSource(date Date NOT NULL, time Time NOT NULL, timestamp Timestamp NOT NULL);
CREATE PHYSICAL SOURCE FOR timeSource TYPE FILE;
ATTACH INLINE
2004-08-28,03:52:42,2008-07-11 12:32:07.278912

SELECT * FROM timeSource INTO FILE('CSV' as "SINK".OUTPUT_FORMAT);
----
2004-08-28,03:52:42,2008-07-11 12:32:07.278912
```

### 11. Timestamp Plugin as EventTimeWatermark
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

### 12. MEOS-backed functions for geospatial and geotemporal data types
Building upon the MobilityNebula MEOS-wrapper by Mariana Duarte, we added several data type plugins for geospatial and geotemporal types like `(Temporal)Point(Sequence)`, `SpatioTemporalBox`, and `(Temporal)Polygon`.
Previously, the MEOS-wrapper used textual value definitions to create `TemporalInstants` and `TemporalSequence` objects for geotemporal values.
We extended the wrapper to use the binary data of the plugin values to directly construct the `MEOS` objects.
This avoids the textual serialization of the values on NebulaStream's side and the deserialization on MEOS's side.
```sql

# temporal_eintersects_geometry with a TemporalPointSequence on the left side. Sequences are treated as discrete, so only the contained instants are checked:
# 1: all instants inside, 2: one instant inside, 3: all instants outside, 4: the segment between the instants crosses the polygon but no instant lies in it, 5: instant on the boundary
CREATE LOGICAL SOURCE tSequencePoints(id UINT64 NOT NULL, seq TemporalPointSequence NOT NULL);
CREATE PHYSICAL SOURCE FOR tSequencePoints TYPE File SET('Json' as INPUT_FORMATTER."TYPE");
ATTACH INLINE
{"ID": 1, "SEQ": {"sequence": [{"lon": 1.0, "lat": 1.0, "ts": 100}, {"lon": 2.0, "lat": 2.0, "ts": 110}]}}
{"ID": 2, "SEQ": {"sequence": [{"lon": 6.0, "lat": 6.0, "ts": 100}, {"lon": 2.5, "lat": 2.5, "ts": 110}, {"lon": 7.0, "lat": 7.0, "ts": 120}]}}
{"ID": 3, "SEQ": {"sequence": [{"lon": 6.0, "lat": 1.0, "ts": 100}, {"lon": 7.0, "lat": 2.0, "ts": 110}]}}
{"ID": 4, "SEQ": {"sequence": [{"lon": -1.0, "lat": 2.5, "ts": 100}, {"lon": 6.0, "lat": 2.5, "ts": 110}]}}
{"ID": 5, "SEQ": {"sequence": [{"lon": 5.0, "lat": 5.0, "ts": 100}]}}

SELECT id FROM tSequencePoints WHERE temporal_eintersects_geometry(seq, Polygon(VECTOR(Point(0.0, 0.0), Point(5.0, 0.0), Point(5.0, 5.0), Point(0.0, 5.0), Point(0.0, 0.0)))) INTO FILE('CSV' as "SINK".OUTPUT_FORMAT);
----
1
2
5
```

## Locations of Changes
- `nes-plugins/DataTypes/Image/`: registers `Image` and `ThermalFrame`, plus `to_celsius`, `is_fever`, `to_rgb`. Demo at `nes-plugins/DataTypes/Image/demo/run_demo.py` consumes a thermal image and emits a colorized RGB image.
- `nes-plugins/DataTypes/Reading/`: registers `Reading{celsius, humidity_pct}` and an addition function over `Reading`s, demonstrating a second independent struct with arithmetic semantics.
- `nes-plugins/InputFormatters/JSONInputFormatter/Deserializers`: New deserializers for json-formatted structs, arrays, and vectors.
- `nes-plugins/OutputFormatters/JSONOutputFormatter/Serializers`: New serializers for json-formatted structs, arrays, and vectors.
- `nes-plugins/DataTypes/Time`: registers `Time`, `Date`, `(Unsigned)Timestamp` plugins and their Serde.
- Systests under `nes-systests/formatter/JSON_OUTPUT/` (`StructConstruction`, `StructAdd`, `StructWHERE`, `ThermalFrameStructJSON`, `ThermalFrameFunctions`, `ThermalFrameToRGB`, `FixedSizedArrayJSON` ...) exercise the end-to-end paths.
- Systests under `nes-systests/formatter/datatype_plugins` for all `Timestamp` related tests.
- `nes-plugins/MEOS`: registers all geospatial / geotemporal datatypes and the functions they can be used with. Includes MobilityNebula's MEOS wrapper that acts as interface between NebulaStream and the MEOS library.
Tests exercising the geotemporal functions can be found in `nes-systests/formatter/JSON_OUTPUT/GeoTemporalTypes.test`.

# Implementation Plan
The features covered by our PoC can be gradually integrated into the `main` branch:
- End-to-end `STRUCT`-variant support for `DataType` + enable `DataType` plugin registration. Allow identifiers as field type to define fields of plugin types.
- Dispatch `T(...)` in SQL as plugin type construction, if `T` is the name of a data type plugin.
- Dispatch `T(...)` in SQL as type-construction / casting of any expression, if `T` is the name of a non-struct type. (Can potentially be combined with previous step).
- End-to-end `FIXEDSIZED`-variant for `DataType`. Allow (potentially nested) fixed-sized arrays as field type (`INT32 ARRAY[2][2]`).
- Dispatch `Array(...)` in SQL as array construction.
- End-to-end `VECTOR`-variant support for `DataType`. Allow `VECTOR` as field type (`INT32 VECTOR`, `INT32 ARRAY[2][]`).
- Dispatch `Vector(...)` in SQL as vector construction.
- Enable registration of default Serde for plugin types.
- (potentially): Addition of data type plugins and functions that could serve as exemplary custom types or are generally useful plugins to offer (e.g. geospatial types or Image type with Mono16 and functions for SIGMOD demo use case).
- (potentially) Timestamp refactor that introduces the `Timestamp`, `Time`, `Date` data type plugins in a similar manner to PostgreSQL. Represent as INT64 internally so that timestamps before unix epoch can also be captured. Implement several supported timestamp formats (ISO 8601 ...).
- (potentially, follow-up to previous step): Rewire watermark, window, and slice logic completely to `Timestamp` plugin.
- (potentially: memory layout refactoring)

# Extensible Data Types in other Systems
Allowing users to extend the datatypes, a system offers, with optional plugins can have several meanings. 
As simplification, we can differentiate between two types of data type extensions.

### User-defined logical data types
The system offers a set amount of fully implemented basic / leaf types (`INT`, `CHAR`, `BOOL` ...) and container types (`Array`, `Vector`, `Map`...) and allows users to create compositions out of basic types, container types, and further composed types.
The user might also be able to define `functions` on their custom type composition, which perform operations using the fields of the composite type.

Our PoC follows this approach: As a first step, we introduced `FIXEDSIZED` as array type (and later on `VECTOR`). As a second step, we introduced the `STRUCT` type, which consists of typed and named fields.
We allow users to register a struct with a specific combination of fields under a name, which can be used like a type for fields of a schema and during ad-hoc construction inside a SQL query.
However, `FIXEDSIZED`, `VECTOR`, and `STRUCT` all have fixed physical representations within the records and the buffer, that the user cannot alter within their plugin definition: 
`STRUCT` and `FIXEDSIZED` byte-align the physical representation of their fields / elements inside the buffer.
`VECTOR` stores an 8 byte child buffer address and an 8 byte size of the location of the byte-aligned vector elements.
For the in-record representation, we implemented a nautilus data type for each of the variants.
Therefore, users may define functions and Serde plugins using their datatype plugins, but need to use the functions provided by the `StructData` class to access the fields of their composite type.

### User-defined physical representation
This type of extension allows users, additionally to the logical structure of the type, to define the physical representation of the type in-memory.
Therefore, users are not bound to a predefined physical representation offered by the system, as it is currently the case in NebulaStream (`RowTupleBuffer`, `ColumnTupleBuffer`, `VarVal`).

This type of user-extension is currently not supported in our PoC. Writing and reading a value from memory is hard-coded into the corresponding `VarVal` functions, so the physical layout of each type cannot be altered without rewriting `VarVal`, the nautilus type implementations, and the `BufferRef` files.
The only flexibility in this regard that we offer is the option to create plugins for datatype-plugin specific Serde, which are naturally only employed for incoming / outgoing buffers, ergo the external representation.
Customizable physical layout of values and tuples within the buffer could be the next logical step of this extension, since they offer room for optimizations, but are of higher complexity, as they impact `BufferRefs` and `VarVals`.

### Other Systems
In the following overview, we briefly describe user-extensibility of data types in other stream processing engines and database systems.
#### Apache Flink
Flink offers two "composite" type options: 
`ROW` is a composite type consisting of fields with a name, data type, and an optional field description.
`STRUCTURE` is the type for user-extensions. It consists of fields as well as a structure name. Therefore, two structures with the same fields but different names will not be considered equal types by the system.
The `STRUCTURE` type is similar to what we offer in our PoC. Flink does not allow casting of a specific `STRUCTURE` value to a `STRUCTURE` of a different name, even if the fields are equal -> they are considered different types.

Flink does not allow users to change the physical representation of types. Very similar to NebulaStream, basic logical data types map to their respective internal basic type (SMALLINT → short, FLOAT → float etc.), while complex types like `Array`, `Row`, `String` etc. have their dedicated internal interfaces like `ArrayData` and `RowData` etc., which define functions to access elements, size etc.
These interfaces are realized by classes like `BinaryArrayData`, describing the physical representation of the types. Writer helper classes like `BinaryArrayWriter` are responsible for writing into the memory that the binary data classes point to.
None of these components are easily changeable for users. Therefore, user extensions for data types in Flink only touch the logical layer. The physical representation of `STRUCTURE` types is determined by the system, based on the field types used.

#### Apache Beam
Beam offers users to map a user-facing type of the language of the SDK to an underlying Beam Schema Type in form of a `LogicalType`.
Notably, Beam has a `Row` schema type, which is comparable to Flink's `ROW` type. Composite type definitions with unique identifier are therefore possible.
A `LogicalType` needs to define:
- Unique identifier of the data type
- Input Type: User-facing representation type of the logical type. Values are converted from this type into the base type.
- Base Type: Beam schema type, which serves as the underlying type of the logical type.
- BaseType to InputType conversion function toBaseType()
- InputType to BaseType conversion function toInputType()
- (Optional) Type parameters. These can be used to give additional properties to a single logical type, like scale and precision of a decimal type.

`LogicalType` can therefore be described as a user-defined mapping between a type of the API and a supported Beam-Schema-Type. 
In contrast to our data type plugins, the user-defined type does not need to map to the struct / row type of the engine. 
Unlike Beam, we currently do not support the definition of type parameters for our data types, but it has some applictions like for precision or timezone definition (the Timestamp plugin currently does not consider offsets / timezones).
`LogicalType` allows customizability of the logical type layer. The in-memory representation of the underlying Beam-Schema-Type is controlled by Beam runtime and not alterable for the user.

#### Apache Spark
Similar to Beam, Spark allows to create a `UserDefinedType` for a class X of the , which maps X to an underlying SQL type.
The `UserDefinedType` must define serialize() and deserialize() functions to convert values from X to the SQL type and vice versa.
Therefore, this user-defined type approach does not allow to customize physical in-memory representation of the new type.
Unlike Beam, Spark does not include support for type parameters.
Spark also supports the `STRUCT` type, which consists of named and typed fields. However, the `STRUCT` itself is not named, so the system cannot distinguish two `STRUCT` types with the same fields.

#### PostgreSQL
Unlike most other systems, PostgreSQL already offers a very large selection of implemented types. Notable are geospatial types like `path`, `point`, `circle`, and `polygon`.
PostgresSQL allows to create `enum` types with unique identifiers.
Furthermore, a `composite` type exists, which allows to define a type with identifier, consisting out of multiple named and typed fields. The creation of such a type is similar to the creation of a table.
The `domain` type allows users to create a named type based on any underlying data type (this could potentially be another user-defined type). Additionally, the user can restrict the value range of the domain type during the definition: 
```sql
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE TABLE mytable (id posint);
INSERT INTO mytable VALUES(1); -- works
INSERT INTO mytable VALUES(-1); -- fails
```
Domain types are automatically down-cast to the underlying type when applying a function of the underlying type.

Besides these fully implemented data types, PostgreSQL also offers users to create entirely new base types, known as `User-Defined Types`.
The user needs to define the following properties inside a library:
- In-memory representation (for example as C struct)
- input function, receives a string of the value and converts it into the in-memory representation
- output function, receives the in-memory representation of the value and converts it into a string
- (optional) receive function, receives a binary representation of a value and converts it into the in-memory representation
- (optional) send function, receives the in-memory representation of the value and converts it into the external binary format

For variable-sized types, PostgreSQL demands that the first 4 bytes of the internal representation describe the length of the whole datum.
These functions can then be used in the SQL definition of the data type:
```c++
typedef struct Complex {
double x;
double y;
} Complex;
```

```sql
CREATE TYPE complex (
internallength = 16,
input = complex_in,
output = complex_out,
receive = complex_recv,
send = complex_send,
alignment = double
);
```
`User-defined Functions` may take `User-defined Types` as argument or return them.

This approach is mightier than our PoC, as it gives the user full control over the object used to represent the data type and therefore over the physical representation.
The approach of defining input, output, receive, and send functions is very similar to our concept of registering several `Serde` for data type plugins.

#### DuckDB
DuckDB supports the logical `STRUCT` data type, which contains potentially nested fields of a name and a type.
Furthermore, the `UNION` type allows to define multiple types which a value of this column might belong to.
Similar to PostgreSQL, DuckDB enables users to define `ENUM` types.
Extending the data types can be done via extensions consisting of additional c++ code. However, there is no dedicated SQL interface for this like in PostgreSQL.

#### Apache Arrow
Apache Arrow's `DataType` class has the `ExtensionType` subclass.
Users can implement subclasses of `ExtensionType` to create new logical types.
Users need to define:
- the extension name, which identifies the type
- the underlying physical type, which defines how the values are interpreted by the system. This can be any of the supported datatypes (`INT32`, `FIXED_SIZE_BINARY`, `LIST`, `STRUCT`...)
- (optional) parameters of the data type. These can add additional properties to a datatype without defining an entirely new datatype. For example, this could be a `freq` parameter for a `Period` data type.
- serialize and deserialize functions to convert the data type (meaning the parameters) to a string and construct the data type from a string.

This type of extension is similar to what Apache Beam offers for their `LogicalType` extensions.

# Summary
The PoC adds three extensible variants (`FIXEDSIZED`, `VECTOR`, `STRUCT`) to `DataType`, a generic SQL `T(...)` constructor pipeline, and multiple plugins for several real-life use cases that demonstrate the full path from registration to physical execution.
P1 is addressed by routing all type-specific logic through the existing registry pattern and through generic `Construct`/`Cast` functions instead of per-type switch cases.
P2 is addressed by `STRUCT` with nominal typing.                 
P3 is addressed by the generic `exitFunctionCall` parser dispatch.
P4 is addressed by the addition of the `FIXEDSIZED` and `VECTOR` types.

# Open Questions
- OQ1: Do we want to evolve `DataType` itself to a polymorphic representation (NG1)? Or do we wait until there is a clear need? In our opinion, such a refactor would warrant its own DD that then also covers a full-blown type system.
- OQ2: Do we want to refactor the memory layout / buffer ref before implementing fixed-sized and struct as variants? While this is the better solution, a memory layout / buffer ref refactor that can handle compound/struct data types is a major undertaking. Additionally, I'd rather work on that refactor with already existing compound/struct data types that can demonstrate the effectiveness. 
- OQ3: (Possibly worth a separate design doc): In the case that we replace UINT64 with the `Timestamp` datatype plugin as input type for our watermarks: Do we want to switch to signed integers to represent the timestamps internally? This allows us to capture points int time before `1970-01-01 00:00:00`. The change would impact multiple files responsible for window slices and aggregation/join build.
