# General Information
The PoC branch attached to this DD is not a feature branch in disguise.
Its sole purpose is to give you a concrete, interactive vision of what I am aiming for.
I plan to branch off from the currently ongoing schema inference branch and then work towards the vision, demonstrated in the PoC in small increments, issue by issue. 
The idea/hope is that the (smallish) issues then make sense to you on a higher level.
As a result there are at least three major simplifications that the PoC makes use of:
1. builds on top of current main and not on top of the schema inference branch
2. keeps simplistic datatype frontend/logical implementation
3. does not dive into a memorylayout-/bufferref-based implementation for custom/new datatypes, which is probably the clean way, but requires a significant refactor of bufferrefs

# The Problem
NebulaStream's `DataType` is a closed C++ enum with a fixed set of variants (numeric, `VARSIZED`, ...).
Adding a new logical type means editing core code at every switch site: parsers, schema printer, sinks, JSON/CSV formatters, Nautilus runtime, generator source, etc.
There is no way to register a new type from `nes-plugins/`, although sources, sinks, and input/output formatters already follow that pattern.

In particular, real-world ingest scenarios need *composite* types whose layout is fixed but bigger than a scalar, e.g. a thermal-camera reading `Image{timestamp, cameraId, ThermalFrame{pixels: UINT16[N]}}`.
Today the only way to model this is to flatten everything into top-level columns, which loses the type identity that makes domain-specific functions (`to_celsius`, `is_fever`, `to_rgb`) safe to register.

- P1: `DataType` cannot be extended without modifying core code paths spread across ~20 files.
- P2: There is no first-class composite type — no way to represent a named struct of fields and refer to it by name in SQL or in physical functions.
- P3: There is no way to add SQL constructor syntax (`Image(...)`) for a new type without per-type parser changes. The casting/construction surface is hard-coded.
- P4: There is no vector- or array-like container type that is type-specific. VarSized is essentially a string/byte-vector.

# Goals
- G1: A new `DataType` can be added from `nes-plugins/` using the same registration pattern that sources, sinks, and formatters already use, addressing P1.
- G2: Support a composite `STRUCT` variant with a name and a field list that participates in schemas, parsers, formatters, and Nautilus values, addressing P2.
- G3: Provide a generic SQL constructor syntax `T(arg1, arg2, ...)` for any registered type, dispatched in the parser without per-type code, addressing P3.
- G4: Demonstrate G1–G3 end-to-end with example plugins that fit on top of the core without modifying it (`Image/ThermalFrame`, `Reading`).
- G5: Create a fixed-sized datatype that is type-specific, i.e., represents an array of type T.

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
- `nes-plugins/DataTypes/Image/`: registers `Image` and `ThermalFrame`, plus `to_celsius`, `is_fever`, `to_rgb`. Demo at `nes-plugins/DataTypes/Image/demo/run_demo.py` consumes a thermal image and emits a colorized RGB image.
- `nes-plugins/DataTypes/Reading/`: registers `Reading{celsius, humidity_pct}` and an addition function over `Reading`s, demonstrating a second independent struct with arithmetic semantics.
- `nes-plugins/InputFormatters/NestedJSONInputFormatter/`: separate plugin, demonstrates that ingesting nested JSON into composite columns is itself an opt-in plugin.
- Systests under `nes-systests/formatter/JSON_OUTPUT/` (`StructConstruction`, `StructAdd`, `StructWHERE`, `ThermalFrameStructJSON`, `ThermalFrameFunctions`, `ThermalFrameToRGB`, `FixedSizedArrayJSON`) exercise the end-to-end paths.

# Summary
The PoC adds two extensible variants (`FIXEDSIZED`, `STRUCT`) to `DataType`, a generic SQL `T(...)` constructor pipeline, and three plugins that demonstrate the full path from registration to physical execution.
P1 is addressed by routing all type-specific logic through the existing registry pattern and through generic `Construct`/`Cast` functions instead of per-type switch cases.
P2 is addressed by `STRUCT` with nominal typing.
P3 is addressed by the generic `exitFunctionCall` parser dispatch.

# Open Questions
- OQ1: How do we want to evolve `DataType` itself once we have multiple composite variants — keep the flat-struct-with-optional-fields shape, or refactor to a polymorphic representation (NG1)?
