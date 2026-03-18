# Feature Landscape

**Domain:** C++ to WebAssembly validation pipeline for NES topology editor
**Researched:** 2026-03-15
**Milestone:** v1.1 -- Shared Validation via WebAssembly

## Context: What Exists Today vs What the WASM Module Adds

The v1.0 topology editor already has two validation layers implemented in TypeScript:

1. **Topology validation** (`src/lib/validation.ts`): Structural checks -- orphan nodes, missing logical sources, empty schemas, missing required config fields. These are client-side heuristics that approximate NES behavior.

2. **SQL syntax validation** (`src/lib/sql/validateSql.ts`): ANTLR4 parse using a TypeScript port of the NES grammar. Catches syntax errors only -- misspelled keywords, malformed clauses. No semantic checking whatsoever.

The gap is everything the NES engine does AFTER parsing: statement binding against a source catalog, schema inference, type inference, expression type checking, and plan construction. A query like `SELECT nonexistent_field FROM sensor` passes the TypeScript SQL validator but would fail in NES. This is the validation the WASM module must provide.

## Table Stakes

Features the WASM module MUST have. Without these, the module adds no value over existing TypeScript validation.

### Core Validation Capabilities

| Feature | Why Expected | Complexity | C++ Location |
|---------|--------------|------------|-------------|
| SQL parsing (ANTLR) | Foundation of all query validation; replaces the TS ANTLR port | Med | `nes-sql-parser/src/AntlrSQLQueryParser.cpp`, `AntlrSQL.g4` |
| Statement binding | Bind parsed AST to catalog -- resolve source names, validate CREATE/DROP/QUERY statements | Med | `nes-sql-parser/src/StatementBinder.cpp` (PIMPL `Impl::bind()`) |
| Source catalog population | Load logical/physical sources from YAML so queries can reference them | Med | `nes-sources/include/Sources/SourceCatalog.hpp` -- `addLogicalSource()`, `addPhysicalSource()` |
| Sink catalog population | Load sinks from YAML so INTO clauses can resolve | Low | `nes-sinks/include/Sinks/SinkCatalog.hpp` |
| Schema inference | Propagate schemas from source through operators to sink; detect unknown field references | High | `nes-query-optimizer/src/LegacyOptimizer/TypeInferencePhase.cpp` -- `propagateSchema()` bottom-up traversal |
| Type inference for expressions | Check that `WHERE a > 10` is valid when `a` is UINT64, not when `a` is a string; catch type mismatches in arithmetic/comparison/boolean expressions | High | `TypeInferencePhase::apply()` calls `withInferredSchema()` on each operator, which validates expression types against input schemas |
| Structured error reporting | Return errors as JSON with error type, message, and location info (line/column from ANTLR, or operator name) | Med | Currently uses `std::expected<T, Exception>` pattern throughout; need to serialize Exception to JSON |
| YAML input parsing | Accept YAML topology string, parse into internal catalog state | Med | `yaml-cpp` is already a dependency; topology YAML format defined in `docs/nebulastream-frontend.md` |

### Narrow Interface (YAML string in, errors out)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Single entry point function | `validate(yamlString) -> JSON errors[]` -- minimal API surface per PROJECT.md decision | Low | Emscripten `EMSCRIPTEN_BINDINGS` or `extern "C"` exported function |
| Stateless validation | Each call is independent -- no persistent state between invocations | Low | Construct catalogs from YAML, validate, destroy. Simplifies WASM lifecycle |
| Error categorization | Distinguish parse errors, binding errors, type errors, topology errors | Med | Map NES exception types (`InvalidQuerySyntax`, `UnknownSourceName`, `TypeInferenceException`) to error categories |

## Differentiators

Features that go beyond minimum viability. Valuable but not blocking.

### High Value, Moderate Effort

| Feature | Value Proposition | Complexity | Dependencies |
|---------|-------------------|------------|-------------|
| Source validation via plugin registry | Validate source-specific config (e.g., Generator source requires `generator_schema`, TCP source requires `host`/`port`) using NES `SourceValidationProvider` | Med | Depends on: `nes-sources/src/SourceValidationProvider.cpp`, `SourceValidationRegistry` -- needs static registration of validators without dynamic plugin loading |
| Logical source expansion validation | Verify that physical sources exist for each logical source referenced in queries (the `LogicalSourceExpansionRule` checks this) | Med | Depends on: `nes-query-optimizer/private/LegacyOptimizer/LogicalSourceExpansionRule.hpp` |
| Expression function validation | Validate that functions used in queries (ABS, CEIL, SQRT, CONCAT, etc.) are applied to compatible types | Med | Depends on: `nes-logical-operators/include/Functions/` -- each function defines `withInferredSchema()` |
| Window/aggregation validation | Validate windowed aggregation queries (tumbling/sliding windows, GROUP BY, aggregation functions like SUM/AVG/COUNT/MIN/MAX/MEDIAN) | High | Depends on: `nes-logical-operators/include/Operators/Windows/` and aggregation functions |
| EXPLAIN query output | Return the logical plan as a string (like NES `EXPLAIN` statement) so the UI can show users what the engine will execute | Low | Already exists: `explain(plan, verbosity)` in `Plans/LogicalPlan.hpp` |

### Medium Value, Lower Effort

| Feature | Value Proposition | Complexity | Dependencies |
|---------|-------------------|------------|-------------|
| Multi-query validation | Validate all queries in the topology in a single call, not one-at-a-time | Low | `StatementBinder::parseAndBind()` already handles multiple statements |
| Error location mapping | Return ANTLR line/column for SQL errors so the UI can highlight the exact position in the Monaco editor | Low | ANTLR error listener already captures line/column; need to propagate through binding |
| Schema output for valid queries | For valid queries, return the inferred output schema so the UI can auto-populate sink schemas | Med | Available from `LogicalOperator::getOutputSchema()` after type inference |
| Data type enumeration | Expose the list of valid NES data types (UINT8, UINT16, UINT32, UINT64, INT8, ..., FLOAT32, FLOAT64, BOOLEAN, TEXT) for UI dropdowns | Low | `nes-data-types/include/DataTypes/DataType.hpp` |

### Medium Value, Higher Effort

| Feature | Value Proposition | Complexity | Dependencies |
|---------|-------------------|------------|-------------|
| Redundant operator detection | Report when a projection selects all fields (redundant) or a union has only one input | Med | `RedundantProjectionRemovalRule`, `RedundantUnionRemovalRule` in optimizer |
| Query plan visualization data | Return the logical plan as structured data (not just string) so the UI could render a query DAG | High | Requires serialization of `LogicalPlan` operator tree to JSON |
| Incremental validation | Cache catalog state between calls so only changed queries need re-validation | Med | Breaks stateless design; adds complexity for marginal performance gain |

## Anti-Features

Features to explicitly NOT build in the WASM module.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Query optimization (operator placement, decomposition) | `BottomUpPlacement` and `QueryDecomposer` depend on `WorkerCatalog` with live cluster state (node addresses, capacities, connectivity). This is runtime concern, not validation | Stop after `TypeInferencePhase`. The LegacyOptimizer steps after type inference (placement, decomposition) are deployment-time |
| Physical operator generation | `nes-physical-operators` contains runtime execution semantics (hashmap aggregation, join strategies). Nothing to validate here | Only include `nes-logical-operators` |
| Code compilation / Nautilus | Query compilation to machine code via MLIR/Nautilus is execution, not validation | Exclude `nes-query-compiler`, `nes-executable`, `nes-nautilus` entirely |
| Source/sink instantiation | Actually creating source/sink instances (TCP connections, file handles) is runtime | Only validate descriptors and configs |
| gRPC / networking | All distributed communication is irrelevant for offline validation | Exclude `grpc/`, `nes-frontend` gRPC backend, `nes-single-node-worker` |
| Buffer management / execution engine | `nes-memory`, `nes-runtime`, `nes-query-engine` are execution infrastructure | Exclude entirely |
| Dynamic plugin loading | `SourceValidationRegistry` uses dynamic library loading which is not available in WASM | Statically link known source/sink validators at compile time |
| Topology structure validation | Orphan nodes, missing host assignments, worker connectivity checks | Keep in TypeScript (`src/lib/validation.ts`). These are topology-level, not query-level. The WASM module validates queries against a catalog, not the topology graph |
| Live config field validation | Required field checking for source/sink configs (e.g., "File sink needs file_path") | Keep in TypeScript. Source/sink config schemas are already defined in `sourceConfigs.ts`. WASM module validates at a higher level via `SourceValidationProvider` |

## Feature Dependencies

```
YAML Parsing --> Source Catalog Population (parse YAML to populate catalogs)
YAML Parsing --> Sink Catalog Population
Source Catalog Population --> Statement Binding (binder needs catalog to resolve source names)
Sink Catalog Population --> Statement Binding (binder needs sink catalog for INTO clauses)
ANTLR SQL Parsing --> Statement Binding (binder consumes parsed AST)
Statement Binding --> Logical Plan Construction (binding produces LogicalPlan)
Logical Plan Construction --> Source Inference (resolve source schemas)
Source Inference --> Type Inference / Schema Propagation (needs source schemas to propagate)
Type Inference --> Expression Function Validation (expression checking happens during type inference)
Type Inference --> Window/Aggregation Validation (window operator schema inference)

Source Validation Provider --> Source Catalog Population (validates config during catalog creation)
Multi-query Validation --> Statement Binding (iterates over multiple statements)
EXPLAIN Output --> Logical Plan Construction (calls explain() on LogicalPlan)
Schema Output --> Type Inference (reads output schema after inference)
Error Location Mapping --> ANTLR SQL Parsing (line/column from ANTLR errors)
```

## NES Validation Pipeline: Stages and What Each Catches

Based on analysis of the actual C++ codebase, the NES validation pipeline has these distinct stages. The WASM module must replicate stages 1-6. Stages 7+ are deployment/runtime and out of scope.

### Stage 1: ANTLR Parsing
- **Location:** `nes-sql-parser/src/AntlrSQLQueryParser.cpp`, `AntlrSQL.g4`
- **Catches:** Syntax errors -- misspelled keywords, malformed expressions, unclosed parentheses
- **Currently in TS:** Yes (TypeScript ANTLR port)
- **WASM replaces:** Yes, with authoritative C++ ANTLR runtime

### Stage 2: Statement Binding
- **Location:** `nes-sql-parser/src/StatementBinder.cpp`
- **Catches:** Unrecognized statement types, invalid filter attributes (e.g., `DROP SOURCE WHERE BADATTR = ...`), type mismatches in config values
- **Currently in TS:** No
- **WASM adds:** Yes -- semantic validation of all NES statement types

### Stage 3: Query Plan Binding (for SELECT queries)
- **Location:** `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp`
- **Catches:** Unknown aggregate functions, invalid window specifications, malformed JOIN conditions, invalid expression nesting, unknown function names
- **Currently in TS:** No
- **WASM adds:** Yes -- the most complex validation step, 800+ lines of plan construction with validation

### Stage 4: Source Inference
- **Location:** `nes-query-optimizer/private/LegacyOptimizer/SourceInferencePhase.hpp`
- **Catches:** `LogicalSourceNotFoundInQueryDescription` -- query references a source name not in the catalog
- **Currently in TS:** No (TS validates source existence in topology, not in query SQL)
- **WASM adds:** Yes -- cross-validates SQL source references against catalog

### Stage 5: Logical Source Expansion
- **Location:** `nes-query-optimizer/private/LegacyOptimizer/LogicalSourceExpansionRule.hpp`
- **Catches:** Logical source with no physical sources attached (empty expansion)
- **Currently in TS:** No
- **WASM adds:** Yes (differentiator feature)

### Stage 6: Type Inference / Schema Propagation
- **Location:** `nes-query-optimizer/src/LegacyOptimizer/TypeInferencePhase.cpp`
- **Catches:** Type mismatches in expressions (comparing incompatible types), unknown field references in WHERE/SELECT/GROUP BY, schema mismatch in UNION operands, invalid aggregation input types
- **Currently in TS:** No
- **WASM adds:** Yes -- this is the highest-value validation the WASM module provides

### Stage 7: Operator Placement (OUT OF SCOPE)
- **Location:** `nes-query-optimizer/private/LegacyOptimizer/BottomUpPlacement.hpp`
- **Why excluded:** Requires live worker catalog with node addresses and capacities

### Stage 8: Query Decomposition (OUT OF SCOPE)
- **Location:** `nes-query-optimizer/private/LegacyOptimizer/QueryDecomposition.hpp`
- **Why excluded:** Splits query across workers -- deployment concern, not validation

## MVP Recommendation

### Must Have (blocks v1.1 release)

1. **YAML parsing + catalog population** -- Foundation. Without this, nothing works.
2. **ANTLR SQL parsing** -- Replaces TypeScript port with authoritative C++ implementation.
3. **Statement binding** -- Semantic validation of all statement types.
4. **Query plan construction** -- Build logical plans from SQL, catching unknown functions/operators.
5. **Source/sink inference** -- Resolve source and sink names against catalog.
6. **Type inference / schema propagation** -- The key value-add: catch field/type errors.
7. **Structured error reporting** -- Return errors as JSON the UI can render.
8. **Single-function WASM interface** -- `validate(yaml) -> errors[]`.

### Should Have (high value, include if feasible)

9. **EXPLAIN output** -- Low effort, high value for debugging.
10. **Error location mapping** -- ANTLR line/column for SQL errors.
11. **Multi-query validation** -- Already supported by `parseAndBind()`.
12. **Schema output for valid queries** -- Auto-populate sink schemas.

### Defer (to v1.2+)

13. **Source-specific config validation** -- Requires static plugin registration work.
14. **Query plan visualization data** -- Structured plan export for UI rendering.
15. **Redundant operator detection** -- Nice-to-have optimization hints.
16. **Incremental validation** -- Only if validation proves too slow (unlikely for typical topologies).

## C++ Modules Required for WASM Compilation

Based on the validation pipeline analysis, these NES modules must be included:

| Module | Why Needed | External Dependencies |
|--------|-----------|----------------------|
| `nes-sql-parser` | ANTLR parsing + statement binding | `antlr4-runtime` |
| `nes-logical-operators` | Operator types, logical plan, schema propagation | `nes-data-types` |
| `nes-data-types` | Type system, Schema | None (foundational) |
| `nes-sources` | SourceCatalog, LogicalSource, SourceDescriptor | `nes-data-types`, `nes-configurations` |
| `nes-sinks` | SinkCatalog, SinkDescriptor | `nes-data-types`, `nes-configurations` |
| `nes-configurations` | Descriptor config types | None |
| `nes-common` | Identifiers, logging stubs | None |
| `nes-query-optimizer` (partial) | TypeInferencePhase, SourceInferencePhase, LogicalSourceExpansionRule only | `nes-logical-operators`, `nes-sources` |

**Explicitly excluded:** `nes-physical-operators`, `nes-query-compiler`, `nes-query-engine`, `nes-runtime`, `nes-executable`, `nes-nautilus`, `nes-memory`, `nes-frontend` (except types), `nes-single-node-worker`, `grpc/`.

## Complexity Budget Estimate

| Component | Estimated Effort | Risk | Notes |
|-----------|-----------------|------|-------|
| Extract validation library (CMake target) | High | High | Untangling dependencies from runtime is the hardest part. Headers include chains may pull in unexpected deps |
| Emscripten compilation | Med | Med | ANTLR4 C++ runtime has been compiled to WASM before. `yaml-cpp` compiles cleanly. Unknown: `folly`, `fmt`, `spdlog` transitive deps |
| WASM interface wrapper | Low | Low | Thin C function: parse YAML, populate catalogs, run validation, serialize errors to JSON |
| UI integration (TypeScript) | Med | Low | Replace `validateSql()` calls with WASM module calls. Handle async loading of WASM binary |
| Testing | Med | Low | Port existing TypeScript validation tests. Add tests for semantic errors that TS currently misses |

## Sources

- NES codebase analysis: `nes-sql-parser/`, `nes-query-optimizer/`, `nes-logical-operators/`, `nes-sources/`, `nes-sinks/` (HIGH confidence -- direct code reading)
- NES frontend documentation: `docs/nebulastream-frontend.md` (HIGH confidence -- authoritative)
- Existing topology editor: `nes-topology-editor/src/lib/validation.ts`, `src/lib/sql/validateSql.ts` (HIGH confidence -- direct code reading)
- PROJECT.md milestone definition (HIGH confidence -- project specification)
