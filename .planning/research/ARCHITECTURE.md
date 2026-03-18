# Architecture Patterns

**Domain:** NES Validation Pipeline Extraction to WebAssembly
**Researched:** 2026-03-15

## Current Validation Pipeline (As-Is)

The NES validation pipeline is distributed across several modules. The flow when `nes-cli` processes a YAML topology file is:

```
YAML file
  |  (yaml-cpp parsing in CLIStarter.cpp)
  v
QueryConfig struct (workers, logical sources, physical sources, sinks, queries)
  |  (loadStatements() converts to Statement variants)
  v
Statement[] (CreateWorkerStatement, CreateLogicalSourceStatement, etc.)
  |  (handleStatements() dispatches to handlers)
  v
Catalog Population (SourceCatalog, SinkCatalog, WorkerCatalog)
  |
  v
SQL Query String
  |  (AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString)
  v
LogicalPlan (operator DAG: source -> operators -> sink)
  |  (LegacyOptimizer::optimize)
  v
Validated/Optimized Plan
```

### What "Validation" Actually Is

There is no single "validate" function. Validation happens as side effects across multiple phases:

1. **YAML Parsing** -- yaml-cpp deserializes into typed structs. Invalid YAML throws `YAML::Exception`. Invalid field types throw `SLTWrongSchema`.

2. **Identifier Binding** -- `bindIdentifierName()` in CLIStarter.cpp validates characters (alphanumeric, underscore, dollar), applies case normalization.

3. **Catalog Population** -- `SourceStatementHandler`, `SinkStatementHandler`, `TopologyStatementHandler` register entries. Duplicate sources throw `SourceAlreadyExists`. Invalid configs throw `InvalidConfigParameter`.

4. **SQL Parsing** -- ANTLR parses SQL into AST. Syntax errors are caught by ANTLR error listeners.

5. **Statement Binding** -- `StatementBinder::parseAndBind()` resolves source references against the `SourceCatalog`, binding `SourceNameLogicalOperator` nodes.

6. **Source Inference** -- `SourceInferencePhase` (in `nes-query-optimizer/src/LegacyOptimizer/`) resolves logical source names to schemas from the catalog. Throws `UnknownSourceName` if not registered.

7. **Type Inference** -- `TypeInferencePhase` propagates schemas bottom-up through the operator DAG. Each operator's `withInferredSchema()` validates field references, type compatibility, and produces output schemas. This is where most semantic validation happens (e.g., filtering on non-existent fields, type mismatches).

8. **Optimization and Placement** (NOT validation) -- `LegacyOptimizer` runs `BottomUpOperatorPlacer` and `QueryDecomposer` for distributed placement. This is NOT needed for validation.

### The Validation Boundary

For the WASM module, we need steps 1-7. Step 8 (distributed placement/decomposition) is NOT needed and is exactly what pulls in the heavy runtime dependencies (WorkerCatalog with NetworkTopology, gRPC, etc.).

The critical insight: **The LegacyOptimizer conflates validation with distributed placement.** The validation-relevant phases (source inference, type inference, redundant operator removal) are interleaved with placement phases in `LegacyOptimizer::optimize()`.

### Key Source Files in the Validation Path

| File | Role in Validation |
|------|-------------------|
| `nes-frontend/apps/cli/CLIStarter.cpp` | YAML parsing, catalog population, query loading |
| `nes-sql-parser/src/StatementBinder.cpp` | SQL text to Statement binding, source resolution |
| `nes-sql-parser/src/AntlrSQLQueryParser.cpp` | SQL string to LogicalPlan |
| `nes-query-optimizer/src/LegacyOptimizer/SourceInferencePhase.cpp` | Resolves source names to schemas from catalog |
| `nes-query-optimizer/src/LegacyOptimizer/TypeInferencePhase.cpp` | Propagates schemas through operator DAG |
| `nes-query-optimizer/src/LegacyOptimizer/InlineSourceBindingPhase.cpp` | Inlines source definitions |
| `nes-query-optimizer/src/LegacyOptimizer/InlineSinkBindingPhase.cpp` | Inlines sink definitions |
| `nes-query-optimizer/src/LegacyOptimizer/SinkBindingRule.cpp` | Binds sink references |
| `nes-query-optimizer/src/LegacyOptimizer/LogicalSourceExpansionRule.cpp` | Expands logical sources |
| `nes-query-optimizer/src/LegacyOptimizer/RedundantUnionRemovalRule.cpp` | Simplifies plan |
| `nes-query-optimizer/src/LegacyOptimizer/RedundantProjectionRemovalRule.cpp` | Simplifies plan |
| `nes-query-optimizer/src/LegacyOptimizer/OriginIdInferencePhase.cpp` | Assigns origin IDs |
| `nes-sources/include/Sources/SourceCatalog.hpp` | Source registration and lookup |
| `nes-sinks/include/Sinks/SinkCatalog.hpp` | Sink registration and lookup |

## Recommended Architecture

### New Component: `nes-validator`

A new CMake library target that extracts the validation-relevant subset of the pipeline.

```
nes-validator/
  include/
    Validator/
      TopologyValidator.hpp       -- Public API
      ValidationResult.hpp        -- Error/warning types
  src/
    TopologyValidator.cpp         -- Implementation
    YamlTopologyParser.cpp        -- YAML -> catalog + queries
    ValidationPipeline.cpp        -- Source inference + type inference
  wasm/
    validator_wasm_binding.cpp    -- Emscripten bindings (embind)
    CMakeLists.txt                -- Emscripten-specific build
  CMakeLists.txt
```

### Public API (Narrow Interface)

```cpp
namespace NES::Validator
{

struct ValidationError
{
    enum class Severity { Warning, Error };
    Severity severity;
    std::string message;
    std::string location;     // e.g., "query[0]", "source.MY_SOURCE", "worker.host1"
};

struct ValidationResult
{
    bool valid;
    std::vector<ValidationError> errors;
};

/// The single entry point: YAML string in, validation result out.
/// No pointers, no shared state, no catalogs to manage.
ValidationResult validateTopology(std::string_view yamlContent);

} // namespace NES::Validator
```

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| `nes-validator` (NEW) | Standalone validation library. Parses YAML, populates in-memory catalogs, parses SQL, runs type inference. Returns structured errors. | Uses: `nes-sql-parser`, `nes-logical-operators`, `nes-sources` (catalog only), `nes-sinks` (catalog only), `nes-data-types` |
| `nes-validator-wasm` (NEW build target) | Emscripten binding layer. Exposes `validateTopology()` as a JS-callable function via embind. Compiles `nes-validator` to WASM. | Uses: `nes-validator` |
| `nes-topology-editor` (MODIFIED) | React app. Loads WASM module, calls `validateTopology()` with YAML string, renders errors as overlays. | Uses: WASM output files (.wasm + .js glue) |

### Data Flow

```
[React Topology Editor]
        |
        | (1) Generate YAML string from UI state (existing serializeTopology())
        v
[Web Worker]
        |
        | (2) postMessage(yamlString)
        v
[WASM Module: nes-validator-wasm]
        |
        | (3) Call validateTopology(yamlString)
        v
[nes-validator: YamlTopologyParser]
        |
        | (4) Parse YAML with yaml-cpp
        | (5) Populate SourceCatalog, SinkCatalog with parsed entries
        | (6) Extract SQL query strings
        v
[nes-validator: ValidationPipeline]
        |
        | (7) For each query:
        |     a. AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString()
        |     b. SourceInferencePhase::apply() -- resolve sources from catalog
        |     c. TypeInferencePhase::apply() -- propagate and validate schemas
        | (8) Catch all NES exceptions, convert to ValidationError[]
        v
[ValidationResult { valid, errors[] }]
        |
        | (9) Serialize to JSON string via embind
        v
[Web Worker: postMessage(resultJson)]
        |
        v
[React: Parse JSON, display errors as overlays on nodes]
```

## Dependency Graph Analysis

### Current CMake Dependency Chain (Validation-Relevant)

```
nes-common
  PUBLIC deps: nes-grpc, cpptrace, fmt, libcuckoo, magic_enum, spdlog,
               yaml-cpp, folly, Boost::url, PkgConfig::uuid

nes-configurations
  PUBLIC deps: nes-common

nes-data-types
  PUBLIC deps: nes-configurations
  PRIVATE deps: reflectcpp

nes-memory
  PUBLIC deps: nes-common, nes-data-types
  PRIVATE deps: folly

nes-executable
  PUBLIC deps: nes-common, nes-runtime  <-- PROBLEM: pulls entire runtime

nes-sources
  PUBLIC deps: nes-common, nes-configurations, nes-memory, nes-executable

nes-sinks
  PUBLIC deps: nes-common, nes-configurations, nes-data-types, nes-memory, nes-executable

nes-logical-operators
  PUBLIC deps: nes-sources, nes-sinks, nes-query-optimizer, nes-data-types

nes-sql-parser
  PUBLIC deps: antlr4_static, nes-common, nes-sinks, nes-sources, nes-logical-operators
  PRIVATE deps: nes-input-formatters

nes-query-optimizer
  PUBLIC deps: nes-configurations, nes-logical-operators, nes-single-node-worker-interface,
               nes-distributed
  PRIVATE deps: nes-input-formatter-provider, highs
```

### Why This Cannot Be Used As-Is for WASM

1. **`nes-common` depends on `nes-grpc`** -- gRPC generates protobuf stubs and links gRPC libraries. These are heavy, threading-intensive, and do not compile to WASM.

2. **`nes-executable` depends on `nes-runtime`** -- This pulls in NodeEngine, pipelines, Nautilus code generation.

3. **`nes-sources` and `nes-sinks` link `nes-memory` and `nes-executable`** -- The catalog classes we need are bundled with runtime provider/implementation classes we do not need.

4. **`nes-sinks` uses `folly::Synchronized`** -- Folly is a large dependency with pthread-heavy internals.

5. **`nes-query-optimizer` depends on `nes-distributed` and `nes-single-node-worker-interface`** -- These pull in networking, gRPC service definitions, and distributed execution concepts.

6. **Circular dependency: `nes-logical-operators` <-> `nes-query-optimizer`** -- Logical operators depend on the optimizer's trait system (`Trait.hpp`, `TraitSet.hpp`), and the optimizer depends on logical operators.

## Isolation Strategy

### What We Need from Each Module

| Module | What We Need | What We Must Exclude |
|--------|-------------|---------------------|
| `nes-common` | Identifiers, ErrorHandling, fmt formatting, Schema utilities | gRPC, protobuf, folly, uuid, libcuckoo, Boost::url, cpptrace, spdlog (file I/O) |
| `nes-configurations` | Descriptor configuration types | Nothing problematic (only depends on nes-common) |
| `nes-data-types` | DataType hierarchy, Schema, DataTypeProvider | reflectcpp (verify WASM compat) |
| `nes-sources` | SourceCatalog, LogicalSource, SourceDescriptor | Source implementations, providers, nes-memory, nes-executable |
| `nes-sinks` | SinkCatalog, SinkDescriptor | Sink implementations, folly::Synchronized |
| `nes-logical-operators` | All logical operators, LogicalPlan, LogicalPlanBuilder | None (all needed) |
| `nes-sql-parser` | AntlrSQLQueryParser, StatementBinder | nes-input-formatters (PRIVATE dep, can be excluded) |
| `nes-query-optimizer` | TypeInferencePhase, SourceInferencePhase, plus 6 other validation phases | LegacyOptimizer class itself, BottomUpPlacement, QueryDecomposition, DecideJoinTypes, DecideMemoryLayout, highs |

### Recommended Approach: Phased Refactoring

**Phase 1: Proof-of-Concept (Source File Inclusion)**

Create `nes-validator` with its own `CMakeLists.txt` that directly includes specific `.cpp` files from existing modules via `target_sources`. Provide stubs for problematic dependencies.

```cmake
# nes-validator/CMakeLists.txt (Phase 1 -- proof-of-concept)
add_library(nes-validator STATIC
    src/TopologyValidator.cpp
    src/YamlTopologyParser.cpp
    src/ValidationPipeline.cpp
    # Stubs for WASM-incompatible deps
    src/stubs/LoggerStub.cpp
    src/stubs/GrpcStub.cpp
)

# Include source files from existing modules
target_sources(nes-validator PRIVATE
    ${PROJECT_SOURCE_DIR}/nes-sources/src/SourceCatalog.cpp
    ${PROJECT_SOURCE_DIR}/nes-sources/src/LogicalSource.cpp
    # ... etc for each needed file
)

# Link only WASM-compatible dependencies
target_link_libraries(nes-validator
    PRIVATE antlr4_static yaml-cpp::yaml-cpp fmt::fmt magic_enum::magic_enum
)
```

**Phase 2: Proper Module Splitting (Coordinate with Colleague)**

After the colleague separates validation from optimization:

```
nes-common-core (NEW -- split from nes-common)
  deps: fmt, magic_enum
  content: Identifiers, ErrorHandling, Logger interface

nes-source-catalog (NEW -- split from nes-sources)
  deps: nes-common-core, nes-configurations, nes-data-types
  content: SourceCatalog, LogicalSource, SourceDescriptor

nes-sink-catalog (NEW -- split from nes-sinks)
  deps: nes-common-core, nes-configurations, nes-data-types
  content: SinkCatalog, SinkDescriptor

nes-validation-phases (NEW -- split from nes-query-optimizer)
  deps: nes-logical-operators, nes-source-catalog, nes-sink-catalog
  content: TypeInferencePhase, SourceInferencePhase, binding phases

nes-validator
  deps: nes-validation-phases, nes-sql-parser-core, yaml-cpp, antlr4_static
```

## WASM-Specific Concerns

### Dependencies Classification

**Cannot compile to WASM -- must exclude or stub:**

| Dependency | Why Not | Resolution |
|------------|---------|------------|
| gRPC + protobuf | Threading, networking, system calls | Exclude entirely (not needed for validation) |
| folly | pthread-heavy, system-specific allocators | Replace `folly::Synchronized` in SinkCatalog with `std::unordered_map` (single-threaded in WASM) |
| libuuid | System call (getrandom) | Stub with counter-based IDs |
| cpptrace | Stack unwinding, signal handling | Stub with empty implementations |
| spdlog | File I/O, threading | Stub logger: discard output or route to `emscripten_log` |
| libcuckoo | Threading primitives | Exclude (not in validation path) |
| Boost::url | Unnecessary for validation | Exclude |

**Will compile to WASM:**

| Dependency | Notes |
|------------|-------|
| ANTLR4 runtime | Pure C++ parser. Compiles with Emscripten. Needs `-fexceptions` flag. |
| yaml-cpp | Pure C++ YAML parser. Known to compile with Emscripten. |
| fmt | Mostly header-only. Compiles cleanly. |
| magic_enum | Header-only. No issues. |
| reflectcpp | Needs verification. Used by nes-data-types. May need stubs if it uses system features. |

### Emscripten Build Configuration

```cmake
# nes-validator/wasm/CMakeLists.txt
cmake_minimum_required(VERSION 3.21)
project(nes-validator-wasm)

set(CMAKE_CXX_STANDARD 23)

# Emscripten flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} \
  -s WASM=1 \
  -s MODULARIZE=1 \
  -s EXPORT_NAME='NESValidator' \
  -s EXPORTED_RUNTIME_METHODS=['ccall','cwrap'] \
  -s ALLOW_MEMORY_GROWTH=1 \
  -s INITIAL_MEMORY=33554432 \
  --bind \
  -fexceptions \
")

add_executable(nes-validator-wasm
    validator_wasm_binding.cpp
)
target_link_libraries(nes-validator-wasm PRIVATE nes-validator)
```

### Emscripten Binding Layer

```cpp
// validator_wasm_binding.cpp
#include <emscripten/bind.h>
#include <Validator/TopologyValidator.hpp>
#include <string>

using namespace emscripten;

std::string validateTopologyJS(const std::string& yamlContent)
{
    auto result = NES::Validator::validateTopology(yamlContent);
    // Serialize to JSON for JS consumption
    std::string json = "{\"valid\":";
    json += result.valid ? "true" : "false";
    json += ",\"errors\":[";
    for (size_t i = 0; i < result.errors.size(); ++i)
    {
        if (i > 0) json += ",";
        json += "{\"severity\":\"";
        json += result.errors[i].severity == NES::Validator::ValidationError::Severity::Error
            ? "error" : "warning";
        json += "\",\"message\":\"";
        // TODO: proper JSON escaping for message string
        json += result.errors[i].message;
        json += "\",\"location\":\"";
        json += result.errors[i].location;
        json += "\"}";
    }
    json += "]}";
    return json;
}

EMSCRIPTEN_BINDINGS(nes_validator) {
    function("validateTopology", &validateTopologyJS);
}
```

## React Integration Architecture

### Web Worker Pattern (Recommended)

WASM validation should run in a Web Worker to avoid blocking the UI thread:

```typescript
// nes-topology-editor/src/lib/wasm/validatorWorker.ts
let validatorModule: any = null;

self.onmessage = async (event: MessageEvent) => {
    if (event.data.type === 'init') {
        const NESValidator = (await import('./nes-validator.js')).default;
        validatorModule = await NESValidator();
        self.postMessage({ type: 'ready' });
    } else if (event.data.type === 'validate') {
        const resultJson = validatorModule.validateTopology(event.data.yaml);
        self.postMessage({ type: 'result', data: JSON.parse(resultJson) });
    }
};
```

```typescript
// nes-topology-editor/src/lib/wasm/useWasmValidator.ts
export function useWasmValidator() {
    const workerRef = useRef<Worker>();
    const [result, setResult] = useState<WasmValidationResult | null>(null);

    useEffect(() => {
        workerRef.current = new Worker(
            new URL('./validatorWorker.ts', import.meta.url),
            { type: 'module' }
        );
        workerRef.current.postMessage({ type: 'init' });
        workerRef.current.onmessage = (e) => {
            if (e.data.type === 'result') setResult(e.data.data);
        };
        return () => workerRef.current?.terminate();
    }, []);

    const validate = useCallback((yaml: string) => {
        workerRef.current?.postMessage({ type: 'validate', yaml });
    }, []);

    return { validate, result };
}
```

### Composition with Existing Validation

The existing `validation.ts` does structural checks (orphan nodes, missing config fields). The WASM validator adds semantic checks (SQL validity, schema correctness, type compatibility). They compose:

```typescript
// Combined validation: structural (fast, JS) + semantic (WASM)
export function fullValidate(topology: TopologyState): ValidationError[] {
    const structuralErrors = validateTopology(
        topology.workers, topology.logicalSources,
        topology.physicalSources, topology.sinks
    );

    // WASM validation runs async via Web Worker
    const yamlContent = serializeTopology(topology);
    wasmValidator.validate(yamlContent);
    // Results arrive via callback/state update

    return structuralErrors; // semantic errors merged when WASM responds
}
```

## Patterns to Follow

### Pattern 1: Exception-to-Error Translation

**What:** Catch all NES exceptions at the validation boundary and convert to `ValidationError` entries.
**When:** At the boundary between NES internal code and the public `validateTopology()` API.
**Why:** Exceptions in WASM are expensive and cannot propagate to JS. All errors must be data.

```cpp
ValidationResult validateTopology(std::string_view yamlContent)
{
    ValidationResult result{.valid = true};
    try {
        auto config = parseYaml(yamlContent);
        auto [sourceCatalog, sinkCatalog] = populateCatalogs(config);
        for (size_t i = 0; i < config.queries.size(); ++i) {
            try {
                auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.queries[i]);
                SourceInferencePhase{sourceCatalog}.apply(plan);
                TypeInferencePhase{}.apply(plan);
            } catch (const UnknownSourceName& e) {
                result.valid = false;
                result.errors.push_back({Severity::Error, e.what(), fmt::format("query[{}]", i)});
            } catch (const CannotInferSchema& e) {
                result.valid = false;
                result.errors.push_back({Severity::Error, e.what(), fmt::format("query[{}]", i)});
            }
        }
    } catch (const YAML::Exception& e) {
        result.valid = false;
        result.errors.push_back({Severity::Error,
            fmt::format("YAML parse error at line {}: {}", e.mark.line, e.what()), "yaml"});
    }
    return result;
}
```

### Pattern 2: Conditional Compilation for WASM Stubs

**What:** Use `#ifdef __EMSCRIPTEN__` to swap thread-safe implementations for single-threaded ones.
**When:** Any NES code that uses threading primitives (mutex, folly::Synchronized).

```cpp
// In SinkCatalog or a WASM-specific adaptation
#ifdef __EMSCRIPTEN__
    // Single-threaded: no synchronization needed
    std::unordered_map<std::string, SinkDescriptor> sinks;
#else
    folly::Synchronized<std::unordered_map<std::string, SinkDescriptor>> sinks;
#endif
```

### Pattern 3: Debounced Validation Trigger

**What:** Do not call WASM validation on every keystroke. Debounce by 300-500ms.
**When:** User edits topology (adds node, changes property, edits SQL).
**Why:** YAML serialization + WASM validation is not free. Debouncing prevents wasted work.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Copying Source Files

**What:** Copying `.cpp/.hpp` files from NES modules into `nes-validator`.
**Why bad:** Creates maintenance burden. When NES validation logic changes, the copy diverges.
**Instead:** Reference existing source files via CMake `target_sources`, or refactor modules to expose clean sub-targets.

### Anti-Pattern 2: Fat WASM Module

**What:** Including unnecessary modules (runtime, networking, compilation) to avoid refactoring effort.
**Why bad:** WASM modules should be small for fast loading. gRPC/protobuf alone would add megabytes and may not compile at all.
**Instead:** Invest in dependency isolation. Target: under 5MB compressed WASM binary.

### Anti-Pattern 3: Synchronous WASM on Main Thread

**What:** Calling the WASM validation function synchronously on the React main thread.
**Why bad:** Complex topologies with multiple queries could block UI for 100ms+. WASM module initialization alone can take 500ms+.
**Instead:** Use a Web Worker. Initialize WASM once, validate asynchronously.

### Anti-Pattern 4: Reimplementing Validation in TypeScript

**What:** Porting the NES validation logic to TypeScript instead of compiling to WASM.
**Why bad:** Two implementations that must stay in sync forever. TypeScript cannot access ANTLR grammar, type inference rules, or operator semantics without reimplementing them.
**Instead:** WASM is the whole point -- single source of truth for validation.

## Scalability Considerations

| Concern | Single Query | 10 Queries | 100+ Queries |
|---------|-------------|------------|--------------|
| WASM memory | ~2MB | ~5MB | ~20MB (ALLOW_MEMORY_GROWTH handles this) |
| Validation time | <10ms | <100ms | Debounce; consider per-query incremental validation |
| Module load time | ~500ms first load | N/A (loaded once in Worker) | N/A |
| Bundle size | ~2-5MB .wasm (compressed) | Same | Same |

## Build Integration

### How WASM Output Gets into the React App

```
nes-validator/wasm/CMakeLists.txt (Emscripten build)
        |
        | produces: nes-validator.wasm, nes-validator.js
        v
nes-topology-editor/public/wasm/
        |  (copy as build step or symlink)
        v
Vite serves as static assets
        |
        v
Web Worker imports nes-validator.js, loads .wasm
```

The Vite config needs to handle `.wasm` files:
```typescript
// vite.config.ts addition
optimizeDeps: {
    exclude: ['nes-validator']  // Don't try to bundle the WASM
}
```

## Sources

- Direct source code analysis of NES C++ modules (HIGH confidence)
- CMakeLists.txt dependency chain tracing -- every link traced from actual files (HIGH confidence)
- `CLIStarter.cpp` end-to-end YAML processing flow (HIGH confidence)
- `LegacyOptimizer.cpp` validation phase ordering (HIGH confidence)
- `.planning/codebase/ARCHITECTURE.md` existing codebase documentation (HIGH confidence)
- Emscripten documentation for C++ to WASM compilation patterns (MEDIUM confidence -- verified against training data, not current docs)

---

*Architecture analysis: 2026-03-15*
