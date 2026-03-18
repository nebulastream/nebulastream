# Phase 6: Validation Library Extraction - Research

**Researched:** 2026-03-15
**Domain:** C++ library extraction, Emscripten/WebAssembly, CMake dependency management, stub/facade patterns
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Validation depth:**
- Full semantic validation: YAML parsing → catalog population → SQL parsing → source inference → type inference
- Single entry point: `validateTopology(yamlString)` — no per-query or per-component validation (TS ANTLR parser handles inline syntax)
- Extract YAML binding logic from nes-cli starter as the front half of the pipeline (same deserialization path as the real engine)

**Error structure:**
- Simple string return: empty string = valid topology, non-empty string = error message
- Single try-catch around the entire validation pipeline — report the exception message
- No structured error objects, severity levels, or categorization for now

**YAML parsing approach:**
- NES uses yaml-cpp for YAML parsing (not reflectcpp)
- All WASM dependencies via vcpkg with wasm32-emscripten triplet (consistent with NES approach)
- Migrate ANTLR4 from FetchContent to vcpkg in this phase (unify dependency management)

**Library placement:**
- New top-level CMake target: `nes-validator/` alongside nes-sql-parser/, nes-sources/, etc.
- Links existing NES targets as CMake dependencies — no source file duplication
- Stubs/facades only for heavy runtime deps (gRPC, Folly, cpptrace) that don't belong in validation
- Compiles both natively (for C++ unit tests and NES frontends) and to WASM (for topology editor)
- NES frontends (nes-cli, nes-repl) restructured to depend on nes-validator in this phase

### Claude's Discretion
- Exact stub/facade implementations for heavy deps
- CMake configuration details for dual native/WASM build
- vcpkg manifest and triplet configuration for yaml-cpp + ANTLR4
- C++ unit test structure and test cases
- How to restructure nes-frontend to use nes-validator

### Deferred Ideas (OUT OF SCOPE)
- Structured error objects with categories, severity, and location info (future v1.2)
- Error location mapping to specific UI nodes (deferred to v1.2)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| VLIB-01 | NES validation pipeline extracted into standalone CMake target without runtime dependencies | CMake target structure, dependency pruning strategy documented |
| VLIB-02 | Stub/facade layer replaces gRPC, Folly, spdlog, cpptrace for WASM-compatible build | Stub approach per dep documented with code patterns |
| VLIB-03 | Validation library accepts YAML string and returns structured error list | validateTopology(yamlString) → string interface documented, YAML binding code identified in CLIStarter.cpp |
| VLIB-04 | Catalog population from YAML (workers, sources, sinks, logical sources) | YAML→QueryConfig deserialization path traced, SourceCatalog/SinkCatalog population logic identified |
| VLIB-05 | SQL parsing via ANTLR4 C++ runtime within the validation library | StatementBinder::parseAndBind path + AntlrSQLQueryParser identified, FetchContent pattern validated |
| VLIB-06 | Logical plan construction and validation (source inference, type inference) | AntlrSQLQueryPlanCreator + LegacyOptimizer interaction identified |
| WASM-03 | Validation library compiles to .wasm via Emscripten with embind interface | Phase 5 WASM build pattern directly applicable, Embind pattern documented |
| WASM-04 | WASM binary under 1MB gzipped | Baseline: antlr4 spike = 675KB uncompressed; full pipeline needs size budget analysis |
| TEST-01 | C++ unit tests for validation library correctness | GTest/NES test patterns documented, StatementBinderTest.cpp as model |
</phase_requirements>

---

## Summary

Phase 6 extracts the NES validation pipeline into a standalone `nes-validator` CMake library that compiles both natively (used by NES frontends) and to WebAssembly (used by the topology editor). The core challenge is dependency surgery: the existing NES target graph pulls in heavy runtime dependencies (gRPC via nes-common, Folly via nes-memory/nes-sources, cpptrace via nes-common/ErrorHandling) that are incompatible with or unnecessary for a WASM validation-only build.

The extraction strategy is a facade/stub pattern at the CMake level. Rather than copying source files, `nes-validator` will link existing NES CMake targets (`nes-sql-parser`, `nes-logical-operators`, `nes-data-types`, `nes-sources`, `nes-sinks`, `nes-configurations`) and stub out the headers that pull in heavy deps. The YAML binding code already exists in `nes-frontend/apps/cli/CLIStarter.cpp` and will be extracted into `nes-validator/src/` as the front half of the pipeline.

The WASM build pattern from Phase 5 is directly applicable: Emscripten + Embind, `-fwasm-exceptions` on all translation units, `.cjs` suffix, and FetchContent for ANTLR4 runtime (no vcpkg binary available for wasm32-emscripten target). The Phase 5 antlr4 spike produced a 675KB uncompressed WASM binary with just SQL parsing; the full validation pipeline needs careful size measurement but the 1MB gzipped budget appears achievable given aggressive optimization flags.

**Primary recommendation:** Create `nes-validator/` as a new top-level CMake module. Use a two-level stubbing strategy: (1) compile-time stubs for cpptrace and Folly hash that replace specific includes, and (2) CMake-level interface stubs for heavy transitive dependencies (nes-memory, nes-executable, nes-runtime) that nes-sources/nes-sinks pull in. The WASM build lives in `nes-topology-editor/wasm/nes-validator/` and links nes-validator through FetchContent or direct source inclusion.

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| yaml-cpp | vcpkg baseline | YAML parsing in C++ | Already used by NES (nes-common links it), available via vcpkg for wasm32-emscripten |
| antlr4-runtime | 4.13.2 (FetchContent) | SQL parsing in WASM | Phase 5 validated this path; no vcpkg binary for WASM target |
| antlr4-runtime | vcpkg (native) | SQL parsing in native build | Already in NES vcpkg.json, used by nes-sql-parser |
| Emscripten | 5.0.3 (installed in emsdk/) | C++ to WASM compilation | Already installed by Phase 5 in nes-topology-editor/wasm/emsdk/ |
| Google Test | vcpkg | C++ unit tests | Project standard (TESTING.md) |
| fmt | vcpkg | Formatting | Transitive dep via nes-common, WASM compatible |
| spdlog | vcpkg | Logging | Transitive dep via nes-common; header-only-usable, stub null logger for WASM |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| nlohmann-json | vcpkg | JSON output | If error serialization is needed (currently just string) |
| emscripten/bind.h | Emscripten built-in | JS-C++ interop | WASM build only |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| FetchContent for ANTLR4 (WASM) | vcpkg antlr4 | vcpkg has no prebuilt wasm32-emscripten binary; FetchContent builds from source with correct flags |
| Stub header for Folly hash | Full Folly port to WASM | Full Folly has deep system dependencies; std::hash stub is correct and minimal |

**Installation (native):**
```bash
# NES vcpkg.json already contains: antlr4, yaml-cpp, fmt, spdlog, cpptrace
# No new vcpkg dependencies needed for native build

# WASM vcpkg.json needs:
# yaml-cpp (for WASM triplet)
# antlr4 handled via FetchContent (no vcpkg binary)
```

---

## Architecture Patterns

### Recommended Project Structure
```
nes-validator/
├── CMakeLists.txt              # Target definition, dual native/WASM config
├── include/
│   └── Validator/
│       └── TopologyValidator.hpp   # validateTopology() public API
├── src/
│   ├── TopologyValidator.cpp       # Entry point: YAML parsing + catalog + SQL validation
│   ├── YamlBinding.cpp             # Ported from CLIStarter.cpp (YAML→QueryConfig structs)
│   └── YamlBinding.hpp             # Internal types: SchemaField, LogicalSource, PhysicalSource, etc.
├── stubs/
│   ├── folly/
│   │   └── hash/
│   │       └── Hash.h              # std::hash replacement stub (for SourceDescriptor.hpp)
│   └── cpptrace/
│       ├── basic.hpp               # Empty stub
│       ├── cpptrace.hpp            # Empty stub
│       └── from_current.hpp        # CPPTRACE_TRY/CATCH → try/catch macros
└── tests/
    ├── CMakeLists.txt
    └── UnitTests/
        └── TopologyValidatorTest.cpp

nes-topology-editor/wasm/
├── CMakeLists.txt                  # Updated to add nes-validator subdirectory
├── nes-validator/
│   ├── CMakeLists.txt              # WASM-specific build: FetchContent ANTLR4, Embind
│   └── main.cpp                   # EMSCRIPTEN_BINDINGS for validateTopology
├── vcpkg.json                      # Add yaml-cpp dependency
└── triplets/
    └── wasm32-emscripten.cmake     # Existing from Phase 5
```

### Pattern 1: Public Interface — validateTopology()
**What:** Single C function exposed via both native C++ and Embind
**When to use:** Always — this is the only public API
**Example:**
```cpp
// Source: nes-validator/include/Validator/TopologyValidator.hpp
namespace NES::Validator {

/// Returns empty string on success, error message on failure.
/// Thread-safe (no shared state).
std::string validateTopology(const std::string& yamlString);

} // namespace NES::Validator

// Source: nes-topology-editor/wasm/nes-validator/main.cpp (WASM only)
#include <emscripten/bind.h>
#include <Validator/TopologyValidator.hpp>

EMSCRIPTEN_BINDINGS(nes_validator) {
    emscripten::function("validateTopology", &NES::Validator::validateTopology);
}
```

### Pattern 2: YAML Binding (from CLIStarter.cpp)
**What:** yaml-cpp decode specializations for NES topology structures
**When to use:** Front half of validation pipeline; port verbatim from CLIStarter.cpp
**Example:**
```cpp
// Source: nes-validator/src/YamlBinding.hpp — port from nes-frontend/apps/cli/CLIStarter.cpp
namespace NES::Validator {
struct SchemaField { std::string name; DataType type; };
struct LogicalSource { std::string name; std::vector<SchemaField> schema; };
struct PhysicalSource { std::string logical; std::string type;
                        std::unordered_map<std::string,std::string> parserConfig, sourceConfig; };
struct Sink { std::string name; std::string type; std::vector<SchemaField> schema;
              std::unordered_map<std::string,std::string> config, parserConfig; };
struct TopologyConfig {
    std::vector<std::string> query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};
} // namespace NES::Validator
// yaml-cpp convert<> specializations follow (copy from CLIStarter.cpp)
```

### Pattern 3: Catalog Population
**What:** Use StatementBinder via SourceStatementHandler/SinkStatementHandler to populate catalogs
**When to use:** Middle of pipeline after YAML binding
**Example:**
```cpp
// Source: nes-validator/src/TopologyValidator.cpp
std::string NES::Validator::validateTopology(const std::string& yamlString) {
    try {
        // 1. Parse YAML
        auto config = YAML::Load(yamlString).as<TopologyConfig>();

        // 2. Populate catalogs (same as CLIStarter::loadStatements)
        auto sourceCatalog = std::make_shared<SourceCatalog>();
        auto sinkCatalog = std::make_shared<SinkCatalog>();
        SourceStatementHandler sourceHandler{sourceCatalog};
        SinkStatementHandler sinkHandler{sinkCatalog};
        // ... call handlers for each logical/physical/sink

        // 3. Validate SQL queries via StatementBinder
        auto binder = StatementBinder{sourceCatalog,
            [](auto* ctx){ return AntlrSQLQueryParser::bindLogicalQueryPlan(ctx); }};
        for (const auto& query : config.query) {
            auto result = binder.parseAndBind(query);
            if (!result) return result.error().what();
        }
        return ""; // success
    } catch (const std::exception& e) {
        return e.what();
    }
}
```

### Pattern 4: Folly Hash Stub
**What:** Replace `folly/hash/Hash.h` with a std::hash wrapper so SourceDescriptor.hpp compiles
**When to use:** Anytime SourceDescriptor.hpp is included in a non-Folly build
**Example:**
```cpp
// Source: nes-validator/stubs/folly/hash/Hash.h
#pragma once
#include <functional>
namespace folly::hash {
template <typename... Ts>
size_t hash_combine(const Ts&... args) {
    size_t seed = 0;
    (..., (seed ^= std::hash<Ts>{}(args) + 0x9e3779b9 + (seed << 6) + (seed >> 2)));
    return seed;
}
} // namespace folly::hash
```

### Pattern 5: cpptrace Stub
**What:** Replace cpptrace headers with no-ops so ErrorHandling.hpp compiles without cpptrace
**When to use:** WASM build; optionally native nes-validator build to avoid cpptrace linkage
**Example:**
```cpp
// Source: nes-validator/stubs/cpptrace/basic.hpp
#pragma once
namespace cpptrace {
struct raw_trace { static raw_trace current(int = 0) { return {}; } };
std::string generate_trace() { return ""; }
} // namespace cpptrace

// Source: nes-validator/stubs/cpptrace/from_current.hpp
#pragma once
#define CPPTRACE_TRY try
#define CPPTRACE_CATCH(...) catch(...)
```

### Pattern 6: CMake Stub Include Path Injection
**What:** Prepend stub include directory to override system/vcpkg headers
**When to use:** Any target that needs stub headers in place of heavy deps
**Example:**
```cmake
# Source: nes-validator/CMakeLists.txt
target_include_directories(nes-validator-wasm BEFORE PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/stubs   # folly/hash/Hash.h, cpptrace stubs appear first
)
```

### Pattern 7: WASM Build Flags (from Phase 5)
**What:** Required Emscripten flags for correct exception handling
**When to use:** Every WASM target and every library it links (including ANTLR4)
**Example:**
```cmake
# Source: nes-topology-editor/wasm/nes-validator/CMakeLists.txt
set_target_properties(nes-validator-wasm PROPERTIES
    SUFFIX ".cjs"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2"
)
target_compile_options(nes-validator-wasm PRIVATE -fwasm-exceptions -O2)
# Also apply -fwasm-exceptions to antlr4_static (as in Phase 5)
target_compile_options(antlr4_static PRIVATE -fwasm-exceptions -w)
```

### Anti-Patterns to Avoid
- **Copying source files into nes-validator:** Duplicates code, creates maintenance burden. Link existing targets instead.
- **Linking nes-runtime or nes-executable:** These pull in Folly, gRPC, MLIR, and the full execution engine. nes-sources links them via PUBLIC, which means nes-validator must avoid linking nes-sources directly and instead use a stripped interface.
- **Using nes-common directly in WASM without stubs:** nes-common's ErrorHandling.hpp includes cpptrace, which has system-level stack tracing incompatible with WASM. Stubs must appear before nes-common headers.
- **Forgetting -fwasm-exceptions on ANTLR4 runtime:** Phase 5 found this causes hard crashes at parse time. Every TU including antlr4_static must use -fwasm-exceptions.
- **SINGLE_FILE=1 for a library this large:** SINGLE_FILE embeds the WASM binary in the JS as base64, inflating the JS by 33%. With a ~1MB target budget, use separate .wasm + .cjs files. SINGLE_FILE=1 is appropriate for Phase 7 integration, not here.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| YAML parsing | Custom YAML tokenizer | yaml-cpp | Already in NES (nes-common), proven on production topology files |
| SQL parsing | Custom SQL parser | ANTLR4 + AntlrSQL.g4 | NES grammar encodes all valid NES SQL syntax; Phase 5 validated WASM compile |
| Type checking | Custom type inference | AntlrSQLQueryPlanCreator + LogicalPlan | Type inference happens during plan construction; already handles schema propagation |
| Hash combining | Boost hash_combine | folly::hash stub wrapping std::hash | A 3-line XOR combiner is correct for the stub; no need for a complex implementation |
| Exception handling in WASM | Custom error codes | std::exception + try/catch | -fwasm-exceptions makes native C++ exceptions work correctly in WASM |
| JS-C++ bridge | cwrap/ccall | Embind | Embind handles std::string marshaling automatically; cwrap requires manual memory management |

**Key insight:** The entire validation pipeline already exists in production NES code. This phase is wiring, not building — the only new code is the thin adapter (TopologyValidator.cpp + YamlBinding.cpp) and the stubs.

---

## Common Pitfalls

### Pitfall 1: nes-sources → nes-executable → nes-runtime Transitive Pull
**What goes wrong:** Linking `nes-sources` (needed for SourceCatalog) also links `nes-executable` and then `nes-runtime`, which brings in gRPC, Folly, MLIR, and the full execution engine.
**Why it happens:** `target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-memory nes-executable)` — nes-executable is PUBLIC.
**How to avoid:** Do NOT link nes-sources as a target. Instead, either (a) include only the specific headers needed from nes-sources by adding its include path directly, or (b) create a CMake INTERFACE target `nes-validator-sources-stub` that provides just the include paths without the transitive link deps.
**Warning signs:** CMake configuration errors about missing gRPC, Folly, or nes-runtime symbols.

### Pitfall 2: cpptrace in Exception class definition
**What goes wrong:** `Exception final : public cpptrace::lazy_exception` in ErrorHandling.hpp means you can't use NES's Exception class at all without cpptrace. Every throw site creates a `cpptrace::raw_trace`.
**Why it happens:** cpptrace is part of the class inheritance chain, not just an include.
**How to avoid:** For WASM, provide a cpptrace stub that defines `cpptrace::lazy_exception` as a plain `std::exception` subclass with a no-op constructor. The stub must be on the include path BEFORE nes-common headers.
**Warning signs:** "cpptrace::lazy_exception" undeclared, or cpptrace functions missing during link.

### Pitfall 3: Folly Hash in std::hash<SourceDescriptor>
**What goes wrong:** `std::hash<NES::SourceDescriptor>` is defined in SourceDescriptor.hpp using `folly::hash::hash_combine`. Any code that uses SourceDescriptor in an unordered container will fail to compile without Folly.
**Why it happens:** The std::hash specialization is in the header, not in a .cpp, so it can't be hidden.
**How to avoid:** The stub `folly/hash/Hash.h` (3 lines) must appear on the include path before any Folly installation. Use CMake `BEFORE` keyword in `target_include_directories`.
**Warning signs:** "folly/hash/Hash.h: No such file or directory" or link errors about missing folly symbols.

### Pitfall 4: reflectcpp in nes-logical-operators
**What goes wrong:** `nes-logical-operators` links `reflectcpp::reflectcpp PRIVATE`. This is PRIVATE, so it doesn't propagate — but the `Util/Reflection.hpp` header (included by SourceDescriptor.hpp) includes `<rfl/Generic.hpp>` and other rfl headers.
**Why it happens:** Reflection.hpp is a PUBLIC header in nes-common, but its rfl includes require reflectcpp headers to be on the include path.
**How to avoid:** Either add reflectcpp to the WASM vcpkg.json (check WASM compatibility first) or stub the minimal rfl headers needed by Reflection.hpp. reflectcpp's WASM compatibility is UNKNOWN — this is a risk to investigate early.
**Warning signs:** "rfl/Generic.hpp: No such file or directory" in WASM build.

### Pitfall 5: spdlog in nes-common Logger
**What goes wrong:** `Logger.hpp` includes `spdlog/fwd.h` and `spdlog/logger.h`. spdlog needs threading primitives. In WASM single-threaded mode, spdlog may fail to compile or link.
**Why it happens:** spdlog uses `std::mutex` and OS thread IDs.
**How to avoid:** Add spdlog to the WASM vcpkg.json. spdlog is widely used in embedded/WASM contexts and is generally compatible. Alternatively, define `SPDLOG_NO_THREAD_ID` and `SPDLOG_DISABLE_DEFAULT_LOGGER` to reduce threading dependencies. A null logger implementation (no-op sink) suffices for validation.
**Warning signs:** spdlog compile errors about `pthread` or thread-local storage in WASM.

### Pitfall 6: ANTLR4 Exception Propagation
**What goes wrong:** ANTLR4's `ParseCancellationException` (thrown by BailErrorStrategy) fails to propagate correctly to the catch block in StatementBinder::parseAndBind if -fwasm-exceptions is not applied consistently.
**Why it happens:** Emscripten's default exception handling uses JavaScript exceptions; WASM native exceptions (-fwasm-exceptions) are a separate mechanism. Mixed compilation causes exceptions thrown in one TU to not be caught in another.
**How to avoid:** Apply `-fwasm-exceptions` to every compilation unit in the WASM build — antlr4_static, all parser generated files, and nes-validator itself. Verified pattern from Phase 5.
**Warning signs:** Uncaught exception crashes in WASM that work fine natively.

### Pitfall 7: WASM Binary Size Budget
**What goes wrong:** Adding full NES validation stack (yaml-cpp + ANTLR4 + SourceCatalog + SinkCatalog + LogicalOperators) may exceed 1MB gzipped.
**Why it happens:** Phase 5 antlr4 spike alone = 675KB uncompressed; adding yaml-cpp, schemas, catalog logic will add more. -O2 on all code is needed.
**How to avoid:** Use `-O2` (or `-Os` for size) on all WASM compilation. Use `-sASSERTIONS=0` in release. Measure gzipped size early (after first successful WASM build) to detect budget overrun before final task.
**Warning signs:** WASM binary >800KB uncompressed (gzip typically achieves 40-60% compression on WASM).

---

## Code Examples

Verified patterns from official sources / Phase 5 code:

### WASM CMakeLists Pattern (from Phase 5 spike-antlr4)
```cmake
# Source: nes-topology-editor/wasm/spike-antlr4/CMakeLists.txt (Phase 5)
include(FetchContent)
FetchContent_Declare(
    antlr4-runtime
    GIT_REPOSITORY https://github.com/antlr/antlr4.git
    GIT_TAG        4.13.2
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  runtime/Cpp
)
set(ANTLR4_INSTALL OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_STATIC ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(antlr4-runtime)

# CRITICAL: -fwasm-exceptions must be on antlr4_static too
target_compile_options(antlr4_static PRIVATE -fwasm-exceptions -w)

# Target link flags
set_target_properties(nes-validator-wasm PROPERTIES
    SUFFIX ".cjs"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2"
)
```

### Embind Interface (from Phase 5 spike-embind)
```cpp
// Source: nes-topology-editor/wasm/spike-embind/main.cpp (Phase 5)
#include <emscripten/bind.h>
#include <Validator/TopologyValidator.hpp>

EMSCRIPTEN_BINDINGS(nes_validator) {
    emscripten::function("validateTopology", &NES::Validator::validateTopology);
}
```

### NES GTest Unit Test Pattern (from StatementBinderTest.cpp)
```cpp
// Source: nes-sql-parser/tests/StatementBinderTest.cpp
class TopologyValidatorTest : public Testing::BaseUnitTest {
public:
    static void SetUpTestSuite() {
        Logger::setupLogging("TopologyValidatorTest.log", LogLevel::LOG_DEBUG);
    }
    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(TopologyValidatorTest, ValidTopologyReturnsEmptyString) {
    auto result = NES::Validator::validateTopology(validYaml);
    EXPECT_EQ(result, "");
}

TEST_F(TopologyValidatorTest, MissingSourceReturnsError) {
    auto result = NES::Validator::validateTopology(yamlWithUnknownSource);
    EXPECT_NE(result, "");
    EXPECT_THAT(result, testing::HasSubstr("unknown"));
}
```

### CMake Test Registration Pattern
```cmake
# Source: nes-sql-parser/tests/CMakeLists.txt pattern
add_nes_unit_test(topology-validator-test "UnitTests/TopologyValidatorTest.cpp")
target_link_libraries(topology-validator-test nes-validator)
```

### Validation Pipeline Flow (traced from CLIStarter.cpp)
```
YAML string
  → YAML::Load() → as<TopologyConfig>()  [yaml-cpp, in YamlBinding.cpp]
  → loadStatements() → CreateLogicalSourceStatement, CreatePhysicalSourceStatement, CreateSinkStatement
  → SourceStatementHandler::operator() → SourceCatalog::addLogicalSource/addPhysicalSource
  → SinkStatementHandler::operator()  → SinkCatalog::addSinkDescriptor
  → for each query string:
      → StatementBinder::parseAndBind(query)
          → ANTLR4 lexer/parser → StatementContext
          → bindQueryStatement() → AntlrSQLQueryPlanCreator::bindLogicalQueryPlan()
          → LogicalPlan (with source/type inference)
  → return "" on success, exception.what() on any throw
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| FetchContent for ANTLR4 (Phase 5 spike) | vcpkg for native build, FetchContent for WASM | Phase 6 | Unifies dependency management; WASM still uses FetchContent (no vcpkg binary) |
| Validation in nes-frontend (CLIStarter) | nes-validator standalone library | Phase 6 | NES frontends and topology editor share same validation path |
| nes-cli directly does YAML→validate→submit | nes-cli delegates to nes-validator | Phase 6 | nes-frontend refactored to use nes-validator |

**Deprecated/outdated:**
- Direct YAML parsing in CLIStarter.cpp: Replace with `NES::Validator::validateTopology()` call in nes-frontend after refactor
- FetchContent for ANTLR4 in main NES CMakeLists: Already using vcpkg for native; Phase 6 migrates WASM spike to consistent pattern

---

## Open Questions

1. **reflectcpp WASM compatibility**
   - What we know: nes-logical-operators links reflectcpp PRIVATE; Reflection.hpp (a PUBLIC header in nes-common) includes `<rfl/Generic.hpp>` and other rfl headers
   - What's unclear: Whether reflectcpp compiles to wasm32-emscripten; whether Reflection.hpp is transitively included by headers the validator needs
   - Recommendation: Try adding reflectcpp to WASM vcpkg.json first; if it fails, create minimal rfl stubs (rfl::Generic, rfl::Result, rfl::from_generic, rfl::to_generic templates with no-op implementations)

2. **nes-sources dependency chain severity**
   - What we know: nes-sources links nes-memory and nes-executable PUBLIC, both of which pull in heavy deps; nes-sources is needed for SourceCatalog
   - What's unclear: Whether cherry-picking just SourceCatalog.hpp + SourceDescriptor.hpp include paths (without linking nes-sources target) is feasible or if source compilation requires nes-sources .cpp files
   - Recommendation: Try linking nes-sources with a WASM-safe interface lib that only exposes include paths; if SourceCatalog.cpp itself compiles cleanly without the heavy transitive deps, this works

3. **LegacyOptimizer necessity**
   - What we know: CLIStarter.cpp uses LegacyOptimizer to validate query plans (optimize → check for errors); StatementBinder alone may not do full type inference
   - What's unclear: Whether LegacyOptimizer is needed for validation or if StatementBinder::parseAndBind is sufficient to catch type/source errors
   - Recommendation: Start without LegacyOptimizer (just parseAndBind); add it if test cases show type mismatches aren't caught

4. **WASM binary size with full pipeline**
   - What we know: antlr4 spike alone = 675KB uncompressed; gzip typically 40-60% reduction (~270-400KB gzipped); budget is 1MB gzipped
   - What's unclear: How much yaml-cpp + SourceCatalog + SinkCatalog + LogicalOperators add to binary
   - Recommendation: Measure after first successful WASM build; use `-Os` instead of `-O2` if over budget

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Google Test (GTest) via vcpkg |
| Config file | cmake/NebulaStreamTest.cmake (defines add_nes_unit_test) |
| Quick run command | `ctest --test-dir cmake-build-debug -R "topology-validator" -j --output-on-failure` |
| Full suite command | `ctest --test-dir cmake-build-debug -R "topology-validator|statement-binder" -j --output-on-failure` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VLIB-01 | nes-validator compiles without runtime deps | build | `cmake --build cmake-build-debug --target nes-validator -j` | ❌ Wave 0 |
| VLIB-02 | Stubs compile clean for gRPC/Folly/cpptrace | build | `cmake --build cmake-build-debug --target nes-validator -j` | ❌ Wave 0 |
| VLIB-03 | validateTopology() returns "" for valid YAML | unit | `ctest --test-dir cmake-build-debug -R "ValidTopologyReturnsEmpty" -j` | ❌ Wave 0 |
| VLIB-04 | Catalog population from YAML (sources/sinks) | unit | `ctest --test-dir cmake-build-debug -R "CatalogPopulation" -j` | ❌ Wave 0 |
| VLIB-05 | SQL parse errors surface as non-empty string | unit | `ctest --test-dir cmake-build-debug -R "InvalidSqlReturnsError" -j` | ❌ Wave 0 |
| VLIB-06 | Unknown source name surfaces as non-empty string | unit | `ctest --test-dir cmake-build-debug -R "UnknownSourceReturnsError" -j` | ❌ Wave 0 |
| WASM-03 | WASM binary builds successfully | build | `cmake --build nes-topology-editor/wasm/build --target nes-validator-wasm -j` | ❌ Wave 0 |
| WASM-04 | WASM binary < 1MB gzipped | manual/script | `gzip -c nes-validator-wasm.wasm | wc -c` | ❌ Wave 0 |
| TEST-01 | All validator unit tests pass | unit | `ctest --test-dir cmake-build-debug -R "topology-validator-test" -j --output-on-failure` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `ctest --test-dir cmake-build-debug -R "topology-validator-test" -j --output-on-failure`
- **Per wave merge:** `ctest --test-dir cmake-build-debug -R "topology-validator|statement-binder" -j --output-on-failure`
- **Phase gate:** Full suite green + WASM binary builds before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `nes-validator/CMakeLists.txt` — CMake target definition
- [ ] `nes-validator/include/Validator/TopologyValidator.hpp` — public API header
- [ ] `nes-validator/src/TopologyValidator.cpp` — implementation stub
- [ ] `nes-validator/stubs/folly/hash/Hash.h` — Folly stub
- [ ] `nes-validator/stubs/cpptrace/basic.hpp` etc. — cpptrace stubs
- [ ] `nes-validator/tests/CMakeLists.txt` — test registration
- [ ] `nes-validator/tests/UnitTests/TopologyValidatorTest.cpp` — test file
- [ ] `nes-topology-editor/wasm/nes-validator/CMakeLists.txt` — WASM build target

---

## Sources

### Primary (HIGH confidence)
- Codebase direct inspection: `nes-frontend/apps/cli/CLIStarter.cpp` — YAML binding code to extract
- Codebase direct inspection: `nes-sql-parser/src/StatementBinder.cpp` — SQL validation pipeline
- Codebase direct inspection: `nes-sources/include/Sources/SourceDescriptor.hpp` — Folly hash dependency confirmed
- Codebase direct inspection: `nes-common/include/ErrorHandling.hpp` — cpptrace dependency confirmed
- Codebase direct inspection: `nes-sources/CMakeLists.txt`, `nes-sinks/CMakeLists.txt`, `nes-common/CMakeLists.txt` — dependency graph traced
- Phase 5 WASM spike: `nes-topology-editor/wasm/spike-antlr4/CMakeLists.txt` (git commit daee06a4fa) — ANTLR4 FetchContent + wasm-exceptions pattern validated
- Phase 5 WASM spike: `nes-topology-editor/wasm/spike-embind/main.cpp` (git commit f0d3c9d701) — Embind pattern validated
- Phase 5 summary: `.planning/phases/05-proof-of-concept-spikes/05-02-SUMMARY.md` — binary sizes, key decisions

### Secondary (MEDIUM confidence)
- `.planning/codebase/TESTING.md` — GTest patterns, test macro usage
- `.planning/codebase/STACK.md` — vcpkg dependency list confirmed
- `.planning/codebase/ARCHITECTURE.md` — NES module layer structure

### Tertiary (LOW confidence)
- reflectcpp WASM compatibility: Unknown — requires empirical test during execution
- spdlog WASM compatibility: HIGH confidence based on library design (no POSIX deps in header-only path), but not tested in this project yet

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — yaml-cpp and ANTLR4 patterns directly validated, stub patterns derived from header inspection
- Architecture: HIGH — YAML binding code, catalog population, and SQL validation pipeline traced directly in source
- Pitfalls: HIGH for Folly/cpptrace (confirmed by header inspection), MEDIUM for reflectcpp/spdlog WASM compat (not yet tested)
- Dependency graph: HIGH — traced CMakeLists.txt at every layer

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable project — no fast-moving dependencies)
