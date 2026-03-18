# Technology Stack: WASM Validation Milestone

**Project:** NebulaStream Topology Editor -- v1.1 Shared Validation via WebAssembly
**Researched:** 2026-03-15
**Scope:** Stack additions/changes for C++ validation library extraction and WebAssembly compilation. Existing UI stack (React, React Flow, Zustand, etc.) is not re-evaluated here.

## Executive Summary

The NES validation pipeline (`nes-sql-parser` -> `nes-logical-operators` -> `nes-frontend`) is deeply entangled with the full NES runtime through `nes-common`, which depends on gRPC, Folly, cpptrace, spdlog, libuuid, and Boost. **None of these compile to WebAssembly.** The primary technical challenge is not "how to use Emscripten" but "how to extract the validation code from these dependencies."

Two strategies exist:
1. **Facade pattern (recommended):** Create a thin `nes-validation-lib` that wraps only the validation-relevant code, with compile-time substitution of heavy dependencies (stub out logging, replace ErrorHandling with a WASM-compatible version, etc.)
2. **Full refactor:** Decouple `nes-common` into `nes-common-core` (header-only/lightweight) and `nes-common-runtime` (gRPC, folly, etc.) -- correct but high-risk and invasive to the main codebase.

Strategy 1 is recommended because it can be done in the topology editor's build without modifying the NES core libraries.

## The Dependency Problem

The validation pipeline's transitive dependency graph:

```
nes-frontend-lib
  -> nes-sql-parser
       -> antlr4_static (ANTLR C++ runtime)  -- OK for WASM
       -> nes-common                           -- PROBLEM
       -> nes-sources                          -- PROBLEM (via nes-common, nes-memory, nes-executable)
       -> nes-sinks                            -- PROBLEM (via nes-common, nes-memory, nes-executable)
       -> nes-logical-operators                -- PROBLEM (via nes-sources, nes-sinks, nes-query-optimizer)
       -> nes-input-formatters (PRIVATE)       -- PROBLEM (via nes-runtime)
  -> nes-logical-operators
       -> nes-sources -> nes-common, nes-configurations, nes-memory, nes-executable
       -> nes-sinks   -> nes-common, nes-configurations, nes-data-types, nes-memory, nes-executable
       -> nes-query-optimizer -> nes-distributed -> nes-grpc
       -> reflectcpp
  -> nes-query-optimizer
       -> highs (LP solver)
       -> nes-distributed -> nes-grpc
  -> yaml-cpp

nes-common (the root problem):
  -> nes-grpc (protobuf + gRPC)  -- NOT compilable to WASM
  -> folly                        -- NOT compilable to WASM
  -> cpptrace                     -- NOT compilable to WASM (OS-specific stack unwinding)
  -> spdlog                       -- Possible but heavy, uses filesystem/threads
  -> fmt                          -- OK for WASM
  -> libcuckoo                    -- Possible but concurrent hashmap is pointless in WASM
  -> magic_enum                   -- OK for WASM (header-only)
  -> yaml-cpp                     -- OK for WASM
  -> Boost::url                   -- Possible, header-heavy
  -> PkgConfig::uuid              -- NOT compilable to WASM (libuuid is OS-specific)
```

**Dependencies that compile to WASM cleanly:** antlr4 C++ runtime, yaml-cpp, fmt, magic_enum, nlohmann-json
**Dependencies that do NOT compile to WASM:** gRPC/protobuf, folly, cpptrace, libuuid, Boost.Asio
**Dependencies that are possible but unnecessary for validation:** spdlog, libcuckoo, reflectcpp, highs

## Recommended Stack

### Emscripten Toolchain

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Emscripten SDK | 4.0.x+ (latest stable) | C++ to WASM compiler | Standard toolchain. Uses LLVM 19+ internally, which supports C++23 features including `std::expected`, `std::variant`, concepts. The NES codebase requires C++23 | MEDIUM |

**C++23 Compatibility Note:** Emscripten 4.0+ ships with LLVM 19's libc++, which implements `std::expected` (the NES error handling pattern). The `-std=c++23` flag is accepted by `em++`. C++23 module support (`import std`) does NOT work, but the NES codebase uses traditional `#include` headers, so this is not a blocker. The codebase also uses `std::ranges` extensively -- this is supported in LLVM 19 libc++.

**Confidence rationale:** MEDIUM because C++23 support in Emscripten is not extensively documented. The LLVM version supports it, but edge cases (especially around `concepts` and `std::ranges` in the WASM target) need hands-on validation in the first phase.

### WASM Build Configuration

| Setting | Value | Purpose | Why |
|---------|-------|---------|-----|
| `-std=c++23` | Required | C++ standard | NES codebase uses `std::expected`, concepts, ranges |
| `-Os` | Recommended | Optimization | Balances binary size and performance. WASM downloads matter |
| `-flto` | Recommended | Link-time optimization | Eliminates dead code from stubbed dependencies |
| `-fno-exceptions` | **DO NOT USE** | -- | NES uses `std::expected` but also throws in `ErrorHandling.hpp`. Must keep exceptions |
| `-sEXPORT_ES6=1` | Required | ES module output | Integrates with Vite/ESM bundler pipeline |
| `-sMODULARIZE=1` | Required | Module wrapper | Wraps WASM in a factory function, avoids global scope pollution |
| `-sEXPORT_NAME=NESValidation` | Required | Module name | Clean import: `import NESValidation from './nes-validation.js'` |
| `-sALLOW_MEMORY_GROWTH=1` | Required | Dynamic memory | Validation input size is unpredictable |
| `-sINITIAL_MEMORY=4194304` | Recommended | 4MB initial heap | Reasonable for parsing/validation workloads |
| `-sNO_FILESYSTEM=1` | Recommended | Disable FS emulation | Validation library does not need filesystem access. Saves ~50KB |
| `-sSINGLE_FILE=1` | Optional | Inline WASM in JS | Simplifies deployment (no separate .wasm file), but increases JS bundle. Evaluate tradeoff |

### JS/WASM Interop

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Embind | (bundled with Emscripten) | C++/JS binding layer | The validation API is a single function: `string -> JSON`. Embind handles `std::string` <-> JS string conversion natively. WebIDL cannot handle `std::string` or smart pointers. Embind's runtime overhead (~15KB) is acceptable for a non-hot-path validation call | HIGH |

**Why Embind over WebIDL:** WebIDL is lighter but cannot express `std::string`, `std::vector`, or any STL types. The validation interface returns structured error objects. Embind supports registering C++ classes and returning `emscripten::val` (JS objects) directly from C++, which lets us return `{valid: bool, errors: [{line: number, message: string}]}` without manual JSON serialization.

**Why NOT raw `cwrap`/`ccall`:** These work only with numeric types and raw pointers. We would need to manually manage string memory (allocate buffer in WASM, copy string in, call function, read result pointer, free). Embind eliminates this boilerplate.

### C++ Dependencies for WASM Build

| Library | WASM Compatible | Role in Validation | Strategy | Confidence |
|---------|-----------------|-------------------|----------|------------|
| antlr4 C++ runtime | YES | SQL parsing | Compile from source with Emscripten. Pure C++, no OS dependencies. The antlr4wasm project proved this works (though it was abandoned because JS runtime was faster -- our case is different since we need the full NES binding logic) | HIGH |
| yaml-cpp | YES | YAML parsing | Compile from source with Emscripten. Pure C++ with no OS dependencies. CMake-based, straightforward cross-compilation | HIGH |
| fmt | YES | String formatting | Header-mostly library. Compiles cleanly with Emscripten | HIGH |
| magic_enum | YES | Enum reflection | Header-only. Zero compilation issues | HIGH |
| spdlog | PARTIAL | Logging | Not needed in WASM. Stub out with no-op macros at compile time | HIGH |
| cpptrace | NO | Stack traces | Not needed in WASM. Stub out ErrorHandling.hpp to skip stack trace capture | HIGH |
| folly | NO | Concurrent utilities | Not used in validation path. Exclude via dependency isolation | HIGH |
| gRPC/protobuf | NO | RPC communication | Not used in validation path. Exclude via dependency isolation | HIGH |
| libcuckoo | NO | Concurrent hashmap | Not used in validation path. Exclude via dependency isolation | HIGH |
| libuuid | NO | UUID generation | Not used in validation path. Provide trivial stub | HIGH |
| reflectcpp | UNKNOWN | Reflection for `nes-logical-operators` | Needs investigation. May need to be stubbed or compiled | LOW |
| highs | NO | LP solver (optimizer) | Not needed for validation-only path. Exclude | HIGH |
| Boost::url | PARTIAL | URL parsing | May be needed by some `nes-common` utilities. Evaluate if validation path actually uses it | LOW |

### CMake Target Structure

The key architectural decision: create a **parallel CMake target** that compiles only the validation-relevant source files with WASM-compatible dependency stubs.

```
nes-topology-editor/
  wasm/
    CMakeLists.txt              # Top-level WASM build
    toolchain/
      Emscripten.cmake          # Emscripten toolchain config
    stubs/
      include/
        Util/Logger/Logger.hpp  # No-op logging macros
        Util/Logger/NesLogger.hpp
        ErrorHandling.hpp       # Simplified (no cpptrace)
      src/
        ErrorHandling.cpp       # Minimal exception without stack traces
    bindings/
      validation_bindings.cpp   # Embind interface: validateTopology(yaml) -> errors
    src/
      ValidationFacade.cpp      # Orchestrates: YAML parse -> statements -> bind -> validate
```

**CMakeLists.txt structure:**

```cmake
cmake_minimum_required(VERSION 3.21)
project(nes-validation-wasm LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)

# Import only the source files needed (NOT the full CMake targets)
# This avoids pulling in the full dependency graph

# 1. ANTLR4 runtime (compile from vcpkg source or bundled)
add_subdirectory(third-party/antlr4-runtime)

# 2. yaml-cpp (compile from vcpkg source or bundled)
add_subdirectory(third-party/yaml-cpp)

# 3. fmt (header-only mode or compile)
add_subdirectory(third-party/fmt)

# 4. Collect NES source files directly (bypassing NES CMake targets)
set(NES_COMMON_WASM_SOURCES
    ${NES_ROOT}/nes-common/src/Identifiers/...
    ${NES_ROOT}/nes-common/src/Windowing/...
    # Only the files actually needed by the validation path
)

set(NES_VALIDATION_SOURCES
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLQueryParser.cpp
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp
    ${NES_ROOT}/nes-sql-parser/src/StatementBinder.cpp
    ${NES_ROOT}/nes-sql-parser/src/CommonParserFunctions.cpp
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLHelper.cpp
    ${NES_ROOT}/nes-data-types/src/...
    ${NES_ROOT}/nes-logical-operators/src/...  # Subset
    ${NES_ROOT}/nes-configurations/src/...     # Subset
)

# 5. Stub library (replaces nes-common's heavy deps)
add_library(nes-wasm-stubs STATIC
    stubs/src/ErrorHandling.cpp
    stubs/src/Logger.cpp
)
target_include_directories(nes-wasm-stubs PUBLIC
    stubs/include
    ${NES_ROOT}/nes-common/include  # For headers that DON'T need stubbing
)

# 6. Main validation library
add_library(nes-validation STATIC
    ${NES_COMMON_WASM_SOURCES}
    ${NES_VALIDATION_SOURCES}
    src/ValidationFacade.cpp
)
target_link_libraries(nes-validation
    antlr4_static yaml-cpp fmt::fmt nes-wasm-stubs
)

# 7. Embind executable (produces .js + .wasm)
add_executable(nes-validation-wasm bindings/validation_bindings.cpp)
target_link_libraries(nes-validation-wasm nes-validation)
target_link_options(nes-validation-wasm PRIVATE
    -lembind
    -sEXPORT_ES6=1
    -sMODULARIZE=1
    -sEXPORT_NAME=NESValidation
    -sALLOW_MEMORY_GROWTH=1
    -sNO_FILESYSTEM=1
)
```

### Embind Interface Design

```cpp
// bindings/validation_bindings.cpp
#include <emscripten/bind.h>
#include "ValidationFacade.hpp"

using namespace emscripten;

EMSCRIPTEN_BINDINGS(nes_validation) {
    value_object<ValidationError>("ValidationError")
        .field("line", &ValidationError::line)
        .field("column", &ValidationError::column)
        .field("message", &ValidationError::message)
        .field("severity", &ValidationError::severity);

    register_vector<ValidationError>("ValidationErrorVector");

    value_object<ValidationResult>("ValidationResult")
        .field("valid", &ValidationResult::valid)
        .field("errors", &ValidationResult::errors);

    function("validateTopology", &validateTopology);
    // validateTopology(yamlString: string) -> ValidationResult
}
```

### TypeScript Integration

```typescript
// nes-topology-editor/src/wasm/validation.ts
import createModule from './nes-validation.js';

let moduleInstance: any = null;

export async function initValidation(): Promise<void> {
  moduleInstance = await createModule();
}

export interface ValidationError {
  line: number;
  column: number;
  message: string;
  severity: 'error' | 'warning';
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

export function validateTopology(yaml: string): ValidationResult {
  if (!moduleInstance) throw new Error('WASM module not initialized');
  return moduleInstance.validateTopology(yaml);
}
```

### Build Tooling Integration

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| vite-plugin-wasm | ^3.4+ | WASM loading in Vite | Handles `.wasm` file loading and instantiation in dev/prod. Without this, Vite requires manual `fetch()` + `WebAssembly.instantiate()` | MEDIUM |
| vite-plugin-top-level-await | ^1.4+ | Top-level await support | WASM initialization is async. This plugin allows `await init()` at module scope in older browsers | MEDIUM |

**Alternative to plugins:** If `SINGLE_FILE=1` is used (WASM inlined in JS), no special Vite plugin is needed -- the output is a plain ES module. This simplifies the build but increases the JS bundle size. Recommended to start with `SINGLE_FILE=1` and optimize later.

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| WASM toolchain | Emscripten | wasi-sdk / raw Clang | Emscripten provides libc++, embind, browser integration out of box. wasi-sdk requires manual JS glue |
| JS binding | Embind | WebIDL | WebIDL cannot handle std::string, std::vector, or complex return types |
| JS binding | Embind | cwrap/ccall | Manual memory management for every string parameter. Error-prone, no type safety |
| Dep isolation | Stub/facade | Full NES refactor | Modifying nes-common would affect the entire NES build. Too invasive for a UI tool |
| Dep isolation | Source-file cherry-pick | Compile full NES targets | Full targets pull in gRPC, folly, etc. No way to compile those to WASM |
| ANTLR in WASM | Compile C++ runtime | Use antlr4wasm | antlr4wasm is abandoned. We need the full NES binding logic, not just the parser |
| ANTLR in WASM | Compile C++ runtime | Keep TypeScript ANTLR | Defeats the purpose -- we want single-source-of-truth validation matching the NES engine |

## Do NOT Add

| Technology | Reason |
|------------|--------|
| protobuf for JS | The validation interface is YAML-in, JSON-out. No need for protobuf serialization |
| gRPC-Web | This is offline/client-side. No server communication |
| wasm-bindgen | Rust tooling. Not applicable to C++ WASM |
| AssemblyScript | Compiles TS to WASM. We need to compile existing C++ code |
| wasm-pack | Rust tooling. Not applicable |
| Cheerp | Alternative C++ to WASM compiler. Emscripten is the standard with better ecosystem support |

## Build Scripts

```bash
# Install Emscripten SDK (one-time setup)
git clone https://github.com/emscripten-core/emsdk.git
cd emsdk
./emsdk install latest
./emsdk activate latest
source ./emsdk_env.sh

# Build WASM validation module
cd nes-topology-editor/wasm
emcmake cmake -B build -DCMAKE_BUILD_TYPE=Release
emmake cmake --build build

# Output: build/nes-validation-wasm.js + build/nes-validation-wasm.wasm
# Copy to: nes-topology-editor/src/wasm/
cp build/nes-validation-wasm.js ../src/wasm/
cp build/nes-validation-wasm.wasm ../src/wasm/  # if not SINGLE_FILE
```

## Expected WASM Binary Size

| Component | Estimated Size (gzipped) |
|-----------|-------------------------|
| ANTLR4 C++ runtime | ~150-250 KB |
| yaml-cpp | ~80-120 KB |
| NES validation logic | ~50-100 KB |
| fmt | ~30-50 KB |
| Embind runtime | ~15 KB |
| **Total estimate** | **~350-550 KB gzipped** |

This is acceptable for a developer tool loaded once per session. For comparison, Monaco Editor is ~2MB.

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| C++23 features fail in Emscripten | HIGH | First phase: compile a minimal test with `std::expected`, concepts, ranges. If broken, fall back to C++20 polyfills |
| Dependency isolation is incomplete (missing stubs) | HIGH | Iterative: compile, fix linker errors one at a time. Track all needed stubs in a checklist |
| `reflectcpp` required by `nes-logical-operators` cannot compile to WASM | MEDIUM | Investigate what reflectcpp does in the validation path. If only used for serialization, stub it out |
| ANTLR4 generated parser files need Java toolchain for regeneration | LOW | Generated files can be committed. Only regenerate when grammar changes |
| Binary size exceeds 1MB gzipped | LOW | Apply `-Os -flto`, strip unused ANTLR features, evaluate code splitting |

## Sources

- [Emscripten Embind documentation](https://emscripten.org/docs/porting/connecting_cpp_and_javascript/embind.html) -- binding API reference
- [Emscripten WebIDL Binder](https://emscripten.org/docs/porting/connecting_cpp_and_javascript/WebIDL-Binder.html) -- limitations vs Embind
- [Emscripten Building Projects](https://emscripten.org/docs/compiling/Building-Projects.html) -- CMake integration
- [Emscripten Optimizing Code](https://emscripten.org/docs/optimizing/Optimizing-Code.html) -- binary size optimization flags
- [Emscripten Settings Reference](https://emscripten.org/docs/tools_reference/settings_reference.html) -- MODULARIZE, EXPORT_ES6, etc.
- [antlr4wasm](https://github.com/mike-lischke/antlr4wasm) -- abandoned proof that ANTLR4 C++ compiles to WASM
- [ANTLR4 C++ runtime CMake](https://github.com/antlr/antlr4/blob/master/runtime/Cpp/cmake/README.md) -- build instructions
- [vcpkg + Emscripten discussion](https://github.com/microsoft/vcpkg/discussions/40368) -- toolchain chaining approach
- [Emscripten C++23 module support issue](https://github.com/emscripten-core/emscripten/issues/21143) -- modules not supported, but `-std=c++23` flag works
- [Meson C++23 Emscripten issue](https://github.com/mesonbuild/meson/issues/13628) -- confirms `em++` accepts `-std=c++23`
- NES codebase analysis: `nes-common/CMakeLists.txt`, `nes-sql-parser/CMakeLists.txt`, `nes-frontend/CMakeLists.txt` -- dependency graph traced manually
