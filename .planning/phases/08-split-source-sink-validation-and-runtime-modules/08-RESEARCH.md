# Phase 8: Split Source/Sink Validation and Runtime Modules - Research

**Researched:** 2026-03-16
**Domain:** CMake target splitting, C++ module decomposition, WASM build dependency management
**Confidence:** HIGH

## Summary

Phase 8 splits the monolithic `nes-sources` and `nes-sinks` CMake targets into validation-only and runtime portions. Currently, each source/sink type (File, TCP, Generator, Network, Print, Checksum, Void) combines ConfigParameters definitions, validation registration functions, and runtime class implementations in a single .cpp file. The validation functions only need `nes-configurations` and `nes-common`, but they are trapped inside files that include heavy runtime headers (`Runtime/TupleBuffer.hpp`, `Runtime/AbstractBufferProvider.hpp`, `Sources/Source.hpp`, `folly/Synchronized.h`, `network/lib.h`, etc.).

The split extracts ConfigParameters structs and `RegisterXxxValidation()` free functions into separate validation-only files. These go into new `nes-sources-validation` and `nes-sinks-validation` CMake targets that link only `nes-configurations` + `nes-common`. The runtime targets (`nes-sources`, `nes-sinks`) link against the validation targets, inheriting the validation registrations. The WASM build then links the validation targets directly instead of compiling pass-through stubs (`source_validation_stubs.cpp`).

**Primary recommendation:** Create validation .hpp/.cpp file pairs for each source/sink type, build them into `nes-sources-validation` / `nes-sinks-validation` targets, wire them via the existing `create_registries_for_component` macro pattern, and replace WASM stubs with real validation target linkage.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Separate validation into dedicated .cpp files (e.g., FileSourceValidation.cpp) -- not same-file conditional compilation
- Remove private implementation headers (e.g., FileSource.hpp) since they're only included via the registry which doesn't use #includes -- the registry uses function declarations, not class headers
- ConfigParameters + validation registration live in a public header (e.g., FileSourceValidation.hpp) that both the validation target and the runtime .cpp include
- All source and sink types get the split -- File, TCP, Generator, Network sources + File, Print, Network, Checksum sinks
- Core sources (File) validation headers live in nes-sources/include/Sources/
- Plugin source/sink validation headers stay in their plugin directories (nes-plugins/Sources/TCPSource/, etc.)
- New nes-sources-validation target calls `create_registries_for_component(SourceValidation)` only
- New nes-sinks-validation target calls `create_registries_for_component(SinkValidation)` only
- nes-sources links against nes-sources-validation (no duplicate validation code)
- nes-sinks links against nes-sinks-validation (no duplicate validation code)
- Runtime targets inherit validation registrations from validation targets -- single ownership
- WASM CMake links nes-sources-validation and nes-sinks-validation directly instead of compiling stubs
- Delete source_validation_stubs.cpp from WASM build
- All source/sink types get real validation in WASM
- Strict: nes-sources-validation links ONLY nes-configurations + nes-common (no nes-memory, no nes-executable, no nes-data-types)
- Same for nes-sinks-validation: ONLY nes-configurations + nes-common
- If validation code needs anything from nes-executable/nes-memory, that's a sign the code needs refactoring
- Reuse existing folly stub from nes-validator/stubs/ for WASM compilation (SourceDescriptor.hpp includes folly/hash/Hash.h) -- no refactoring of SourceDescriptor

### Claude's Discretion
- Exact file naming for validation .cpp/.hpp files
- CMake configuration details for new targets
- How to wire plugin validation files into nes-sources-validation/nes-sinks-validation CMake targets
- How to handle include path ordering for stubs in WASM build
- Test adjustments if any are needed for the new target structure

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| VLIB-01 | NES validation pipeline extracted into standalone CMake target without runtime dependencies | This phase completes VLIB-01 by splitting source/sink validation from runtime, enabling real config validation (not stubs) in the WASM validator. The validation targets link only nes-configurations + nes-common, fulfilling "without runtime dependencies." |
</phase_requirements>

## Architecture Patterns

### Current Structure (What We're Changing)

Each source/sink has a single file that mixes validation and runtime:

```
nes-sources/
  private/FileSource.hpp          # ConfigParametersCSV + FileSource class (runtime headers)
  src/FileSource.cpp              # RegisterFileSourceValidation() + FileSource runtime methods

nes-plugins/Sources/TCPSource/
  TCPSource.hpp                   # ConfigParametersTCP + TCPSource class (runtime headers)
  TCPSource.cpp                   # RegisterTCPSourceValidation() + TCPSource runtime methods

nes-sinks/
  include/Sinks/FileSink.hpp      # ConfigParametersFile + FileSink class (runtime headers)
  src/FileSink.cpp                # RegisterFileSinkValidation() + FileSink runtime methods
```

**Problem:** The private headers include heavy runtime deps (`Runtime/TupleBuffer.hpp`, `Sources/Source.hpp`, `folly/Synchronized.h`, `network/lib.h`, `rust/cxx.h`) because ConfigParameters and class definition live in the same header.

### Target Structure (What We're Building)

```
nes-sources/
  include/Sources/
    FileSourceValidation.hpp       # ConfigParametersCSV + RegisterFileSourceValidation() declaration
    SourceValidationProvider.hpp   # (unchanged)
  src/
    FileSourceValidation.cpp       # RegisterFileSourceValidation() impl + validateAndFormat()
    FileSource.cpp                 # Runtime only: includes FileSourceValidation.hpp, NO ConfigParametersCSV def
    SourceValidationProvider.cpp   # (unchanged)
  private/
    FileSource.hpp                 # DELETE -- class decl moves to FileSource.cpp as internal-only
  CMakeLists.txt                   # nes-sources links nes-sources-validation

nes-sources-validation/            # NEW CMake target (virtual -- same directory, separate target)
  CMakeLists.txt                   # links ONLY nes-configurations + nes-common
                                   # calls create_registries_for_component(SourceValidation)
                                   # sources: FileSourceValidation.cpp + plugin validation .cpps

nes-plugins/Sources/TCPSource/
  TCPSourceValidation.hpp          # ConfigParametersTCP + RegisterTCPSourceValidation() decl
  TCPSourceValidation.cpp          # RegisterTCPSourceValidation() impl
  TCPSource.hpp                    # Runtime class only (includes TCPSourceValidation.hpp)
  TCPSource.cpp                    # Runtime methods only

nes-sinks/
  include/Sinks/
    FileSinkValidation.hpp         # ConfigParametersFile + RegisterFileSinkValidation() decl
    PrintSinkValidation.hpp        # ConfigParametersPrint + RegisterPrintSinkValidation() decl
  src/
    FileSinkValidation.cpp         # RegisterFileSinkValidation() impl
    PrintSinkValidation.cpp        # RegisterPrintSinkValidation() impl
    FileSink.cpp                   # Runtime only
    PrintSink.cpp                  # Runtime only

nes-sinks-validation/              # NEW CMake target
  CMakeLists.txt                   # links ONLY nes-configurations + nes-common

nes-plugins/Sinks/NetworkSink/
  NetworkSinkValidation.hpp        # ConfigParametersNetworkSink + decl
  NetworkSinkValidation.cpp        # impl
  ...
```

### Pattern: Validation File Split

For each source/sink type, create a validation header and validation .cpp:

**Validation Header (e.g., FileSourceValidation.hpp):**
```cpp
#pragma once
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>  // for SourceDescriptor::parameterMap

namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";

struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

/// Validates and formats a File source config map.
DescriptorConfig::Config validateFileSourceConfig(std::unordered_map<std::string, std::string> config);

} // namespace NES
```

**Validation .cpp (e.g., FileSourceValidation.cpp):**
```cpp
#include <Sources/FileSourceValidation.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

DescriptorConfig::Config validateFileSourceConfig(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), "File");
}

SourceValidationRegistryReturnType RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return validateFileSourceConfig(std::move(sourceConfig.config));
}

} // namespace NES
```

**Runtime .cpp (FileSource.cpp) changes:**
```cpp
// BEFORE: #include <FileSource.hpp>  (private header with ConfigParametersCSV + class)
// AFTER:
#include <Sources/FileSourceValidation.hpp>  // ConfigParametersCSV
// ... runtime-specific includes ...

namespace NES
{
// FileSource class definition moved inline here (was in private header)
class FileSource final : public Source { ... };

// Implementation methods unchanged
FileSource::FileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH)) { }
// ...

// Remove validateAndFormat() and RegisterFileSourceValidation() -- now in validation .cpp
// Keep SourceRegistryReturnType, InlineDataRegistryReturnType, FileDataRegistryReturnType
}
```

### Pattern: CMake Target Organization

The `nes-sources-validation` and `nes-sinks-validation` targets need to own the SourceValidation and SinkValidation registry components. Key insight from the existing `create_registries_for_component` macro:

```cmake
# create_registries_for_component derives COMPONENT_NAME from current directory name.
# So nes-sources-validation needs its own directory OR we use a manual approach.
```

**Recommended approach:** Create `nes-sources-validation/CMakeLists.txt` and `nes-sinks-validation/CMakeLists.txt` as new directories at the root level, mirroring nes-sources and nes-sinks structure.

```cmake
# nes-sources-validation/CMakeLists.txt
add_library(nes-sources-validation
    ${CMAKE_CURRENT_SOURCE_DIR}/../nes-sources/src/FileSourceValidation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../nes-sources/src/SourceValidationProvider.cpp
)

target_link_libraries(nes-sources-validation PUBLIC nes-common nes-configurations)

target_include_directories(nes-sources-validation PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-sources/include>
    $<INSTALL_INTERFACE:include/nebulastream/>
)

# Use nes-sources registry directory for SourceValidation registry
create_registries_for_component(SourceValidation)
```

**CRITICAL INSIGHT:** The `create_registries_for_component` macro derives `COMPONENT_NAME` from the current directory name (`get_filename_component(COMPONENT_NAME "${CMAKE_CURRENT_LIST_DIR}" NAME)`). This means:
- If CMakeLists.txt is in `nes-sources-validation/`, COMPONENT_NAME = `nes-sources-validation`
- The macro would look for `nes-sources-validation-registry` and templates in `nes-sources-validation/registry/templates/`
- This does NOT match the existing `nes-sources/registry/templates/SourceValidationGeneratedRegistrar.inc.in`

**Resolution options:**
1. Create a separate `nes-sources-validation/registry/` directory with just `SourceValidation` registry templates (symlinked or copied from nes-sources/registry)
2. Add the validation target within `nes-sources/CMakeLists.txt` itself (no new directory), using a more manual approach
3. Use `generate_plugin_registrars` directly instead of `create_registries_for_component`

**Recommended: Option 2** -- Keep validation target definition in `nes-sources/CMakeLists.txt`:

```cmake
# nes-sources/CMakeLists.txt (modified)
add_subdirectory(src)

# --- Validation-only target ---
add_library(nes-sources-validation
    src/FileSourceValidation.cpp
    src/SourceValidationProvider.cpp
)
target_link_libraries(nes-sources-validation PUBLIC nes-common nes-configurations)
target_include_directories(nes-sources-validation PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/nebulastream/>
)

# Registry for validation target -- only SourceValidation
create_plugin_registry_library(nes-sources-validation-registry nes-sources-validation)
target_link_libraries(nes-sources-validation PRIVATE nes-sources-validation-registry)
generate_plugin_registrars(nes-sources-validation SourceValidation)

# --- Runtime target ---
get_source(nes-sources NES_SOURCES_SOURCE_FILES)
add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-sources PUBLIC nes-sources-validation nes-common nes-configurations nes-memory nes-executable)
# ... rest unchanged, but remove SourceValidation from create_registries_for_component
create_registries_for_component(Source FileData InlineData)
```

**WAIT -- there's a subtlety.** The `create_plugin_registry_library` function creates a library called `nes-sources-validation-registry` but it expects to find the registry templates in `${CMAKE_CURRENT_SOURCE_DIR}/registry/templates/`. Since we're IN `nes-sources/`, this would look for `nes-sources/registry/templates/SourceValidationGeneratedRegistrar.inc.in` which ALREADY EXISTS. This should work.

The `generate_plugin_registrars` call generates the registrar for the `nes-sources-validation` target. The plugin registration (`add_plugin_as_library`) already creates separate libraries for SourceValidation (e.g., `tcp_source_validation_plugin_library`). These plugin libraries will be linked against `nes-sources-validation` by the registrar generator.

### Pattern: Plugin Validation Wiring

The plugins already register separate SourceValidation libraries:
```cmake
# nes-plugins/Sources/TCPSource/CMakeLists.txt
add_plugin_as_library(TCP SourceValidation nes-sources-registry tcp_source_validation_plugin_library TCPSource.cpp)
```

After the split, the validation plugin files change:
```cmake
add_plugin_as_library(TCP SourceValidation nes-sources-registry tcp_source_validation_plugin_library TCPSourceValidation.cpp)
```

The `add_plugin_as_library` links the plugin library against `nes-sources-registry` (which becomes `nes-sources-validation-registry` for validation). The `generate_plugin_registrar` function then links all `SourceValidation_plugin_libraries` against the validation target.

**CRITICAL DETAIL:** The `add_plugin_as_library` third parameter is `plugin_registry_component`. Currently it's `nes-sources-registry` for all source plugins. After the split, validation plugins should link against `nes-sources-validation-registry` (the validation registry library), not the full `nes-sources-registry`. This needs careful wiring.

Actually, looking more carefully at `add_plugin_as_library`:
```cmake
function(add_plugin_as_library plugin_name plugin_registry plugin_registry_component plugin_library)
    ...
    target_link_libraries(${plugin_library} PRIVATE ${plugin_registry_component})
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_libraries" "${plugin_library}")
endfunction()
```

The `plugin_registry` param (`SourceValidation`) is used as a global property prefix. The `plugin_registry_component` param is what the plugin links against. The registrar generator iterates `${plugin_registry}_plugin_names` and `${plugin_registry}_plugin_libraries`.

So the split needs to change `nes-sources-registry` to a component that the validation plugins can link against. Since the validation plugin .cpp files will only need headers from `nes-configurations` and `nes-common`, the registry component needs to provide those transitive deps.

### Anti-Patterns to Avoid

- **Including runtime headers in validation .hpp files:** ConfigParameters headers must NOT include `Runtime/TupleBuffer.hpp`, `Sources/Source.hpp`, `folly/Synchronized.h`, etc. They should only include `Configurations/Descriptor.hpp` and `Sources/SourceDescriptor.hpp` (for `SourceDescriptor::parameterMap`).
- **Duplicating validation code:** The runtime target must link against the validation target, not have its own copy of validation registration.
- **Breaking the registry macro system:** Don't hand-roll registry wiring. Work within the existing `create_registries_for_component` / `add_plugin_as_library` / `generate_plugin_registrars` system.

## Detailed Source/Sink Analysis

### Sources to Split

| Source | Current Location | ConfigParameters | Validation Fn | Runtime Deps in Header |
|--------|-----------------|------------------|---------------|----------------------|
| File | `nes-sources/private/FileSource.hpp` + `src/FileSource.cpp` | `ConfigParametersCSV` (FILEPATH) | `RegisterFileSourceValidation` | `Runtime/TupleBuffer.hpp`, `Runtime/AbstractBufferProvider.hpp`, `Sources/Source.hpp` |
| TCP | `nes-plugins/Sources/TCPSource/TCPSource.hpp` + `.cpp` | `ConfigParametersTCP` (HOST, PORT, DOMAIN, TYPE, SEPARATOR, etc.) | `RegisterTCPSourceValidation` | `Runtime/TupleBuffer.hpp`, `Sources/Source.hpp`, `netdb.h`, `sys/socket.h` |
| Generator | `nes-plugins/Sources/GeneratorSource/GeneratorSource.hpp` + `.cpp` | `ConfigParametersGenerator` (SEED, MAX_RUNTIME_MS, GENERATOR_SCHEMA, etc.) | `RegisterGeneratorSourceValidation` | `Runtime/TupleBuffer.hpp`, `Sources/Source.hpp`, `Generator.hpp`, `FixedGeneratorRate.hpp`, etc. |
| Network | `nes-plugins/Sources/NetworkSource/NetworkSource.hpp` + `.cpp` | `ConfigParametersNetworkSource` (CHANNEL, BIND, RECEIVER_QUEUE_SIZE) | `RegisterNetworkSourceValidation` | `network/lib.h`, `rust/cxx.h`, `Runtime/TupleBuffer.hpp` |

### Sinks to Split

| Sink | Current Location | ConfigParameters | Validation Fn | Runtime Deps in Header |
|------|-----------------|------------------|---------------|----------------------|
| File | `nes-sinks/include/Sinks/FileSink.hpp` + `src/FileSink.cpp` | `ConfigParametersFile` (FILE_PATH, APPEND) | `RegisterFileSinkValidation` | `folly/Synchronized.h`, `Runtime/TupleBuffer.hpp`, `Sinks/Sink.hpp`, `PipelineExecutionContext.hpp` |
| Print | `nes-sinks/include/Sinks/PrintSink.hpp` + `src/PrintSink.cpp` | `ConfigParametersPrint` (INGESTION, INPUT_FORMAT) | `RegisterPrintSinkValidation` | `folly/Synchronized.h`, `Runtime/TupleBuffer.hpp`, `Sinks/Sink.hpp` |
| Network | `nes-plugins/Sinks/NetworkSink/NetworkSink.hpp` + `.cpp` | `ConfigParametersNetworkSink` (DATA_ENDPOINT, BIND, CHANNEL, etc.) | `RegisterNetworkSinkValidation` | `folly/Synchronized.h`, `network/lib.h`, `rust/cxx.h`, `Runtime/TupleBuffer.hpp` |
| Checksum | `nes-plugins/Sinks/ChecksumSink/ChecksumSink.hpp` + `.cpp` | `ConfigParametersChecksum` (FILE_PATH) | `RegisterChecksumSinkValidation` | `Runtime/TupleBuffer.hpp`, `Sinks/Sink.hpp`, `SinksParsing/Format.hpp` |
| Void | `nes-plugins/Sinks/VoidSink/VoidSink.hpp` + `.cpp` | `ConfigParametersVoid` (empty) | `RegisterVoidSinkValidation` | `Runtime/TupleBuffer.hpp`, `Sinks/Sink.hpp` |

### Header Dependency Analysis for Validation Files

Validation headers need ONLY:
- `<Configurations/Descriptor.hpp>` -- for `DescriptorConfig::ConfigParameter`, `ConfigParameterContainer`, `validateAndFormat<>()`
- `<Sources/SourceDescriptor.hpp>` or `<Sinks/SinkDescriptor.hpp>` -- for `parameterMap` base (used in `createConfigParameterContainerMap`)

**Potential problem:** `SourceDescriptor.hpp` and `SinkDescriptor.hpp` may include heavy headers.

Let me verify -- `SourceDescriptor.hpp` includes `folly/hash/Hash.h` (confirmed in CONTEXT.md). This is handled by the existing folly stub in `nes-validator/stubs/`. For the native build, `SourceDescriptor.hpp` is part of `nes-sources` which already links folly transitively. The validation targets will need `SourceDescriptor.hpp` accessible -- it lives in `nes-sources/include/Sources/SourceDescriptor.hpp` which is a public header of `nes-sources`. But `nes-sources-validation` needs to provide this include path too.

**Key insight:** `nes-sources-validation` must provide `nes-sources/include` as a PUBLIC include directory so that validation .cpp files can include `<Sources/SourceDescriptor.hpp>`. Since `nes-sources-validation`'s CMakeLists.txt is in `nes-sources/`, this is `${CMAKE_CURRENT_SOURCE_DIR}/include`.

### TCP ConfigParameters -- System Header Issue

`ConfigParametersTCP` in `TCPSource.hpp` uses `AF_INET`, `AF_INET6`, `SOCK_STREAM`, etc. from `<sys/socket.h>` and `<netdb.h>`. These are POSIX headers that are NOT available in Emscripten/WASM.

**This is a CRITICAL issue for WASM compilation.** The validation function for TCP needs these constants to validate socket domain/type, but they are platform-specific.

**Resolution options:**
1. Replace POSIX constants with integer literals in ConfigParametersTCP (e.g., `2` for AF_INET, `1` for SOCK_STREAM) -- **not recommended**, loses readability
2. Use `#ifdef __EMSCRIPTEN__` to provide constant definitions -- adds platform coupling to validation code
3. Provide a WASM stub header for `<sys/socket.h>` and `<netdb.h>` that defines the constants -- **recommended**, matches existing stub pattern
4. Move the validation lambda for socket domain/type to just accept any string value in WASM -- defeats the purpose

**Recommended:** Option 3 -- Add minimal stub headers to `nes-topology-editor/wasm/nes-validator/stubs/` (or `nes-validator/stubs/`) that define `AF_INET`, `AF_INET6`, `SOCK_STREAM`, etc. as integer constants. The WASM build already uses stub include path ordering (`BEFORE PRIVATE`) to override system headers.

Actually, checking more carefully: Emscripten DOES provide `<sys/socket.h>` and `<netdb.h>` with POSIX constants. The Emscripten SDK has comprehensive POSIX header support. So this may not be an issue at all. The validation lambdas for TCP only USE the constants for comparison, they don't call socket functions.

**Confidence: MEDIUM** -- Emscripten likely provides these headers. If not, stub headers are the fallback.

### Generator ConfigParameters -- Helper Dependencies

`ConfigParametersGenerator` validation lambdas reference:
- `SinusGeneratorRate::parseAndValidateConfigString()` -- from `SinusGeneratorRate.hpp`
- `FixedGeneratorRate::parseAndValidateConfigString()` -- from `FixedGeneratorRate.hpp`
- `GeneratorFields::Validators` -- from `GeneratorFields.hpp`
- `magic_enum::enum_cast<GeneratorFields::FieldIdentifier>` -- magic_enum
- `NES::toUpperCase`, `splitOnMultipleDelimiters`, `trimWhiteSpaces` -- from `Util/Strings.hpp`

These helper functions are part of the Generator plugin's runtime code. They need to either:
1. Be split into validation-compatible helpers (no runtime deps)
2. Be compiled into the validation target along with the Generator validation

Looking at the existing plugin CMake: `add_plugin_as_library(Generator SourceValidation nes-sources-registry generator_validation_plugin_library GeneratorSource.cpp Generator.cpp GeneratorFields.cpp FixedGeneratorRate.cpp SinusGeneratorRate.cpp)` -- it already includes all these helper files in the validation plugin library!

After the split, the validation plugin library should include the VALIDATION .cpp plus the helper files:
```cmake
add_plugin_as_library(Generator SourceValidation nes-sources-registry generator_validation_plugin_library
    GeneratorSourceValidation.cpp Generator.cpp GeneratorFields.cpp FixedGeneratorRate.cpp SinusGeneratorRate.cpp)
```

**Potential issue:** `Generator.cpp`, `GeneratorFields.cpp`, `FixedGeneratorRate.cpp`, `SinusGeneratorRate.cpp` may include runtime headers. Need to verify whether the validation path through these files actually exercises runtime code.

The GENERATOR_SCHEMA validation lambda calls `GeneratorFields::Validators` which validates field definitions. The GENERATOR_RATE_CONFIG lambda calls `SinusGeneratorRate::parseAndValidateConfigString()` and `FixedGeneratorRate::parseAndValidateConfigString()`. If these static methods don't depend on runtime (TupleBuffer, etc.), they can compile in the validation target.

**For WASM:** The current WASM build does NOT include Generator helper files. The stub `RegisterGeneratorSourceValidation` is pass-through. After Phase 8, real Generator validation would need these helpers compiled for WASM. This may pull in additional dependencies.

**Pragmatic approach:** If Generator helper files have runtime deps, keep the stub behavior for Generator in WASM (accept any config) and only enable real validation for File, TCP, and simpler sources. But the user decision says "All source/sink types get real validation in WASM."

**Confidence: MEDIUM** -- Generator validation helpers may have runtime deps that complicate WASM compilation. Investigate during implementation.

### NetworkSource ConfigParameters -- UUID and Endpoint Validation

`ConfigParametersNetworkSource` validation uses:
- `stringToUUID()` from `<Util/UUID.hpp>` -- for CHANNEL validation
- `EndpointValidation{}.isValid()` from `<Configurations/Validation/EndpointValidation.hpp>` -- for BIND validation

These are utility functions that may or may not have heavy deps. They're from `nes-common` and `nes-configurations` respectively, which are allowed deps for the validation target.

## Common Pitfalls

### Pitfall 1: Registry Macro Interaction
**What goes wrong:** `create_registries_for_component` derives COMPONENT_NAME from directory name. Creating a new target in the same directory can conflict.
**Why it happens:** The macro calls `get_filename_component(COMPONENT_NAME "${CMAKE_CURRENT_LIST_DIR}" NAME)`, so all targets defined in `nes-sources/CMakeLists.txt` get COMPONENT_NAME = `nes-sources`.
**How to avoid:** Use the lower-level `create_plugin_registry_library` and `generate_plugin_registrars` functions directly instead of `create_registries_for_component` for the validation target. Or create the validation target in a subdirectory.
**Warning signs:** CMake configure errors about duplicate targets or missing templates.

### Pitfall 2: Circular Include Between Validation and Runtime Headers
**What goes wrong:** Validation header includes something from runtime, runtime includes validation header -- circular dependency.
**Why it happens:** Easy to accidentally include a runtime header when extracting ConfigParameters.
**How to avoid:** Validation headers include ONLY: `<Configurations/Descriptor.hpp>`, `<Sources/SourceDescriptor.hpp>` (or SinkDescriptor), and standard library headers. Verify each validation header compiles standalone with only `nes-configurations` and `nes-common`.
**Warning signs:** Link errors about undefined symbols from runtime libraries.

### Pitfall 3: Static Initialization Order Fiasco
**What goes wrong:** ConfigParameters use `static inline` members with initializers that depend on other statics (e.g., `SourceDescriptor::parameterMap`).
**Why it happens:** When validation and runtime are in separate TUs/libraries, static init order may change.
**How to avoid:** `ConfigParametersCSV::parameterMap` calls `createConfigParameterContainerMap(SourceDescriptor::parameterMap, ...)`. Since `SourceDescriptor::parameterMap` is also `static inline`, both are initialized when first accessed. The `static inline` pattern is actually safe here because both are in header files -- they're initialized in the TU that first includes them.
**Warning signs:** Empty parameter maps at runtime, validation rejecting valid configs.

### Pitfall 4: WASM Build Picking Up Wrong .cpp Files
**What goes wrong:** WASM CMake manually lists source files. Forgetting to update the file lists after the split causes link errors or uses stale stubs.
**Why it happens:** The WASM build (`nes-topology-editor/wasm/nes-validator/CMakeLists.txt`) cherry-picks individual .cpp files rather than using CMake targets.
**How to avoid:** After creating validation targets that compile for WASM, the WASM CMakeLists.txt should link the validation targets as libraries (if Emscripten supports this) or at minimum replace the stub .cpp with the real validation .cpp files.
**Warning signs:** Linker errors about undefined RegisterXxxValidation symbols, or validation still passing invalid configs.

### Pitfall 5: Plugin Library Linking to Wrong Registry Component
**What goes wrong:** Plugin `add_plugin_as_library` calls still reference `nes-sources-registry` instead of `nes-sources-validation-registry` for validation plugins.
**Why it happens:** After splitting, the registry component for validation is `nes-sources-validation-registry`, but existing plugin CMake files reference `nes-sources-registry`.
**How to avoid:** Update all plugin CMakeLists.txt to reference the correct registry component. The validation plugins should link against the validation registry component.
**Warning signs:** Validation registrations not appearing in the validator; linker errors in plugin libraries.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Registry wiring | Custom registration code | `create_registries_for_component`, `generate_plugin_registrars` macros | Existing macro system generates registrar headers, handles plugin discovery |
| Config validation | Manual field checking | `DescriptorConfig::validateAndFormat<ConfigParams>()` | Already handles required vs optional, type parsing, error reporting |
| WASM header stubs | Inline #ifdef guards | Stub headers in `stubs/` directory with BEFORE include path | Established pattern from Phase 6, cleanly overrides system headers |

## WASM Build Integration

### Current WASM Build (to be replaced)

The WASM CMakeLists.txt currently:
1. Manually lists NES source .cpp files (cherry-picked per module)
2. Includes `source_validation_stubs.cpp` with pass-through validation functions
3. Has NO sink validation stubs (sinks are not validated in current WASM build)
4. Uses `generated/SourceValidationGeneratedRegistrar.inc` copied from native build

### Target WASM Build

After Phase 8, the WASM CMakeLists.txt should:
1. Replace `source_validation_stubs.cpp` with real validation .cpp files (e.g., `FileSourceValidation.cpp`, `TCPSourceValidation.cpp`, etc.)
2. Add sink validation .cpp files (e.g., `FileSinkValidation.cpp`, `PrintSinkValidation.cpp`, etc.)
3. Remove `source_validation_stubs.cpp` from the build
4. Ensure stub include paths still work for `SourceDescriptor.hpp`'s folly include

### WASM File List Changes

**Remove:**
- `source_validation_stubs.cpp`

**Add (sources validation):**
- `${NES_ROOT}/nes-sources/src/FileSourceValidation.cpp`
- Plugin validation .cpp files from nes-plugins/Sources/*/

**Add (sinks validation):**
- `${NES_ROOT}/nes-sinks/src/FileSinkValidation.cpp`
- `${NES_ROOT}/nes-sinks/src/PrintSinkValidation.cpp`
- Plugin validation .cpp files from nes-plugins/Sinks/*/

**Note:** The WASM build does NOT use CMake targets from the NES tree (it compiles everything with Emscripten toolchain). So "linking validation targets" means adding the validation .cpp files to the WASM executable's source list and ensuring include paths are correct.

## Code Examples

### Extracting ConfigParametersCSV from FileSource.hpp

**Before (nes-sources/private/FileSource.hpp):**
```cpp
// Contains BOTH ConfigParametersCSV and FileSource class
// FileSource class includes runtime headers (TupleBuffer, Source, etc.)
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
// ...
struct ConfigParametersCSV { ... };
class FileSource final : public Source { ... };
```

**After -- Validation header (nes-sources/include/Sources/FileSourceValidation.hpp):**
```cpp
#pragma once
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{
static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";

struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};
}
```

**After -- Validation .cpp (nes-sources/src/FileSourceValidation.cpp):**
```cpp
#include <Sources/FileSourceValidation.hpp>
#include <utility>
#include <SourceValidationRegistry.hpp>

namespace NES
{
SourceValidationRegistryReturnType RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(sourceConfig.config), "File");
}
}
```

**After -- Runtime .cpp (nes-sources/src/FileSource.cpp) removes:**
```cpp
// REMOVED: ConfigParametersCSV definition (now in FileSourceValidation.hpp)
// REMOVED: RegisterFileSourceValidation() function (now in FileSourceValidation.cpp)
// ADDED: #include <Sources/FileSourceValidation.hpp>
// FileSource class definition moved here from deleted private/FileSource.hpp
```

### CMake Validation Target

```cmake
# In nes-sources/CMakeLists.txt -- add before runtime target

# Validation-only library (no runtime deps)
add_library(nes-sources-validation
    src/FileSourceValidation.cpp
    src/SourceValidationProvider.cpp
)
target_link_libraries(nes-sources-validation PUBLIC nes-common nes-configurations)
target_include_directories(nes-sources-validation PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/nebulastream/>
)

# Create registry ONLY for SourceValidation
create_plugin_registry_library(nes-sources-validation-registry nes-sources-validation)
target_link_libraries(nes-sources-validation PRIVATE nes-sources-validation-registry)
generate_plugin_registrars(nes-sources-validation SourceValidation)

# Runtime target now links validation target
# ...
target_link_libraries(nes-sources PUBLIC nes-sources-validation nes-common nes-configurations nes-memory nes-executable)
# Remove SourceValidation from this list:
create_registries_for_component(Source FileData InlineData)
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Google Test (gtest/gmock) via CTest |
| Config file | CMakeLists.txt `add_tests_if_enabled(tests)` pattern |
| Quick run command | `cd /IdeaProjects/nebulastream2/nes-build && ctest -R TopologyValidatorTest -j4 --output-on-failure` |
| Full suite command | `cd /IdeaProjects/nebulastream2/nes-build && ctest -j4 --output-on-failure` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VLIB-01a | nes-sources-validation compiles independently | build | `cmake --build nes-build --target nes-sources-validation` | Wave 0 |
| VLIB-01b | nes-sinks-validation compiles independently | build | `cmake --build nes-build --target nes-sinks-validation` | Wave 0 |
| VLIB-01c | File source missing file_path is rejected | unit | `ctest -R TopologyValidatorTest -j1 --output-on-failure` | Need new test |
| VLIB-01d | TCP source missing socket_host/socket_port is rejected | unit | `ctest -R TopologyValidatorTest -j1 --output-on-failure` | Need new test |
| VLIB-01e | All existing C++ tests pass | integration | `ctest -j4 --output-on-failure` | Existing |

### Sampling Rate
- **Per task commit:** `cmake --build nes-build --target nes-sources-validation nes-sinks-validation && ctest -R TopologyValidatorTest -j1 --output-on-failure`
- **Per wave merge:** `ctest -j4 --output-on-failure`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] New test cases in `nes-validator/tests/UnitTests/TopologyValidatorTest.cpp` -- File source missing file_path, TCP source missing socket_host/socket_port
- [ ] WASM build verification (manual -- `em++ relink` or Emscripten cmake)

## Open Questions

1. **Generator validation helpers and WASM compatibility**
   - What we know: Generator validation uses `SinusGeneratorRate::parseAndValidateConfigString()`, `FixedGeneratorRate::parseAndValidateConfigString()`, `GeneratorFields::Validators`, and `magic_enum`. The current plugin CMake already includes these .cpp files in the validation plugin library.
   - What's unclear: Whether these helper files have runtime dependencies that prevent WASM compilation. The WASM build currently uses a pass-through stub for Generator validation.
   - Recommendation: During implementation, try compiling Generator validation helpers for WASM. If they have runtime deps, either stub them for WASM or refactor the validation lambdas to avoid calling them.

2. **Emscripten POSIX header availability for TCP validation**
   - What we know: TCP ConfigParameters uses `AF_INET`, `SOCK_STREAM`, etc. from `<sys/socket.h>`. Emscripten provides POSIX headers.
   - What's unclear: Whether the specific constants used in TCP validation are available in Emscripten's `<sys/socket.h>`.
   - Recommendation: Try compiling and fall back to stub constants if needed.

3. **Registry component naming for split**
   - What we know: `create_registries_for_component` derives component name from directory. `add_plugin_as_library` third param is the registry component to link against.
   - What's unclear: Exact changes needed in each plugin CMakeLists.txt to wire SourceValidation to `nes-sources-validation` instead of `nes-sources`.
   - Recommendation: Use `create_plugin_registry_library` and `generate_plugin_registrars` directly for validation targets. May need to update the third argument in `add_plugin_as_library` calls for validation plugins.

## Sources

### Primary (HIGH confidence)
- `nes-sources/CMakeLists.txt` -- current CMake target structure, `create_registries_for_component(Source SourceValidation FileData InlineData)`
- `nes-sinks/CMakeLists.txt` -- current sink CMake target, `create_registries_for_component(Sink SinkValidation)`
- `cmake/PluginRegistrationUtil.cmake` -- full macro source: `create_registries_for_component`, `add_plugin_as_library`, `generate_plugin_registrars`
- `nes-sources/private/FileSource.hpp` -- ConfigParametersCSV definition (extraction source)
- `nes-plugins/Sources/TCPSource/TCPSource.hpp` -- ConfigParametersTCP definition (extraction source)
- `nes-plugins/Sources/GeneratorSource/GeneratorSource.hpp` -- ConfigParametersGenerator definition
- `nes-sources/src/SourceValidationProvider.cpp` -- delegates to SourceValidationRegistry
- `nes-topology-editor/wasm/nes-validator/CMakeLists.txt` -- WASM build, cherry-picked .cpp files, stub includes
- `nes-topology-editor/wasm/nes-validator/source_validation_stubs.cpp` -- stubs to be replaced
- All plugin CMakeLists.txt files -- current `add_plugin_as_library` calls

### Secondary (MEDIUM confidence)
- Emscripten POSIX header support -- based on known Emscripten behavior, not verified for specific constants

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- pure CMake refactoring using existing NES patterns
- Architecture: HIGH -- direct code evidence for all split points and dependency chains
- Pitfalls: HIGH -- identified from actual macro source code and header dependency analysis
- WASM integration: MEDIUM -- some unknowns around Generator helpers and TCP POSIX constants

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable -- CMake and C++ patterns don't change fast)
