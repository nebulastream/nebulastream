# Phase 8: Split Source/Sink Validation and Runtime Modules - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Split nes-sources and nes-sinks into validation-only and runtime CMake targets. The WASM validator links the validation targets directly, replacing pass-through stubs with real config field validation (e.g., file_path required for File source, socket_host/socket_port required for TCP source). All existing C++ tests must pass without modification.

</domain>

<decisions>
## Implementation Decisions

### Split granularity
- Separate validation into dedicated .cpp files (e.g., FileSourceValidation.cpp) — not same-file conditional compilation
- Remove private implementation headers (e.g., FileSource.hpp) since they're only included via the registry which doesn't use #includes — the registry uses function declarations, not class headers
- ConfigParameters + validation registration live in a public header (e.g., FileSourceValidation.hpp) that both the validation target and the runtime .cpp include
- All source and sink types get the split — File, TCP, Generator, Network sources + File, Print, Network, Checksum sinks
- Core sources (File) validation headers live in nes-sources/include/Sources/
- Plugin source/sink validation headers stay in their plugin directories (nes-plugins/Sources/TCPSource/, etc.)

### Registry wiring
- New nes-sources-validation target calls `create_registries_for_component(SourceValidation)` only
- New nes-sinks-validation target calls `create_registries_for_component(SinkValidation)` only
- nes-sources links against nes-sources-validation (no duplicate validation code)
- nes-sinks links against nes-sinks-validation (no duplicate validation code)
- Runtime targets inherit validation registrations from validation targets — single ownership

### WASM stub replacement
- WASM CMake links nes-sources-validation and nes-sinks-validation directly instead of compiling stubs
- Delete source_validation_stubs.cpp from WASM build
- All source/sink types get real validation in WASM (File, TCP, Generator, Network sources + all sinks)
- Users get real config field validation for any source/sink type used in the topology editor

### Dependency boundaries
- Strict: nes-sources-validation links ONLY nes-configurations + nes-common (no nes-memory, no nes-executable, no nes-data-types)
- Same for nes-sinks-validation: ONLY nes-configurations + nes-common
- If validation code needs anything from nes-executable/nes-memory, that's a sign the code needs refactoring
- Reuse existing folly stub from nes-validator/stubs/ for WASM compilation (SourceDescriptor.hpp includes folly/hash/Hash.h) — no refactoring of SourceDescriptor

### Claude's Discretion
- Exact file naming for validation .cpp/.hpp files
- CMake configuration details for new targets
- How to wire plugin validation files into nes-sources-validation/nes-sinks-validation CMake targets
- How to handle include path ordering for stubs in WASM build
- Test adjustments if any are needed for the new target structure

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source/sink structure
- `nes-sources/CMakeLists.txt` — Current nes-sources CMake target, registry macros, link deps
- `nes-sinks/CMakeLists.txt` — Current nes-sinks CMake target, registry macros, link deps
- `nes-sources/src/FileSource.cpp` — Example of validation + runtime in one file (pattern to split)
- `nes-sources/src/SourceValidationProvider.cpp` — Validation provider that delegates to registry
- `nes-sources/include/Sources/SourceValidationProvider.hpp` — Validation provider interface

### Plugin sources/sinks
- `nes-plugins/Sources/TCPSource/TCPSource.cpp` — Plugin source with validation registration
- `nes-plugins/Sources/GeneratorSource/GeneratorSource.cpp` — Plugin source with validation registration
- `nes-plugins/Sources/NetworkSource/NetworkSource.cpp` — Plugin source with validation registration
- `nes-plugins/Sinks/NetworkSink/NetworkSink.cpp` — Plugin sink with validation registration
- `nes-plugins/Sinks/ChecksumSink/ChecksumSink.cpp` — Plugin sink with validation registration

### Validator and WASM build
- `nes-validator/CMakeLists.txt` — Current validator CMake target, link deps, stub include paths
- `nes-topology-editor/wasm/nes-validator/CMakeLists.txt` — WASM build CMake (cherry-picks .cpp files)
- `nes-topology-editor/wasm/nes-validator/source_validation_stubs.cpp` — Stubs to be replaced
- `nes-topology-editor/wasm/nes-validator/descriptor_stubs.cpp` — Descriptor stubs (may need updates)

### Registry system
- `nes-sources/registry/include/SourceValidationRegistry.hpp` — SourceValidation registry definition
- `nes-sinks/registry/include/SinkValidationRegistry.hpp` — SinkValidation registry definition

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SourceValidationProvider`: Delegates to SourceValidationRegistry — already the right abstraction for validation-only use
- `create_registries_for_component()` CMake macro: Auto-generates registry classes, supports selective component creation
- `DescriptorConfig::validateAndFormat<ConfigParams>()`: Template function that validates config maps against ConfigParameter definitions — this is what validation functions call
- Existing folly/cpptrace stubs in `nes-validator/stubs/`: Reusable for validation target WASM compilation

### Established Patterns
- Plugin registry pattern: `RegisterXxxValidation()` free functions auto-registered at static init time via registry macros
- ConfigParameters structs (e.g., `ConfigParametersCSV`, `ConfigParametersTCP`): Define required/optional config fields with types and defaults
- `add_plugin()` CMake macro: Registers plugin .cpp files for specific registry components

### Integration Points
- nes-sources-validation (new) — linked by nes-sources (runtime) and nes-validator (native + WASM)
- nes-sinks-validation (new) — linked by nes-sinks (runtime) and nes-validator (native + WASM)
- WASM CMake needs to link validation targets instead of compiling individual .cpp files + stubs
- nes-validator CMakeLists.txt may need to add explicit deps on validation targets

</code_context>

<specifics>
## Specific Ideas

- "We can remove the source implementation hpp, there's no reason to have it as it's only included via the registry which doesn't use #includes"
- ConfigParameters need to live in a header so both validation target and runtime implementation can include them
- The header should also have the implementation for the registration function

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 08-split-source-sink-validation-and-runtime-modules*
*Context gathered: 2026-03-16*
