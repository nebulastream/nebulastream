# Phase 6: Validation Library Extraction - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract the NES validation pipeline (YAML parsing, catalog population, SQL parsing, source inference, type inference) into a standalone `nes-validator` library. The library compiles both natively (used by NES frontends) and to WebAssembly (used by topology editor). Single-function interface: `validateTopology(yamlString)` returns an error string or empty on success. NES frontends (nes-cli, nes-repl) are restructured to use the new library in this phase.

</domain>

<decisions>
## Implementation Decisions

### Validation depth
- Full semantic validation: YAML parsing → catalog population → SQL parsing → source inference → type inference
- Single entry point: `validateTopology(yamlString)` — no per-query or per-component validation (TS ANTLR parser handles inline syntax)
- Extract YAML binding logic from nes-cli starter as the front half of the pipeline (same deserialization path as the real engine)

### Error structure
- Simple string return: empty string = valid topology, non-empty string = error message
- Single try-catch around the entire validation pipeline — report the exception message
- No structured error objects, severity levels, or categorization for now — evolve later (would require NES-side changes)

### YAML parsing approach
- NES uses yaml-cpp for YAML parsing (not reflectcpp — reflectcpp is serialization only)
- All WASM dependencies via vcpkg with wasm32-emscripten triplet (consistent with NES approach)
- Migrate ANTLR4 from FetchContent to vcpkg in this phase (unify dependency management)

### Library placement
- New top-level CMake target: `nes-validator/` alongside nes-sql-parser/, nes-sources/, etc.
- Links existing NES targets (nes-sql-parser, nes-logical-operators, nes-data-types, etc.) as CMake dependencies — no source file duplication
- Stubs/facades only for heavy runtime deps (gRPC, Folly, cpptrace) that don't belong in validation
- Compiles both natively (for C++ unit tests and NES frontends) and to WASM (for topology editor)
- NES frontends (nes-cli, nes-repl) restructured to depend on nes-validator in this phase

### Claude's Discretion
- Exact stub/facade implementations for heavy deps
- CMake configuration details for dual native/WASM build
- vcpkg manifest and triplet configuration for yaml-cpp + ANTLR4
- C++ unit test structure and test cases
- How to restructure nes-frontend to use nes-validator

</decisions>

<specifics>
## Specific Ideas

- "Maybe we can factor out the YAML binding code in the nes-cli starter to use for the validation" — reuse the existing YAML→statements path, not a new parser
- "I would like to restructure the frontends to use the validator library" — nes-validator becomes the single source of truth for validation, used by both NES frontends natively and topology editor via WASM
- Keep it simple: just a string return for errors, single try-catch — matching how NES already propagates errors via exceptions

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `nes-frontend/src/Statements/StatementHandler.cpp`: Statement dispatch logic (visitor over variant types)
- `nes-sql-parser/src/StatementBinder.cpp`: Core validation — binds ANTLR AST to Statement variant
- `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp`: Builds LogicalPlan from ANTLR parse tree
- `nes-sources/include/Sources/SourceCatalog.hpp`: Source metadata registry (logical + physical)
- `nes-sinks/include/Sinks/SinkCatalog.hpp`: Sink metadata registry
- `nes-logical-operators/include/Plans/LogicalPlan.hpp`: LogicalPlan container
- `nes-data-types/include/DataTypes/Schema.hpp`: Schema and type system
- `nes-topology-editor/wasm/triplets/wasm32-emscripten.cmake`: vcpkg Emscripten triplet from Phase 5

### Established Patterns
- CMake with vcpkg: all NES dependencies managed through vcpkg with custom triplets
- C++23 via CMAKE_CXX_STANDARD 23
- std::expected used extensively in StatementHandler/StatementBinder for error propagation
- `-fwasm-exceptions` required on all WASM translation units (compile + link)
- Embind for JS-C++ interop (proven in Phase 5 spike)

### Integration Points
- `nes-validator/` — new top-level directory for the validation library
- Existing CMake targets to link: nes-sql-parser, nes-logical-operators, nes-data-types, nes-sources, nes-sinks, nes-common
- nes-frontend — will be restructured to depend on nes-validator
- `nes-topology-editor/wasm/` — WASM build will consume nes-validator

### Dependency Tree (for extraction)
```
nes-validator (new)
├── yaml-cpp (via vcpkg)
├── ANTLR4 C++ runtime (via vcpkg, migrated from FetchContent)
├── nes-sql-parser (existing)
├── nes-logical-operators (existing)
├── nes-data-types (existing)
├── nes-sources (existing, SourceCatalog)
├── nes-sinks (existing, SinkCatalog)
└── nes-common (existing, stubbing heavy deps)
```

### Heavy Deps Needing Stubs
- gRPC: only in nes-frontend layer, not in validation path
- Folly: `folly/hash/Hash.h` in SourceDescriptor — replace with std::hash
- cpptrace: `cpptrace/from_current.hpp` in ErrorHandling — compile-time disable
- spdlog: indirect via Logger — no control flow dependency, stub or disable

</code_context>

<deferred>
## Deferred Ideas

- Structured error objects with categories, severity, and location info — requires NES-side error handling changes (future v1.2)
- Error location mapping to specific UI nodes — deferred to v1.2 per REQUIREMENTS.md

</deferred>

---

*Phase: 06-validation-library-extraction*
*Context gathered: 2026-03-15*
