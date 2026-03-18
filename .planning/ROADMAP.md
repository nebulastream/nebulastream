# Roadmap: NebulaStream Topology Editor UI

## Milestones

- v1.0 MVP - Phases 1-4 (shipped 2026-03-15)
- v1.1 Shared Validation via WebAssembly - Phases 5-8 (in progress)

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>v1.0 MVP (Phases 1-4) - SHIPPED 2026-03-15</summary>

- [x] **Phase 1: Interactive Canvas** - React Flow canvas with draggable workers, sources, sinks, connections, and layout features (completed 2026-03-14)
- [x] **Phase 2: Configuration Panels** - Property panels, logical source management, schema builder, and SQL query editor (completed 2026-03-15)
- [x] **Phase 3: YAML Pipeline** - Live YAML preview, export/download, import with bidirectional sync (completed 2026-03-15)
- [x] **Phase 4: Polish and Validation** - History nav, keyboard shortcuts, resizable panels, dark mode, ANTLR SQL validation, and topology error highlighting (completed 2026-03-15)

</details>

### v1.1 Shared Validation via WebAssembly (In Progress)

**Milestone Goal:** Extract the NES validation pipeline into a standalone C++ library, compile it to WebAssembly, and wire it into the topology editor so the UI uses the same validation logic as the NES engine.

- [x] **Phase 5: Proof-of-Concept Spikes** - Validate C++23/Emscripten compatibility, ANTLR4 WASM compilation, and Embind integration before committing to full extraction (completed 2026-03-15)
- [x] **Phase 6: Validation Library Extraction** - Extract NES validation pipeline into standalone CMake target, compile to WebAssembly with Embind interface (completed 2026-03-15)
- [ ] **Phase 7: Browser Integration and Testing** - Wire WASM module into topology editor with Web Worker execution, error display, and CI pipeline
- [x] **Phase 8: Split Source/Sink Validation and Runtime Modules** - Extract validation-only CMake targets from nes-sources and nes-sinks so WASM validator can link real config validation (completed 2026-03-16)

## Phase Details

### Phase 5: Proof-of-Concept Spikes
**Goal**: The three highest-risk technical unknowns (C++23/Emscripten, ANTLR4 WASM, Embind integration) are validated or rejected before investing in full extraction
**Depends on**: Phase 4
**Requirements**: WASM-01, WASM-02
**Plans:** 2/2 plans complete
Plans:
- [ ] 05-01-PLAN.md — Emscripten infrastructure, C++23 spike, and Embind spike
- [ ] 05-02-PLAN.md — ANTLR4 C++ runtime WASM compilation and SQL parsing spike
**Success Criteria** (what must be TRUE):
  1. A C++ file using std::expected compiles with Emscripten and runs correctly in Node.js
  2. The ANTLR4 C++ runtime compiles to WebAssembly and can parse a NES SQL string in Node.js
  3. A trivial Embind module loads in Node.js and returns a value to JavaScript (Vite/browser integration deferred to Phase 7)
  4. Baseline WASM binary size is measured and a path to the 1MB gzipped budget is credible

### Phase 6: Validation Library Extraction
**Goal**: The NES validation pipeline (YAML parsing, catalog population, SQL parsing, source inference, type inference) exists as a standalone library that compiles to a working WebAssembly module with a single-function interface
**Depends on**: Phase 5
**Requirements**: VLIB-01, VLIB-02, VLIB-03, VLIB-04, VLIB-05, VLIB-06, WASM-03, WASM-04, TEST-01
**Plans:** 4/4 plans complete
Plans:
- [ ] 06-01-PLAN.md — CMake target skeleton, stubs/facades, test scaffold
- [ ] 06-02-PLAN.md — Full validation pipeline (YAML binding, catalog, SQL validation) and unit tests
- [ ] 06-03-PLAN.md — WASM build with Embind, vcpkg ANTLR4 migration, and Node.js integration tests
- [ ] 06-04-PLAN.md — Restructure nes-cli to use nes-validator for offline validation
**Success Criteria** (what must be TRUE):
  1. A standalone nes-validator CMake target compiles without any NES runtime dependencies (no gRPC, Folly, cpptrace)
  2. Calling validateTopology(yamlString) from JavaScript returns a structured error list for known-bad YAML (missing sources, type mismatches, invalid SQL)
  3. Calling validateTopology(yamlString) with valid topology YAML returns an empty error list
  4. The WASM binary is under 1MB gzipped
  5. C++ unit tests pass for the validation library (catalog population, SQL parsing, type inference)
  6. nes-cli uses nes-validator for offline topology validation

### Phase 7: Browser Integration and Testing
**Goal**: The topology editor validates topologies using the WASM module in real time, with validation running in a Web Worker and errors displayed on the canvas
**Depends on**: Phase 6
**Requirements**: BINT-01, BINT-02, BINT-03, BINT-04, TEST-02, TEST-03
**Plans:** 1/2 plans executed
Plans:
- [ ] 07-01-PLAN.md — WASM rebuild for browser/worker, Web Worker, validation slice, Vite config
- [ ] 07-02-PLAN.md — UI integration (status bar, error display, debounced hook, tests, checkpoint)
**Success Criteria** (what must be TRUE):
  1. The WASM module loads automatically when the topology editor starts, with the UI remaining responsive during loading
  2. When the user modifies the topology, validation runs automatically (debounced) and semantic errors from the WASM module appear as error overlays on the canvas
  3. Validation runs in a Web Worker and does not block UI interactions (dragging, typing, panel switching)
  4. A CI job builds the WASM module and runs integration tests on every relevant PR

### Phase 8: Split Source/Sink Validation and Runtime Modules
**Goal**: nes-sources and nes-sinks are split into validation-only and runtime CMake targets. The WASM validator links the validation targets directly, enabling real config field validation (e.g., file_path required for File source) without runtime stubs.
**Depends on**: Phase 7
**Requirements**: VLIB-01
**Plans:** 2/2 plans complete
Plans:
- [x] 08-01-PLAN.md — Extract validation files for all sources/sinks, create nes-sources-validation and nes-sinks-validation CMake targets
- [x] 08-02-PLAN.md — Wire WASM build to real validation, update native nes-validator deps, add config field validation tests
**Success Criteria** (what must be TRUE):
  1. nes-sources-validation and nes-sinks-validation CMake targets exist and compile independently of runtime dependencies (TupleBuffer, BackpressureChannel, etc.)
  2. nes-sources and nes-sinks link against their respective validation targets (no code duplication)
  3. The WASM validator links nes-sources-validation and nes-sinks-validation instead of using stubs for source/sink validation
  4. validateTopology() rejects File sources missing file_path and TCP sources missing socket_host/socket_port
  5. All existing C++ tests pass without modification

## Progress

**Execution Order:**
Phases execute in numeric order: 5 -> 6 -> 7 -> 8

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Interactive Canvas | v1.0 | 7/7 | Complete | 2026-03-14 |
| 2. Configuration Panels | v1.0 | 5/5 | Complete | 2026-03-15 |
| 3. YAML Pipeline | v1.0 | 4/4 | Complete | 2026-03-15 |
| 4. Polish and Validation | v1.0 | 5/5 | Complete | 2026-03-15 |
| 5. Proof-of-Concept Spikes | 2/2 | Complete    | 2026-03-15 | - |
| 6. Validation Library Extraction | 4/4 | Complete   | 2026-03-15 | - |
| 7. Browser Integration and Testing | 1/2 | In Progress|  | - |
| 8. Split Source/Sink Validation and Runtime | 2/2 | Complete    | 2026-03-16 |
