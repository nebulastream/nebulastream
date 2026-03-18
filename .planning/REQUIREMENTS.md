# Requirements: NebulaStream Topology Editor UI

**Defined:** 2026-03-13
**Core Value:** Users can visually design NES topologies and get valid YAML files for nes-cli

## v1.0 Requirements (Complete)

### Canvas

- [x] **CANV-01**: User can drag worker nodes onto the canvas from a palette
- [x] **CANV-02**: User can drag source/sink nodes onto the canvas from a palette
- [x] **CANV-03**: User can reposition nodes by dragging
- [x] **CANV-04**: User can connect workers by dragging one onto another (creates downstream link)
- [x] **CANV-05**: User can attach sources/sinks to workers by dragging them onto a worker
- [x] **CANV-06**: User can select and delete nodes/edges
- [x] **CANV-07**: Workers, sources, and sinks are visually distinct (size, color)
- [x] **CANV-08**: Invalid connections are prevented (edge validation)
- [x] **CANV-09**: Nodes snap to grid for clean alignment
- [x] **CANV-10**: Minimap shows thumbnail overview of the topology
- [x] **CANV-11**: Auto-layout arranges nodes hierarchically
- [x] **CANV-12**: User can copy/paste nodes with their properties

### Properties

- [x] **PROP-01**: Clicking a worker shows its sources/sinks and host config in a side panel
- [x] **PROP-02**: Clicking a source shows its logical source and physical source config
- [x] **PROP-03**: Clicking a sink shows its sink type and configuration
- [x] **PROP-04**: Property fields use typed inputs (text, number, dropdown) with inline validation
- [x] **PROP-05**: User can build schemas visually (add/remove fields, select types) for logical sources
- [x] **PROP-06**: Topology validation highlights errors on the canvas (orphan nodes, missing config)

### Queries

- [x] **QURY-01**: User can add queries to a query list
- [x] **QURY-02**: User can remove queries from the list
- [x] **QURY-03**: User can edit queries in a SQL editor with syntax highlighting
- [x] **QURY-04**: ANTLR-based SQL validation using the NES grammar

### Sources

- [x] **SRCE-01**: User can manage logical sources (create, edit, delete)
- [x] **SRCE-02**: User can define schema for logical sources (field name + type)
- [x] **SRCE-03**: User can create physical sources and link them to a logical source

### Output

- [x] **OUTP-01**: Live YAML preview updates in real-time as topology changes
- [x] **OUTP-02**: Generated YAML is valid and compatible with nes-cli format
- [x] **OUTP-03**: User can download/export the YAML file
- [x] **OUTP-04**: User can import existing YAML topology files

### UI Polish

- [x] **UIPL-01**: Undo/redo for all editing operations
- [x] **UIPL-02**: Keyboard shortcuts (Ctrl+Z undo, Delete, Ctrl+S export)
- [x] **UIPL-03**: Resizable panels (canvas, properties, YAML preview)
- [x] **UIPL-04**: Dark mode

## v1.1 Requirements

### Validation Library

- [x] **VLIB-01**: NES validation pipeline extracted into standalone CMake target without runtime dependencies
- [x] **VLIB-02**: Stub/facade layer replaces gRPC, Folly, spdlog, cpptrace for WASM-compatible build
- [x] **VLIB-03**: Validation library accepts YAML string and returns structured error list
- [x] **VLIB-04**: Catalog population from YAML (workers, sources, sinks, logical sources)
- [x] **VLIB-05**: SQL parsing via ANTLR4 C++ runtime within the validation library
- [x] **VLIB-06**: Logical plan construction and validation (source inference, type inference)

### WebAssembly

- [x] **WASM-01**: C++23 / Emscripten compatibility validated via proof-of-concept spike
- [x] **WASM-02**: ANTLR4 C++ runtime compiles to WebAssembly
- [x] **WASM-03**: Validation library compiles to .wasm via Emscripten with embind interface
- [x] **WASM-04**: WASM binary under 1MB gzipped

### Browser Integration

- [x] **BINT-01**: WASM module loads in the topology editor
- [ ] **BINT-02**: Topology YAML validated via WASM module with errors displayed to user
- [x] **BINT-03**: Validation runs in Web Worker to avoid blocking UI
- [ ] **BINT-04**: Validation debounced on topology changes

### Testing & CI

- [x] **TEST-01**: C++ unit tests for validation library correctness
- [ ] **TEST-02**: WASM integration tests (validate known-good and known-bad YAML)
- [ ] **TEST-03**: CI job builds WASM module and runs tests

## v2 Requirements

- Error location mapping to specific UI nodes/fields (deferred to v1.2)
- Optimization step visualization (deferred to v1.2)
- Template topologies as starting points (deferred from v1.0 OUTP-05)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Live cluster connection | Offline YAML editor only — deploy via nes-cli |
| Query execution in browser | WASM module is validation-only, no execution engine |
| Optimization passes in WASM | Colleague separating validation from optimization; defer to v1.2 |
| Multi-user collaboration | Single-user editor; share YAML via git |
| Mobile / touch layout | Desktop-focused developer tool |

## Traceability

### v1.0 (Complete)

| Requirement | Phase | Status |
|-------------|-------|--------|
| CANV-01..12 | Phase 1 | Complete |
| PROP-01..05 | Phase 2 | Complete |
| PROP-06 | Phase 4 | Complete |
| QURY-01..04 | Phase 2, 4 | Complete |
| SRCE-01..03 | Phase 2 | Complete |
| OUTP-01..04 | Phase 3 | Complete |
| UIPL-01..04 | Phase 4 | Complete |

### v1.1 (In Progress)

| Requirement | Phase | Status |
|-------------|-------|--------|
| VLIB-01 | Phase 6 | Complete |
| VLIB-02 | Phase 6 | Complete |
| VLIB-03 | Phase 6 | Complete |
| VLIB-04 | Phase 6 | Complete |
| VLIB-05 | Phase 6 | Complete |
| VLIB-06 | Phase 6 | Complete |
| WASM-01 | Phase 5 | Complete |
| WASM-02 | Phase 5 | Complete |
| WASM-03 | Phase 6 | Complete |
| WASM-04 | Phase 6 | Complete |
| BINT-01 | Phase 7 | Complete |
| BINT-02 | Phase 7 | Pending |
| BINT-03 | Phase 7 | Complete |
| BINT-04 | Phase 7 | Pending |
| TEST-01 | Phase 6 | Complete |
| TEST-02 | Phase 7 | Pending |
| TEST-03 | Phase 7 | Pending |

**Coverage:**
- v1.1 requirements: 17 total
- Mapped to phases: 17
- Unmapped: 0

---
*Requirements defined: 2026-03-13*
*Last updated: 2026-03-15 after v1.1 roadmap created*
