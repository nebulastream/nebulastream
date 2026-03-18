# NebulaStream Topology Editor UI

## What This Is

A web-based visual topology editor that lives inside the NebulaStream repo. It lets users design NES deployment topologies by dragging and connecting worker nodes, attaching sources and sinks, writing SQL queries, and exporting the result as YAML files consumable by `nes-cli`. The editor validates topologies using the same validation logic as the NES engine via a shared WebAssembly module.

## Core Value

Users can visually design and configure NES topologies without hand-writing YAML, and get a valid topology file they can immediately deploy with `nes-cli`.

## Current Milestone: v1.1 — Shared Validation via WebAssembly

**Goal:** Extract the NES validation pipeline into a standalone C++ library, compile it to WebAssembly, and wire it into the topology editor so the UI uses the same validation logic as the NES engine.

**Target features:**
- Standalone validation library extracted from NES (no runtime dependencies)
- WebAssembly compilation of the validation pipeline
- Narrow interface: accept YAML string, return validation errors
- Integration into the topology editor UI
- NES build restructuring to support the library extraction

## Requirements

### Validated

- Visual node graph for worker topology (v1.0)
- Source/sink nodes with host placement (v1.0)
- Context-sensitive property panels (v1.0)
- SQL query editor with syntax highlighting and autocompletion (v1.0)
- YAML import/export with live preview (v1.0)
- Topology validation with visual error overlays (v1.0)
- Dark mode, keyboard shortcuts, history navigation (v1.0)

### Active

- [ ] Shared validation library extracted from NES C++ codebase
- [ ] WebAssembly module compiled from the validation library
- [ ] UI integration: validate topology YAML using WASM module
- [ ] NES build restructured to separate validation from runtime

### Out of Scope

- Live cluster connectivity (deploying queries, checking status) — offline YAML editor only
- User authentication or multi-user collaboration
- Mobile layout — desktop-focused editor
- Error location mapping to specific UI nodes (deferred to v1.2)
- Optimization step visualization (deferred to v1.2)

## Context

- NebulaStream already has `nes-cli` which consumes YAML topology files (see `docs/nebulastream-frontend.md`)
- The NES validation pipeline: YAML → SQL statements → catalog manipulation → ANTLR parse → logical plan → validate
- A colleague is working on separating validation from optimization in the NES engine
- The optimization step is currently cheap and could potentially be included
- Emscripten is the standard toolchain for compiling C++ to WebAssembly
- The NES build uses CMake; the validation library will need its own CMake target with minimal dependencies
- Existing codebase map at `.planning/codebase/` documents the C++ architecture

## Constraints

- **Location**: Must live inside the NebulaStream monorepo
- **Stack**: React + TypeScript UI, C++ validation library, Emscripten for WASM compilation
- **Offline**: No backend required — WASM runs client-side in the browser
- **YAML format**: Must produce topology files compatible with `nes-cli`
- **Build isolation**: Validation library must compile without pulling in NES runtime (networking, execution engine, worker coordination)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| React + React Flow for graph editor | Standard library for interactive node graphs in React | ✓ Good |
| ANTLR4 JS runtime for SQL parsing | Reuse existing grammar — but will be replaced by WASM validation in v1.1 | ✓ Good (bridge solution) |
| Offline-only (no cluster connection) | Keeps scope manageable, pure client-side app | ✓ Good |
| Inside NES monorepo | Keeps tooling co-located with the project it serves | ✓ Good |
| Shared validation via WASM | Single source of truth for validation between UI and engine | — Pending |
| Narrow WASM interface (YAML string → errors) | Minimizes coupling, easy to evolve | — Pending |

---
*Last updated: 2026-03-15 after v1.1 milestone start*
