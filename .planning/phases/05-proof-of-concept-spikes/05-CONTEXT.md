# Phase 5: Proof-of-Concept Spikes - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Validate three technical unknowns before committing to the validation library extraction: C++23/Emscripten compatibility, ANTLR4 C++ runtime WASM compilation, and Embind JS interop. Each spike compiles C++ to WASM and verifies it runs in Node.js. No browser/editor integration in this phase.

</domain>

<decisions>
## Implementation Decisions

### C++23 Strategy
- Test Emscripten first — user expects it to work since Emscripten ships LLVM 19 (same as NES)
- If C++23 doesn't work: the WASM target can use a different standard (C++20) from the rest of NES — isolation is the whole point
- Spike should test a representative set of features naturally (std::expected, concepts, ranges) rather than cherry-picking — "we will find out"
- No preemptive polyfill — only if Emscripten actually fails

### ANTLR4 Approach
- Hybrid architecture: keep the existing TypeScript ANTLR parser for fast inline syntax feedback, use WASM module for full semantic validation (catalog + logical plan)
- The WASM module still needs the C++ ANTLR4 runtime internally — it receives YAML containing SQL strings and parses them itself
- Spike must test ANTLR4 C++ runtime compiling to WASM and parsing a NES SQL statement

### Spike Scope
- Minimal: compile C++ to WASM, call from Node.js, verify output — pass/fail on feasibility
- No browser/Vite integration in spikes (that's Phase 7)
- No binary size measurement in spikes (optimization is Phase 6)
- Three discrete spikes: (1) C++23 features, (2) ANTLR4 parse, (3) Embind interop

### Build Integration
- Spike code lives inside nes-topology-editor/wasm/ with its own CMakeLists.txt
- Use vcpkg with a custom Emscripten/WASM triplet for dependency management (consistent with NES)
- Production .wasm artifact built in CI, not committed to repo

### Claude's Discretion
- Exact Emscripten version to use
- vcpkg triplet configuration for WASM
- How to structure the three spike directories
- Node.js test harness approach

</decisions>

<specifics>
## Specific Ideas

- User expects C++23 to just work ("I am surprised if the web assembly compiler does not support C++23 yet") — Emscripten is LLVM-based, same compiler frontend as NES uses
- The hybrid TS+WASM approach means the existing TypeScript ANTLR parser stays for real-time keystroke feedback, while WASM handles the heavy semantic validation on the full YAML

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `nes-sql-parser/AntlrSQL.g4`: The grammar file that ANTLR4 C++ runtime will parse
- `nes-frontend/tests/testdata/basic_single_node.yaml`: Example YAML for testing the validation path
- `nes-frontend/src/Statements/StatementHandler.cpp`: Entry point for YAML → catalog → validation pipeline
- `vcpkg/custom-triplets/`: Template for creating a WASM triplet

### Established Patterns
- CMake with vcpkg: All NES dependencies managed through vcpkg with custom triplets
- C++23 enforced via CMAKE_CXX_STANDARD 23 and toolchain file
- std::expected used extensively in StatementHandler/StatementBinder for error propagation

### Integration Points
- `nes-topology-editor/wasm/` — new directory for spike CMake project
- `vcpkg/custom-triplets/` — where the Emscripten triplet would be added

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 05-proof-of-concept-spikes*
*Context gathered: 2026-03-15*
