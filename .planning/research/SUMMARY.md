# Project Research Summary

**Project:** NebulaStream Topology Editor -- v1.1 Shared Validation via WebAssembly
**Domain:** C++ validation pipeline extraction and WebAssembly compilation
**Researched:** 2026-03-15
**Confidence:** MEDIUM

## Executive Summary

The v1.1 milestone requires extracting the NES C++ validation pipeline (SQL parsing, statement binding, schema/type inference) and compiling it to WebAssembly so the topology editor can run authoritative engine-level validation in the browser. The existing TypeScript validation only catches syntax errors and structural issues; it misses all semantic validation (unknown fields, type mismatches, unresolvable source references) that the NES engine performs. The WASM module closes this gap by providing single-source-of-truth validation identical to what the engine executes.

The central technical challenge is NOT "how to use Emscripten" but "how to extract validation code from a deeply entangled C++ dependency graph." The NES validation pipeline transitively depends on gRPC, Folly, cpptrace, and libuuid via `nes-common` -- none of which compile to WebAssembly. The recommended approach is a facade pattern: create a new `nes-validator` CMake target that cherry-picks only the validation-relevant source files and provides compile-time stubs for heavy dependencies. This avoids modifying the NES core codebase. A proper module split (separating `nes-common` into core vs runtime) is correct long-term but too invasive for this milestone.

The highest risks are: (1) C++23 compatibility with Emscripten -- the NES codebase uses `std::expected` extensively and Emscripten's C++23 support is not battle-tested, requiring early validation; (2) ANTLR4 C++ runtime WASM compilation -- the abandoned antlr4wasm project suggests this path is painful; (3) WASM binary size explosion from C++ exceptions, RTTI, and template bloat. All three risks demand proof-of-concept spikes before committing to full extraction.

## Key Findings

### Recommended Stack

The WASM build uses Emscripten SDK 4.0+ with C++23 mode, Embind for JS/C++ interop, and a separate CMakeLists.txt that compiles only validation-relevant source files. The output is an ES6 module (via `-sEXPORT_ES6=1 -sMODULARIZE=1`) that integrates with the existing Vite pipeline.

**Core technologies:**
- **Emscripten SDK 4.0+**: C++ to WASM compiler -- only viable toolchain for compiling existing C++ with browser integration
- **Embind**: JS/C++ binding layer -- handles `std::string` and structured return types natively; WebIDL and cwrap cannot
- **ANTLR4 C++ runtime**: SQL parsing -- must compile from source with Emscripten; proven possible but needs validation
- **yaml-cpp**: YAML topology parsing -- pure C++, known WASM-compatible
- **vite-plugin-wasm** (or SINGLE_FILE mode): WASM loading in dev/prod -- start with SINGLE_FILE=1 for simplicity

**Critical version note:** Emscripten 4.0+ ships LLVM 19 which supports `std::expected`, but C++23 in Emscripten is under-documented. This MUST be validated in the first spike.

### Expected Features

**Must have (table stakes):**
- ANTLR SQL parsing (replaces TypeScript port with authoritative C++ implementation)
- Statement binding against source/sink catalogs (catches unknown source references)
- Type inference and schema propagation (the highest-value validation -- catches field/type errors)
- Structured error reporting as JSON with error categories
- Single-function stateless interface: `validate(yaml) -> errors[]`

**Should have (high value, include if feasible):**
- EXPLAIN query output (low effort, useful for debugging)
- Error location mapping (ANTLR line/column for Monaco editor highlighting)
- Schema output for valid queries (auto-populate sink schemas in UI)
- Multi-query validation in a single call

**Defer (v1.2+):**
- Source-specific config validation via plugin registry
- Query plan visualization data (structured plan export)
- Incremental validation with cached catalog state

### Architecture Approach

The architecture introduces two new components: `nes-validator` (a standalone C++ validation library) and `nes-validator-wasm` (the Emscripten binding target). The validator parses YAML into in-memory catalogs, runs the NES validation pipeline stages 1-6 (parsing through type inference), catches all exceptions at the boundary, and returns structured errors. It explicitly stops before stage 7 (operator placement) which requires live cluster state. The React app loads the WASM module in a Web Worker, sends YAML strings via postMessage, and merges WASM semantic errors with existing TypeScript structural errors.

**Major components:**
1. **nes-validator** -- Standalone C++ library: YAML parsing, catalog population, SQL parsing, validation pipeline (source inference + type inference). New CMake target referencing existing NES source files with dependency stubs.
2. **nes-validator-wasm** -- Emscripten binding layer: exposes `validateTopology()` via Embind, produces .js + .wasm output files.
3. **Topology Editor WASM integration** -- Web Worker loading, debounced validation triggers, error overlay rendering, graceful degradation before WASM loads.

### Critical Pitfalls

1. **Dependency explosion via nes-common** -- `nes-common` publicly links gRPC, Folly, and other WASM-incompatible libraries. Do NOT try to compile existing CMake targets with Emscripten. Create a clean new target that cherry-picks source files and provides stubs.

2. **C++23 / Emscripten compatibility** -- `std::expected` is used in 12+ locations across the parser API. If Emscripten 4.0 does not support it, the extracted code must be downgraded to C++20 with a polyfill like `tl::expected`. Validate BEFORE extracting.

3. **ANTLR4 C++ runtime in WASM** -- The runtime uses mutexes, RTTI, and dynamic memory patterns not designed for WASM. The abandoned antlr4wasm project is a warning sign. Build and test the runtime standalone with Emscripten as the first spike.

4. **WASM binary size explosion** -- Unoptimized extraction can produce 5-20MB binaries. Set a hard budget of 1MB gzipped. Use `-Os -flto`, consider `-fwasm-exceptions` over JS-based exceptions, and strip RTTI where possible.

5. **Memory leaks at JS-WASM boundary** -- Use Embind with value types only. Keep the interface to one function with string input and vector output. Do NOT expose C++ objects with pointers across the boundary.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Proof-of-Concept Spikes
**Rationale:** Three unknowns must be validated before committing to full extraction. Failure in any spike could require rethinking the entire approach. This phase is cheap (3-4 days) and eliminates the highest risks.
**Delivers:** Go/no-go decision on the WASM approach; baseline binary size measurement; confirmed C++23/Emscripten compatibility.
**Addresses:** No features directly -- risk reduction only.
**Avoids:** Pitfalls 1 (C++23), 3 (ANTLR4), 4 (binary size).
**Spikes:**
  1. Compile ANTLR4 C++ runtime with Emscripten, parse a SQL string in Node.js
  2. Compile a minimal C++ file using `std::expected` with Emscripten to confirm C++23 support
  3. Build a trivial Embind module, load it in the topology editor, measure baseline

### Phase 2: Validation Library Extraction
**Rationale:** Once spikes pass, the core work is isolating the validation code from NES runtime dependencies. This is the hardest phase -- iterative linker-error-fixing to identify all needed source files and stubs.
**Delivers:** `nes-validator` C++ library that compiles with Emscripten; dependency stubs for logging, UUID, error handling; separate CMakeLists.txt.
**Addresses:** YAML parsing, catalog population, SQL parsing, statement binding (features 1-4 from MVP list).
**Avoids:** Pitfalls 2 (dependency explosion), 5 (Folly), 8 (spdlog/cpptrace stubs), 9 (two toolchains).
**Implements:** nes-validator component from architecture.

### Phase 3: Validation Pipeline Integration
**Rationale:** With the library compiling, add the validation phases (source inference, type inference) that provide the actual value over TypeScript validation.
**Delivers:** Full validation pipeline: source inference + type inference + structured error reporting. Working `validateTopology(yaml) -> errors[]` function.
**Addresses:** Source/sink inference, type inference, schema propagation, error categorization (features 5-8 from MVP list).
**Avoids:** Pitfall 7 (memory leaks -- design the Embind interface correctly from the start).

### Phase 4: Browser Integration
**Rationale:** The C++ side is complete. Now integrate the WASM output into the React topology editor with proper loading, Web Worker execution, and UI error rendering.
**Delivers:** WASM module loaded in Web Worker; debounced validation on topology changes; merged structural + semantic error display; graceful degradation before WASM loads.
**Addresses:** Should-have features (EXPLAIN output, error location mapping, schema output).
**Avoids:** Pitfalls 6 (async loading), 12 (main thread blocking).

### Phase 5: Optimization and Testing
**Rationale:** Polish phase. Optimize binary size, set up CI for WASM builds, create three-level test suite.
**Delivers:** Binary under 1MB gzipped; CI job for WASM build on relevant PRs; native + Node.js + browser test coverage.
**Addresses:** Binary size budget, CI integration, cross-browser testing.
**Avoids:** Pitfalls 4 (size), 10 (testing gaps), 11 (version drift).

### Phase Ordering Rationale

- **Spikes first** because the three highest-risk unknowns (C++23, ANTLR4, binary size) could each independently invalidate the approach. Spending 3 days on spikes before 3 weeks on extraction is essential risk management.
- **Extraction before pipeline** because the validation phases (source inference, type inference) depend on having the foundational code (catalogs, parsers, data types) compiling in WASM first.
- **Browser integration after C++ work** because the WASM interface design should be informed by what the C++ side actually produces, not guessed upfront.
- **Optimization last** because premature optimization of binary size wastes effort if the extraction scope changes during phase 2-3.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 1 (Spikes):** Needs hands-on experimentation, not more document research. The spikes ARE the research.
- **Phase 2 (Extraction):** Needs `/gsd:research-phase` -- the exact set of source files, headers, and stubs required is only discoverable through iterative compilation. The dependency graph is documented but the specific file-level extraction is not.

Phases with standard patterns (skip research-phase):
- **Phase 4 (Browser Integration):** Web Worker + WASM loading is well-documented. Embind interface pattern is established.
- **Phase 5 (Optimization):** Emscripten optimization flags and CI setup follow standard patterns.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | MEDIUM | Emscripten + Embind is the right choice, but C++23 support is under-documented. Must be validated by spike. |
| Features | HIGH | Based on direct NES codebase analysis. The validation pipeline stages are well-understood. |
| Architecture | HIGH | Facade pattern with source-file cherry-picking is the only viable approach given the dependency graph. |
| Pitfalls | HIGH | Dependency chain traced from actual CMakeLists.txt files. The gRPC/Folly incompatibility is verified fact. |

**Overall confidence:** MEDIUM -- the approach is sound and well-researched, but the C++23/Emscripten compatibility and ANTLR4 WASM compilation are unknowns that can only be resolved by building, not by reading documentation.

### Gaps to Address

- **C++23 in Emscripten:** STACK.md says Emscripten 4.0+ supports it via LLVM 19; PITFALLS.md says Emscripten maxes out at C++20. This contradiction must be resolved by spike 2. If C++23 fails, a `tl::expected` polyfill adds scope to phase 2.
- **reflectcpp WASM compatibility:** Used by `nes-data-types`, unknown WASM compatibility. May need stubbing if it uses system features. Discover during phase 2 extraction.
- **Boost::url usage in validation path:** May or may not be needed. Evaluate during extraction -- if validation code never calls URL parsing, exclude it.
- **Exact source file list for extraction:** The architecture documents the modules needed but not the specific `.cpp` files. This is inherently iterative -- compile, fix linker errors, repeat.

## Sources

### Primary (HIGH confidence)
- NES codebase: CMakeLists.txt dependency chain for nes-common, nes-sql-parser, nes-logical-operators, nes-query-optimizer, nes-sources, nes-sinks
- NES codebase: CLIStarter.cpp, StatementBinder.cpp, TypeInferencePhase.cpp, SourceInferencePhase.cpp (direct code reading)
- NES codebase: existing topology editor validation.ts, validateSql.ts
- PROJECT.md milestone definition

### Secondary (MEDIUM confidence)
- [Emscripten documentation](https://emscripten.org/docs/) -- Embind, optimization, build settings
- [Emscripten C++23 issues](https://github.com/emscripten-core/emscripten/issues/21143) -- modules not supported, -std=c++23 flag accepted
- [antlr4wasm project](https://github.com/mike-lischke/antlr4wasm) -- abandoned, suggests C++ WASM path is difficult

### Tertiary (LOW confidence)
- reflectcpp WASM compatibility -- unknown, needs validation
- Emscripten 4.0 C++23 `std::expected` support -- inferred from LLVM version, not verified

---
*Research completed: 2026-03-15*
*Ready for roadmap: yes*
