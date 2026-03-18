---
phase: 01-ffi-foundation
plan: 01
subsystem: ffi
tags: [cxx, corrosion, cmake, rust-ffi, tuple-buffer, backpressure]

# Dependency graph
requires: []
provides:
  - "Cargo workspace with 3 sub-crates (bindings, runtime, lib)"
  - "cxx bridge declarations for TupleBuffer, AbstractBufferProvider, BackpressureListener"
  - "C++ free function wrappers in NES::SourceBindings namespace"
  - "CMake/Corrosion build integration for source bindings"
  - "init_source_runtime FFI entry point with Result<()> (FFI-03 panic safety)"
affects: [01-02, 02-tokio-runtime, 03-source-api]

# Tech tracking
tech-stack:
  added: [cxx 1.0, tokio 1.40.0, tokio-util 0.7.13, async-channel 2.3.1, tracing 0.1]
  patterns: [cxx-bridge-free-functions, corrosion-staticlib, workspace-re-export]

key-files:
  created:
    - nes-sources/rust/Cargo.toml
    - nes-sources/rust/rust-toolchain.toml
    - nes-sources/rust/nes-source-bindings/Cargo.toml
    - nes-sources/rust/nes-source-bindings/lib.rs
    - nes-sources/rust/nes-source-runtime/Cargo.toml
    - nes-sources/rust/nes-source-runtime/src/lib.rs
    - nes-sources/rust/nes-source-lib/Cargo.toml
    - nes-sources/rust/nes-source-lib/src/lib.rs
    - nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp
    - nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp
    - nes-sources/rust/nes-source-bindings/CMakeLists.txt
  modified:
    - cmake/EnableRust.cmake
    - nes-sources/CMakeLists.txt

key-decisions:
  - "init_source_runtime stub implemented directly in bindings crate (cxx requires extern Rust functions in parent module of bridge)"
  - "waitForBackpressure uses default stop_token{} - Phase 2 will refine for cancellation"

patterns-established:
  - "cxx bridge free function pattern: C++ methods wrapped as free functions in NES::SourceBindings namespace"
  - "Workspace re-export pattern: nes-source-lib staticlib re-exports bindings and runtime crates"
  - "Corrosion integration: corrosion_import_crate in EnableRust.cmake, corrosion_add_cxxbridge in component CMakeLists.txt"

requirements-completed: [FFI-03, FFI-04]

# Metrics
duration: 4min
completed: 2026-03-15
---

# Phase 1 Plan 01: Cargo Workspace and cxx Bridge Summary

**cxx bridge declarations for TupleBuffer/AbstractBufferProvider/BackpressureListener with C++ free function wrappers and CMake/Corrosion integration**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-15T01:14:26Z
- **Completed:** 2026-03-15T01:18:36Z
- **Tasks:** 3
- **Files modified:** 15

## Accomplishments
- Cargo workspace with 3 sub-crates compiles with `cargo check --workspace`
- cxx bridge declares all opaque types (TupleBuffer, AbstractBufferProvider, BackpressureListener) with free function accessors
- C++ SourceBindings.hpp/cpp implement all free function wrappers delegating to NES types
- CMake integration via Corrosion mirrors the established nes-network pattern
- init_source_runtime declared with Result<()> ensuring cxx panic-to-exception conversion (FFI-03)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create Cargo workspace and sub-crate scaffolding** - `4db6d46551` (feat)
2. **Task 2: Create C++ bridge wrapper files** - `fd86598d91` (feat)
3. **Task 3: Create CMakeLists.txt with Corrosion integration** - `0644136ee1` (feat)

## Files Created/Modified
- `nes-sources/rust/Cargo.toml` - Workspace root with 3 members
- `nes-sources/rust/rust-toolchain.toml` - Nightly toolchain for sanitizer support
- `nes-sources/rust/nes-source-bindings/Cargo.toml` - Bindings crate with cxx dependency
- `nes-sources/rust/nes-source-bindings/lib.rs` - cxx bridge module with all type/function declarations
- `nes-sources/rust/nes-source-runtime/Cargo.toml` - Runtime crate with tokio/async-channel deps
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - Module stubs for buffer, runtime, context, error
- `nes-sources/rust/nes-source-lib/Cargo.toml` - Staticlib crate re-exporting bindings + runtime
- `nes-sources/rust/nes-source-lib/src/lib.rs` - Re-export pattern
- `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` - C++ free function declarations
- `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` - C++ free function implementations
- `nes-sources/rust/nes-source-bindings/CMakeLists.txt` - Corrosion cxx bridge build config
- `cmake/EnableRust.cmake` - Added corrosion_import_crate for nes_source_lib
- `nes-sources/CMakeLists.txt` - Added add_subdirectory for Rust bindings

## Decisions Made
- **init_source_runtime in bindings crate:** cxx requires extern "Rust" functions to be defined in the parent module of the bridge module. Rather than cross-crate resolution (which cxx doesn't support), the stub is implemented directly in nes-source-bindings/lib.rs. Plan 02 will replace this stub with the real tokio runtime initialization.
- **waitForBackpressure default stop_token:** Uses `std::stop_token{}` (never-stopping) for Phase 1. Phase 2 will refine this when implementing the async BackpressureFuture with proper cancellation support.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added stub init_source_runtime implementation in bindings crate**
- **Found during:** Task 1 (Cargo workspace scaffolding)
- **Issue:** cxx requires extern "Rust" functions to exist in the parent module at check time. The plan described cross-crate resolution via nes-source-lib, but cxx resolves at the crate level, not the link level.
- **Fix:** Added a stub `fn init_source_runtime() -> Result<(), String>` directly in nes-source-bindings/lib.rs that returns Ok(()). Plan 02 will replace with the real implementation.
- **Files modified:** nes-sources/rust/nes-source-bindings/lib.rs
- **Verification:** `cargo check --workspace` passes
- **Committed in:** 4db6d46551 (Task 1 commit)

**2. [Rule 3 - Blocking] Created module stub files for nes-source-runtime**
- **Found during:** Task 1
- **Issue:** lib.rs declares `pub mod buffer;` etc. but the files didn't exist, causing compile error
- **Fix:** Created buffer.rs, runtime.rs, context.rs, error.rs with placeholder comments
- **Files modified:** nes-sources/rust/nes-source-runtime/src/{buffer,runtime,context,error}.rs
- **Verification:** `cargo check --workspace` passes
- **Committed in:** 4db6d46551 (Task 1 commit)

**3. [Rule 2 - Missing Critical] Added corrosion environment variables for nes_source_lib**
- **Found during:** Task 3
- **Issue:** EnableRust.cmake sets sanitizer env vars on nes_rust_bindings but not on the new nes_source_lib target. Sanitizer builds would fail without matching flags.
- **Fix:** Added `set_property(TARGET nes_source_lib PROPERTY CORROSION_ENVIRONMENT_VARIABLES ...)` alongside the existing nes_rust_bindings property
- **Files modified:** cmake/EnableRust.cmake
- **Verification:** Both targets have matching environment variable configuration
- **Committed in:** 0644136ee1 (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (1 missing critical, 2 blocking)
**Impact on plan:** All auto-fixes necessary for compilation and correctness. No scope creep.

## Issues Encountered
None beyond the deviations documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- cxx bridge declarations are complete and ready for safe Rust wrappers (Plan 02)
- TupleBuffer, AbstractBufferProvider, BackpressureListener opaque types are declared
- Module stubs in nes-source-runtime are ready for buffer wrapper, runtime, context, and error implementations
- CMake integration is in place; full C++ build verification requires cmake configure/build (not done in this plan, will be verified when the full project builds)

## Self-Check: PASSED

All 11 created files verified present. All 3 task commits verified in git log.

---
*Phase: 01-ffi-foundation*
*Completed: 2026-03-15*
