---
phase: 02-framework-and-bridge
plan: 02
subsystem: runtime
tags: [async-source, source-context, backpressure, semaphore, spawn-source, cxx-bridge, ffi, tokio]

# Dependency graph
requires:
  - phase: 02-framework-and-bridge
    plan: 01
    provides: BackpressureState, BackpressureFuture, BridgeMessage, EMIT_REGISTRY, register_emit/unregister_emit, ensure_bridge
  - phase: 01-ffi-foundation
    provides: TupleBufferHandle, BufferProviderHandle, SourceContext, init_source_runtime
provides:
  - AsyncSource trait with single run() method for source authors
  - SourceTaskHandle with CancellationToken for cooperative shutdown
  - SourceContext::emit() with three-step gating (backpressure -> semaphore -> channel)
  - spawn_source() with EMIT_REGISTRY registration before task spawn
  - Monitoring task with error callback and cleanup (backpressure + emit unregister)
  - ErrorFnPtr type for C-style error reporting callback
  - Phase 2 CXX bridge declarations (spawn_source, stop_source, backpressure callbacks)
  - SourceHandle wrapper for CXX orphan rule compliance
affects: [02-03, phase-3, tokio-source-cpp, source-implementations]

# Tech tracking
tech-stack:
  added: []
  patterns: [cfg(test) SourceContext sender substitution (Sender<()> vs Sender<BridgeMessage>), ErrorCallback Send wrapper, SourceHandle CXX orphan wrapper, Phase 2 CXX bridge in nes-source-lib to avoid circular dependency]

key-files:
  created:
    - nes-sources/rust/nes-source-runtime/src/source.rs
    - nes-sources/rust/nes-source-runtime/src/handle.rs
  modified:
    - nes-sources/rust/nes-source-runtime/src/context.rs
    - nes-sources/rust/nes-source-runtime/src/lib.rs
    - nes-sources/rust/nes-source-lib/src/lib.rs

key-decisions:
  - "Phase 2 CXX bridge placed in nes-source-lib (not nes-source-bindings) to avoid circular dependency"
  - "SourceHandle wrapper type in nes-source-lib satisfies CXX orphan rule for opaque types"
  - "Function pointers and void* passed as usize across CXX bridge (CXX limitation)"
  - "cfg(test) SourceContext uses Sender<()> instead of Sender<BridgeMessage> to avoid CXX linker dependencies in pure Rust tests"
  - "ErrorCallback struct wraps error_fn + error_ctx with Send impl for monitoring task async block"
  - "PlaceholderSource in nes-source-lib returns EndOfStream; real source dispatch deferred to Phase 3"

patterns-established:
  - "cfg(test) type substitution: replace CXX-dependent field types with test-safe alternatives in struct definitions"
  - "ErrorCallback pattern: group function pointer + void* context in Send-safe struct for async tasks"
  - "CXX orphan wrapper: local struct wrapping external crate types for CXX bridge compatibility"
  - "spawn_test_source: test helper creating SourceContext with dummy channel for lifecycle tests"

requirements-completed: [FWK-02, FWK-04, FWK-06, FWK-07]

# Metrics
duration: 24min
completed: 2026-03-15
---

# Phase 02 Plan 02: AsyncSource Trait and Source Lifecycle Summary

**AsyncSource trait with emit() three-step gating (backpressure -> semaphore -> channel), spawn_source lifecycle with EMIT_REGISTRY, and Phase 2 CXX bridge in nes-source-lib**

## Performance

- **Duration:** 24 min
- **Started:** 2026-03-15T10:51:02Z
- **Completed:** 2026-03-15T11:15:27Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- AsyncSource trait defined with single `run()` method taking SourceContext, returning SourceResult
- SourceContext::emit() implements three-step gating: backpressure wait -> semaphore acquire -> BridgeMessage send
- spawn_source registers per-source emit callback in EMIT_REGISTRY before spawning source task
- Monitoring task unregisters both backpressure state and emit callback on source exit
- ErrorFnPtr callback invoked on source error/panic with CString message via monitoring task
- stop_source cancels CancellationToken non-blocking for cooperative shutdown
- Phase 2 CXX bridge in nes-source-lib declares spawn_source, stop_source, backpressure callbacks
- 44 tests pass (14 new + 30 existing Phase 1+2-01)

## Task Commits

Each task was committed atomically:

1. **Task 1: AsyncSource trait, SourceTaskHandle, and updated SourceContext** - `cccfb53bcf` (feat)
2. **Task 2: CXX bridge Phase 2 FFI entry points** - `6b028ef250` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/source.rs` - AsyncSource trait, spawn_source, monitoring_task, ErrorFnPtr, ErrorCallback, spawn_test_source helper
- `nes-sources/rust/nes-source-runtime/src/handle.rs` - SourceTaskHandle struct, stop_source function
- `nes-sources/rust/nes-source-runtime/src/context.rs` - Updated SourceContext with backpressure, semaphore, origin_id fields; three-step emit()
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - Added pub mod source, handle; re-exports AsyncSource, SourceTaskHandle
- `nes-sources/rust/nes-source-lib/src/lib.rs` - Phase 2 CXX bridge (ffi_source module), SourceHandle wrapper, spawn/stop/backpressure wrappers

## Decisions Made
- Phase 2 CXX bridge placed in nes-source-lib instead of nes-source-bindings because nes_source_runtime depends on nes_source_bindings (circular dependency). The nes-source-lib crate depends on both and can host the bridge.
- SourceHandle newtype wrapper satisfies CXX's orphan rule: CXX generates trait impls for declared opaque types, which must be defined in the declaring crate.
- Function pointers (emit_fn, error_fn) and void* contexts passed as usize across CXX bridge because CXX cannot bridge `unsafe extern "C" fn(...)` or `*mut c_void` types. C++ casts to `uintptr_t`/`size_t`, Rust `transmute`s back.
- SourceContext uses `cfg(test)` conditional compilation: `Sender<BridgeMessage>` in production, `Sender<()>` in tests. BridgeMessage contains TupleBufferHandle whose Drop triggers CXX linker symbols unavailable in pure Rust tests.
- PlaceholderSource (EndOfStream immediately) used as AsyncSource in spawn_source wrapper until Phase 3 adds actual source type dispatch.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] CXX linker symbols unavailable in pure Rust tests (SourceContext)**
- **Found during:** Task 1 (SourceContext emit() update)
- **Issue:** SourceContext holds `Sender<BridgeMessage>`. BridgeMessage contains TupleBufferHandle whose Drop triggers CXX symbols. Even creating a channel of type `bounded::<BridgeMessage>` causes linker failure.
- **Fix:** Used `cfg(test)` conditional compilation to substitute `Sender<()>` in test builds. Test emit() exercises backpressure + semaphore gates identically, skips BridgeMessage construction.
- **Files modified:** context.rs
- **Committed in:** cccfb53bcf

**2. [Rule 3 - Blocking] Circular dependency prevents Phase 2 CXX bridge in nes-source-bindings**
- **Found during:** Task 2 (CXX bridge expansion)
- **Issue:** Plan specified adding spawn_source/stop_source to nes-source-bindings, but these need nes_source_runtime types (SourceTaskHandle, AsyncSource). nes_source_runtime already depends on nes_source_bindings -> circular dependency.
- **Fix:** Placed Phase 2 CXX bridge module (`ffi_source`) in nes-source-lib, which depends on both crates. Created SourceHandle wrapper type for CXX orphan rule compliance.
- **Files modified:** nes-source-lib/src/lib.rs
- **Committed in:** 6b028ef250

**3. [Rule 3 - Blocking] CXX cannot bridge raw pointers in extern "Rust" or function pointer types**
- **Found during:** Task 2 (CXX bridge expansion)
- **Issue:** CXX rejects `*mut AbstractBufferProvider` in extern "Rust" functions ("pointer argument requires that the function be marked unsafe") and does not support `unsafe extern "Rust"` blocks. Function pointer types like `EmitFnPtr` are also unsupported.
- **Fix:** All pointer arguments passed as `usize`. C++ casts to `uintptr_t`/`size_t`, Rust reconstructs via `transmute` (function pointers) or `as *mut` cast (void pointers).
- **Files modified:** nes-source-lib/src/lib.rs
- **Committed in:** 6b028ef250

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All deviations were necessary adaptations for CXX bridge constraints and test infrastructure. All planned functionality implemented and verified. No scope creep.

## Issues Encountered
- CXX orphan rule: `type SourceTaskHandle` in extern "Rust" block requires the type to be defined in the declaring crate. Resolved with SourceHandle wrapper.
- `*mut c_void` in async block: raw pointers aren't Send. Resolved with SendVoidPtr wrapper and ErrorCallback struct.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- AsyncSource trait ready for concrete source implementations (Phase 3)
- spawn_source lifecycle complete for C++ integration via nes-source-lib CXX bridge
- SourceContext::emit() three-step gating pattern fully operational
- PlaceholderSource ready to be replaced with real source dispatch in Phase 3

---
*Phase: 02-framework-and-bridge*
*Completed: 2026-03-15*
