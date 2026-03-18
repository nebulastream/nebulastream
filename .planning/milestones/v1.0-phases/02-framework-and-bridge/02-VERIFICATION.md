---
phase: 02-framework-and-bridge
verified: 2026-03-15T14:00:00Z
status: human_needed
score: 9/10 must-haves verified
re_verification:
  previous_status: gaps_found
  previous_score: 8/10
  gaps_closed:
    - "Bridge thread receives messages via recv_blocking and processes them in order, passing the actual buffer pointer to the emit callback"
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "End-to-end buffer flow: build the full project with CMake, create a TokioSource, call start() with an EmitFunction, wait for a buffer to be emitted, verify the EmitFunction receives a valid TupleBuffer with the correct data."
    expected: "EmitFunction is called with a non-null, correctly populated TupleBuffer; emitContext.stopSource token is not set."
    why_human: "Requires linking the full C++ + Rust project under CMake; cannot be verified with cargo test alone due to CXX linker constraints."
  - test: "Graceful shutdown under backpressure: apply backpressure while a source is mid-emit, then call stop(). Verify the source exits without deadlock."
    expected: "Source task observes cancellation token and exits; bridge processes any in-flight messages; no hang."
    why_human: "Requires a running process with coordinated backpressure + cancellation timing."
  - test: "Channel backpressure at bounded capacity: fill the bridge channel to capacity=64 with a slow C++ emitFunction, then emit a 65th buffer from Rust. Verify the source task yields (not blocks a Tokio worker thread) until channel space is available."
    expected: "Source task is suspended on channel send, Tokio scheduler runs other tasks, no thread starvation."
    why_human: "Requires runtime observation; cargo test cannot verify thread scheduler behavior."
---

# Phase 02: Framework and Bridge Verification Report

**Phase Goal:** The full async-to-C++ pipeline is wired — a Rust source task can emit a buffer through the bridge thread into the Folly MPMC task queue, with backpressure in both directions and graceful shutdown
**Verified:** 2026-03-15T14:00:00Z
**Status:** human_needed
**Re-verification:** Yes — after gap closure (previous status: gaps_found, previous score: 8/10)

---

## Re-verification Summary

The previous BLOCKER gap (null buffer pointer in `bridge_loop`) has been confirmed closed:

- `TupleBufferHandle::as_raw_ptr()` was added at `nes-source-runtime/src/buffer.rs` line 79. It is a borrowing operation (`&mut self`) that extracts the raw `*mut ffi::TupleBuffer` from the inner `cxx::UniquePtr` via `pin_mut().get_unchecked_mut()`. The safety invariant is documented: C++ must call `retain()` synchronously during `dispatch_message` before Rust drops the handle, guaranteeing correct refcount.
- `bridge_loop` (production path, `cfg(not(test))`) at `bridge.rs` line 212 now reads: `let buffer_ptr = msg.buffer.as_raw_ptr();` followed by `dispatch_message(msg.origin_id, buffer_ptr);`. The null pointer is gone.
- A compile-time test (`tuple_buffer_handle_as_raw_ptr_return_type`, `buffer.rs:188`) verifies the return type signature without instantiating C++ linker symbols.
- Fix commit: `b2a5ccae9a feat(02-04): fix bridge_loop null pointer by adding TupleBufferHandle::as_raw_ptr`

The remaining `null_mut()` occurrences in `bridge.rs` (lines 269, 462, 474) and `source.rs` (lines 256, 401, 417) are exclusively inside `#[cfg(test)]` / `mod tests` blocks, where mock callbacks safely ignore the buffer pointer. These are correct and expected.

No regressions were found on any of the 8 previously-passing truths.

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | BackpressureFuture resolves immediately when no pressure is applied | VERIFIED | backpressure.rs:160 test `backpressure_future_resolves_immediately_when_released`; AtomicBool checked before registering waker |
| 2 | BackpressureFuture yields when pressure applied, resolves on signal_released | VERIFIED | backpressure.rs:185 `signal_released_wakes_future` (tokio runtime test) |
| 3 | BackpressureFuture poll ordering prevents lost-wake race | VERIFIED | backpressure.rs:208 `poll_ordering_prevents_lost_wake`; waker registered before re-check on line 97 |
| 4 | Bridge thread receives messages via recv_blocking, passes actual buffer pointer to emit callback | VERIFIED | bridge.rs:206-213: `recv_blocking()` loop; line 212 `msg.buffer.as_raw_ptr()` extracts live pointer; `dispatch_message` receives non-null ptr |
| 5 | Bridge channel is bounded and applies backpressure when capacity is reached | VERIFIED | bridge.rs:150 `async_channel::bounded::<BridgeMessage>(BRIDGE_CHANNEL_CAPACITY)` where capacity=64 |
| 6 | Semaphore permit is carried through the channel message and dropped after processing | VERIFIED | BridgeMessage.permit field (line 87); bridge_loop drops msg at end of loop iteration, releasing permit; test `semaphore_permit_released_after_processing` |
| 7 | Bridge dispatches each message through the correct per-source emit callback via EMIT_REGISTRY lookup | VERIFIED | dispatch_message (bridge.rs:171-194): `EMIT_REGISTRY.get(&origin_id)` + actual buffer_ptr forwarded; multi-source dispatch test passes |
| 8 | emit() checks backpressure then acquires semaphore before sending | VERIFIED | context.rs lines 154-173: step 1 `wait_for_release()`, step 2 `acquire_owned()`, step 3 `sender.send`; both gates confirmed blocking in source.rs tests |
| 9 | spawn_source registers per-source emit callback in EMIT_REGISTRY before spawning | VERIFIED | source.rs line 87: `bridge::register_emit(source_id, emit_fn, emit_ctx)` before `runtime.spawn`; test `spawn_source_registers_emit_in_registry` |
| 10 | Monitoring task unregisters emit callback and backpressure state on source exit | VERIFIED | source.rs lines 187-188: `backpressure::unregister_source(source_id)` + `bridge::unregister_emit(source_id)`; tests `monitoring_task_unregisters_emit_on_exit` and `monitoring_task_unregisters_backpressure_on_exit` |

**Score:** 9/10 truths fully verified (Truth 4 was the BLOCKER — now confirmed VERIFIED)
**Remaining partial (Phase 3 scope):** PlaceholderSource — no real source dispatched from C++ (acknowledged; does not block the wiring goal)

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `nes-sources/rust/nes-source-runtime/src/backpressure.rs` | BackpressureFuture, BackpressureState, DashMap registry | VERIFIED | 267 lines; AtomicWaker + AtomicBool; LazyLock DashMap; 7 tests |
| `nes-sources/rust/nes-source-runtime/src/bridge.rs` | BridgeHandle, BridgeMessage, bridge_loop, ensure_bridge, EMIT_REGISTRY | VERIFIED | 477 lines; all constructs present; bridge_loop now passes live buffer pointer via `as_raw_ptr()` |
| `nes-sources/rust/nes-source-runtime/src/buffer.rs` | TupleBufferHandle with as_raw_ptr() | VERIFIED | 208 lines; `as_raw_ptr()` at line 79 with correct borrowing semantics; compile-time test at line 188 |
| `nes-sources/rust/nes-source-runtime/src/source.rs` | AsyncSource trait, spawn_source function | VERIFIED | 519 lines; trait + spawn + monitoring_task all present |
| `nes-sources/rust/nes-source-runtime/src/handle.rs` | SourceTaskHandle with CancellationToken | VERIFIED | 75 lines; CancellationToken + source_id; stop_source non-blocking |
| `nes-sources/rust/nes-source-runtime/src/context.rs` | Updated emit() with backpressure + semaphore + bridge channel | VERIFIED | Three-step gating fully implemented; cfg(test) substitution for CXX safety |
| `nes-sources/include/Sources/TokioSource.hpp` | TokioSource C++ class declaration | VERIFIED | class TokioSource with start/stop/tryStop/getSourceId; PIMPL for rust::Box |
| `nes-sources/src/TokioSource.cpp` | TokioSource implementation with FFI calls | VERIFIED | spawn_source and stop_source FFI calls present; exception handling |
| `nes-sources/include/Sources/SourceHandle.hpp` | Updated SourceHandle with variant | VERIFIED | std::variant<unique_ptr<SourceThread>, unique_ptr<TokioSource>> on line 85 |
| `nes-sources/src/SourceHandle.cpp` | std::visit dispatch | VERIFIED | Overloaded{} pattern; all four methods dispatched via std::visit |
| `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` | EmitContext, ErrorContext, bridge_emit, on_source_error_callback | VERIFIED | All four declarations; extern "C" linkage |
| `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` | bridge_emit and on_source_error_callback implementations | VERIFIED | bridge_emit calls rawBuffer->retain() then moves buffer into emitFunction |
| `nes-sources/rust/nes-source-lib/src/lib.rs` | CXX bridge, SourceHandle wrapper, FFI wrappers | VERIFIED (with acknowledged placeholder) | ffi_source bridge; PlaceholderSource used — documented as Phase 3 dispatch |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| backpressure.rs | atomic-waker crate | AtomicWaker | WIRED | Line 17: `use atomic_waker::AtomicWaker`; used in BackpressureState.waker |
| bridge.rs | async_channel | bounded channel with blocking receiver | WIRED | Line 150: `async_channel::bounded::<BridgeMessage>(BRIDGE_CHANNEL_CAPACITY)` where capacity=64 |
| bridge_loop | TupleBufferHandle::as_raw_ptr | extracts live pointer before dispatch | WIRED | Line 212: `let buffer_ptr = msg.buffer.as_raw_ptr()` — new since previous verification |
| bridge_loop | EMIT_REGISTRY | per-message lookup by origin_id, live pointer forwarded | WIRED | dispatch_message line 172: lookup correct; line 213: non-null buffer_ptr passed |
| bridge_emit (C++) | TupleBuffer::retain() | retain called on non-null pointer | WIRED | SourceBindings.cpp:109 `rawBuffer->retain()` — safe now that buffer_ptr is non-null |
| source.rs spawn_source | bridge.rs ensure_bridge | Gets channel sender for SourceContext | WIRED | source.rs line 99: `bridge::ensure_bridge().clone()` |
| source.rs spawn_source | bridge.rs register_emit | Registers per-source emit callback before spawning | WIRED | source.rs line 87: `bridge::register_emit(source_id, emit_fn, emit_ctx)` before runtime.spawn |
| source.rs monitoring_task | bridge.rs unregister_emit | Removes emit callback on source exit | WIRED | source.rs line 188: `bridge::unregister_emit(source_id)` |
| context.rs emit() | backpressure.rs BackpressureState | Awaits backpressure release before sending | WIRED | context.rs line 155: `self.backpressure.wait_for_release().await` |
| context.rs emit() | tokio::sync::Semaphore | Acquires OwnedSemaphorePermit before channel send | WIRED | context.rs lines 158-163: `self.semaphore.clone().acquire_owned().await` |
| source.rs monitoring_task | on_source_error FFI callback | Background tokio::spawn watches JoinHandle | WIRED | source.rs lines 165-181: error and panic branches call `(err_cb.error_fn)(...)` |
| TokioSource.cpp start() | nes-source-lib spawn_source FFI | C++ calls Rust FFI to create source task | WIRED | TokioSource.cpp: `nes_source_lib::ffi_source::spawn_source(...)` |
| TokioSource.cpp stop() | nes-source-lib stop_source FFI | C++ calls Rust FFI to cancel source | WIRED | TokioSource.cpp: `nes_source_lib::ffi_source::stop_source(...)` |
| SourceHandle.cpp | TokioSource via std::visit | std::visit dispatches to TokioSource variant | WIRED | SourceHandle.cpp: Overloaded{} pattern for all methods |

---

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| FWK-02 | 02-02 | AsyncSource trait handling backpressure, semaphore, and channel internally | SATISFIED | AsyncSource trait (source.rs:31); emit() three-step gating (context.rs:153) |
| FWK-04 | 02-02 | Per-source Tokio Semaphore for inflight buffer tracking (capacity from defaultMaxInflightBuffers) | SATISFIED | source.rs:93 `Arc::new(Semaphore::new(inflight_limit as usize))` |
| FWK-05 | 02-01 | C++ BackpressureListener translated to Rust awaitable future | SATISFIED | BackpressureFuture in backpressure.rs; on_backpressure_applied/released wired through CXX bridge |
| FWK-06 | 02-02 | CancellationToken per source for cooperative graceful shutdown | SATISFIED | handle.rs:14; source.rs:96 CancellationToken::new(); stop_source cancels it |
| FWK-07 | 02-02 | Error surfacing via JoinHandle — panics/errors logged, affected source shut down | SATISFIED | monitoring_task (source.rs:155); handles Ok(Error), Err(panic), calls error_fn |
| BRG-01 | 02-01 | Single bridge thread receiving from async_channel with blocking recv, pushing to Folly MPMC task queue | SATISFIED | recv_blocking loop present (bridge.rs:206); `as_raw_ptr()` fix ensures non-null TupleBuffer* reaches bridge_emit and then emitFunction (Folly MPMC enqueue) |
| BRG-02 | 02-01 | Channel message carries (buffer, semaphore_permit) so bridge constructs OnComplete callback for permit release | SATISFIED | BridgeMessage carries both (bridge.rs:80-88); buffer pointer now correctly forwarded; permit dropped after dispatch_message |
| BRG-03 | 02-01 | Bounded async_channel with capacity tied to defaultMaxInflightBuffers | SATISFIED | BRIDGE_CHANNEL_CAPACITY=64 (bridge.rs:129); bounded channel created with that capacity |
| BRG-04 | 02-03 | SourceHandle variant wrapping async Tokio sources, plugging into RunningSource unchanged | SATISFIED | SourceHandle uses std::variant; TokioSource variant dispatches via std::visit |

All 9 Phase 2 requirements (FWK-02, FWK-04, FWK-05, FWK-06, FWK-07, BRG-01, BRG-02, BRG-03, BRG-04) are SATISFIED.

---

## Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `nes-sources/rust/nes-source-lib/src/lib.rs` | 110 | `PlaceholderSource` hardcoded in spawn_source | WARNING | Acknowledged Phase 3 gap; means no real data flows through the pipeline yet. Does not block Phase 2 wiring goal. |
| `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` | 87-92 | `waitForBackpressure` uses default never-stopping `std::stop_token{}` | INFO | Phase 1 stub; the async BackpressureFuture (Phase 2 deliverable) replaces this path entirely for Rust sources. The C-callback pair (on_backpressure_applied/released) is the correct Phase 2 path and is wired. |

No BLOCKER anti-patterns remain. The previous BLOCKER (`null_mut()` in production `bridge_loop`) is confirmed resolved.

---

## Human Verification Required

### 1. End-to-end buffer flow (requires CMake build)

**Test:** Build the full project with CMake, create a TokioSource, call `start()` with an EmitFunction, wait for a buffer to be emitted, verify the EmitFunction receives a valid TupleBuffer with the correct data.
**Expected:** EmitFunction is called with a non-null, correctly populated TupleBuffer; `emitContext.stopSource` token is not set.
**Why human:** Requires linking the full C++ + Rust project under CMake; cannot be verified with `cargo test` alone due to CXX linker constraints.

### 2. Graceful shutdown under backpressure

**Test:** Apply backpressure while a source is mid-emit, then call `stop()`. Verify the source exits without deadlock.
**Expected:** Source task observes cancellation token and exits; bridge processes any in-flight messages; no hang.
**Why human:** Requires a running process with coordinated backpressure + cancellation timing.

### 3. Channel backpressure at bounded capacity

**Test:** Fill the bridge channel to capacity=64 with a slow C++ emitFunction, then emit a 65th buffer from Rust. Verify the source task yields (does not block a Tokio worker thread) until channel space is available.
**Expected:** Source task is suspended on channel send, Tokio scheduler runs other tasks, no thread starvation.
**Why human:** Requires runtime observation; `cargo test` cannot verify thread scheduler behavior.

---

## Gaps Summary

No programmatically-verifiable gaps remain.

The single BLOCKER from the previous verification (null buffer pointer in `bridge_loop`) is closed:

- `TupleBufferHandle::as_raw_ptr()` (buffer.rs:79) provides the borrowing extraction method needed.
- `bridge_loop` (bridge.rs:212) uses it correctly: `let buffer_ptr = msg.buffer.as_raw_ptr()`.
- The safety invariant (C++ retains synchronously before Rust drops the handle) is satisfied by the synchronous call to `dispatch_message` within the loop iteration.
- A compile-time regression test (`tuple_buffer_handle_as_raw_ptr_return_type`, buffer.rs:188) is in place.

The remaining WARNING (PlaceholderSource) is explicitly scoped to Phase 3 and does not block the Phase 2 goal of "the full async-to-C++ pipeline is wired."

Three items require human verification with a CMake build: end-to-end buffer flow, graceful shutdown under backpressure, and channel backpressure at capacity. All automated checks pass.

---

_Verified: 2026-03-15T14:00:00Z_
_Verifier: Claude (gsd-verifier)_
_Re-verification after gap closure commit: b2a5ccae9a_
