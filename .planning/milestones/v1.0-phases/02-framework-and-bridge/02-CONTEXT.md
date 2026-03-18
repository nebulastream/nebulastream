# Phase 2: Framework and Bridge - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Full async-to-C++ pipeline: a Rust source task emits buffers through a shared bridge thread into the Folly MPMC task queue, with backpressure in both directions and graceful shutdown. Covers AsyncSource trait, per-source semaphore, backpressure translation, bridge thread, TokioSource C++ adapter, and error surfacing.

</domain>

<decisions>
## Implementation Decisions

### Backpressure translation
- AtomicBool + Waker pattern for BackpressureFuture — not spawn_blocking, not bridge-thread-owned
- Per-source BackpressureFuture: each source gets its own future instance, stored in a concurrent map (DashMap or similar)
- C++ signals Rust via new CXX bridge callback: `on_backpressure_released(source_id)` — C++ BackpressureController calls into Rust, which sets AtomicBool and wakes the future
- BackpressureFuture lives inside `SourceContext::emit()` — source authors never see backpressure, emit() just takes longer when pressure is applied
- Initial state: flag=true (no pressure) — source can emit immediately on startup, C++ applies pressure later if needed
- Poll ordering must be correct: store waker first, then check flag — prevents lost-wake race

### Bridge thread design
- Single shared bridge thread for all sources — not per-source
- Emit callback is already source-agnostic (takes origin_id parameter), so no routing needed
- Single shared `async_channel::bounded` channel — all sources send to the same channel via `Sender::clone()`
- Channel messages carry `(OriginId, TupleBufferHandle, SemaphorePermit)`
- Bridge thread calls `receiver.recv_blocking()` — sync interface on the async channel
- Small fixed channel capacity (e.g., 32 or 64) — channel is primarily a sync/async adapter, not a deep buffer. Configurable later.
- Bridge thread started lazily on first source registration via OnceLock — avoids thread overhead if no async sources are used

### TokioSource C++ integration
- New C++ class `TokioSource` (not "AsyncSourceThread" — it's a task, not a thread)
- `TokioSource` implements the same interface as `SourceThread` (start, stop, tryStop)
- `SourceHandle` holds `std::variant<SourceThread, TokioSource>` — RunningSource and QueryEngine unchanged
- `spawn_source()` FFI call returns an opaque `Box<SourceTaskHandle>` to C++ — handle-based lifecycle, not global registry
- Emit callback passed through FFI via C-style function pointer + void* context (standard FFI pattern since CXX doesn't support std::function)
- C++ wraps `EmitFunction` into an `EmitContext` struct, passes pointer; Rust bridge thread calls back through the function pointer

### Shutdown and error handling
- `stop_source()` is non-blocking: just cancels the CancellationToken and returns immediately to C++ — matches existing SourceThread async stop model
- No timeout fallback for unresponsive sources — trust the source author, consistent with existing C++ behavior
- Cleanup via Rust ownership: task exits -> SourceContext dropped -> Sender dropped -> channel closes -> bridge drains remaining items via Drop -> TupleBufferHandle::drop() releases buffers -> permits dropped
- Source panics/errors surfaced via C++ FFI callback: `on_source_error(source_id, msg)` — engine gets immediate notification
- A background Tokio monitoring task watches the JoinHandle and calls on_source_error if it completes with Err or panic

### Claude's Discretion
- Exact BackpressureFuture implementation details (AtomicBool ordering, waker storage)
- DashMap vs RwLock<HashMap> for the futures map
- Exact channel capacity number
- EmitContext struct layout and lifetime management
- SourceTaskHandle internal fields
- Bridge thread's exact recv loop structure
- How TokioSource discovers the bridge channel sender

</decisions>

<specifics>
## Specific Ideas

- "the bridge is shared between sources" — single bridge thread, not per-source, because emit callback is source-agnostic via origin_id
- "we just need it to allow the synchronous interface for the queue and have any async interface in rust" — channel capacity should be small, it's an adapter not a buffer
- "for the other sources the stop is async. the engine signals it and the source will eventually stop" — stop_source just flicks the cancel token, no blocking wait
- BackpressureFuture waker race concern from STATE.md is manageable with correct poll ordering (store-waker-then-check pattern)

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SourceThread` (nes-sources/private/SourceThread.hpp): Reference for the source execution loop — wait → allocate → fill → emit. TokioSource mirrors this interface.
- `BackpressureChannel` (nes-executable/include/BackpressureChannel.hpp): BackpressureController::applyPressure/releasePressure — need to hook releasePressure to call Rust FFI callback
- `SourceContext` (nes-sources/rust/nes-source-runtime/src/context.rs): Already has allocate_buffer, emit, cancellation_token. Phase 2 adds backpressure check inside emit() and wires the channel to the bridge.
- `async_channel` already in nes-source-runtime/Cargo.toml — same crate used for bridge channel

### Established Patterns
- CXX bridge with opaque C++ types and free functions (Phase 1 bindings)
- `spawn_blocking` for C++ blocking calls from Tokio (BufferProviderHandle)
- OnceLock for process-global singletons (source runtime)
- EmitFunction = std::function<EmitResult(OriginId, SourceReturnType, stop_token)> — bridge must produce calls matching this signature

### Integration Points
- `SourceHandle` (nes-sources/include/Sources/SourceHandle.hpp): Needs variant to hold TokioSource alongside SourceThread
- `SourceReturnType::EmitFunction` (nes-sources/include/Sources/SourceReturnType.hpp): Bridge thread must call this with origin_id, Data{buffer}, stop_token
- `BackpressureController::releasePressure()` (BackpressureChannel.cpp): Must be modified to also call Rust `on_backpressure_released(source_id)`
- `init_source_runtime()` (nes-source-bindings/lib.rs): Bridge thread lazily started here or on first source registration

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-framework-and-bridge*
*Context gathered: 2026-03-15*
