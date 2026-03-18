# Phase 5: TokioSink Operator and FFI Bridge - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

C++ TokioSink operator implementing ExecutablePipelineStage that pushes TupleBuffers to Rust AsyncSink implementations via CXX FFI bridge. Includes backpressure hysteresis (NetworkSink pattern), repeatTask-based retry, and flush-confirm stop protocol. Also renames nes-source-bindings crate to nes-io-bindings to reflect shared source+sink scope.

</domain>

<decisions>
## Implementation Decisions

### Buffer ownership across FFI
- C++ retains() TupleBuffer before sending through FFI, Rust releases on TupleBufferHandle drop
- Reuse existing TupleBufferHandle as-is for sink buffers (same type as sources)
- FFI function takes raw pointer (void*) — C++ retains, passes raw ptr, Rust wraps in TupleBufferHandle
- Mirrors source pattern but in reverse direction (C++ -> Rust instead of Rust -> C++)

### Backpressure & repeatTask strategy
- Full hysteresis backpressure matching NetworkSink pattern (upper/lower thresholds)
- Hysteresis thresholds configurable per TokioSink (like NetworkSink's BackpressureHandler constructor params)
- Channel capacity also configurable per sink
- repeatTask retry when try_send fails (non-blocking pipeline thread)
- 10ms retry interval matching NetworkSink's BACKPRESSURE_RETRY_INTERVAL

### Flush-confirm stop protocol
- On stop(): drain BackpressureHandler's internal buffer into channel (may need repeatTask if channel full), then send Close — no data loss
- No waiting for new execute() calls — stop means "flush pending + close"
- tryStop() polls is_sink_done() via repeatTask, returns TIMEOUT if not done — matches TokioSource::tryStop() pattern
- No hard timeout — trust the sink to finish after receiving Close
- Query failure scenario: C++ drops sender, Rust sink sees broken channel → implicit clean termination (already handled by Phase 4's recv() returning SinkMessage::Close on broken channel)

### CXX bridge surface & bindings layout
- Full crate rename: nes-source-bindings → nes-io-bindings (directory, Cargo.toml, CMakeLists, all imports)
- Header/class rename: SourceBindings → IoBindings (or similar)
- sink_send_buffer(handle, buffer_ptr) returns CXX shared enum SendResult { Success, Full, Closed }
- spawn_sink called through CXX bridge, returns Box<SinkTaskHandle> — mirrors spawn_source pattern
- stop_sink and is_sink_done exposed through same CXX bridge

### Claude's Discretion
- Exact BackpressureHandler state machine internals (can mirror or simplify NetworkSink's)
- Default hysteresis threshold values
- Default channel capacity
- TokioSink class member layout and constructor signature
- Error callback integration details (reuse ErrorContext pattern from sources)
- Whether sink_send_buffer wraps stop_sink for the Close case or keeps them separate

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source operator (pattern to mirror for TokioSink)
- `nes-sources/include/Sources/TokioSource.hpp` — TokioSource class, SpawnFn, EmitContext, ErrorContext, start/stop/tryStop lifecycle
- `nes-sources/src/TokioSource.cpp` — Full implementation of TokioSource lifecycle, EOS handling, tryStop polling

### NetworkSink (backpressure pattern to follow)
- `nes-plugins/Sinks/NetworkSink/NetworkSink.hpp` — BackpressureHandler state machine, hysteresis thresholds, SENDER_QUEUE_SIZE config
- `nes-plugins/Sinks/NetworkSink/NetworkSink.cpp` — execute/stop flow, repeatTask usage, flush semantics, onFull/onSuccess methods

### Pipeline interface
- `nes-executable/include/ExecutablePipelineStage.hpp` — start/execute/stop interface that TokioSink must implement
- `nes-sinks/include/Sinks/Sink.hpp` — Sink base class, BackpressureController ownership

### Existing CXX bridge (to be renamed and extended)
- `nes-sources/rust/nes-source-bindings/lib.rs` — CXX bridge definition, init_source_runtime, TupleBuffer/BufferProvider FFI
- `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` — C-linkage callbacks (bridge_emit, on_source_error_callback, nes_release_semaphore_slot)
- `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` — C++ header exposing FFI functions

### Sink framework (Phase 4 output — Rust side)
- `nes-sources/rust/nes-source-runtime/src/sink.rs` — AsyncSink trait, spawn_sink, monitoring task
- `nes-sources/rust/nes-source-runtime/src/sink_handle.rs` — SinkTaskHandle, stop_sink (try_send Close), is_sink_done (sender.is_closed())
- `nes-sources/rust/nes-source-runtime/src/sink_context.rs` — SinkContext, SinkMessage enum, recv() semantics
- `nes-sources/rust/nes-source-runtime/src/sink_error.rs` — SinkError type

### Research
- `.planning/research/ARCHITECTURE.md` — Sink architecture analysis
- `.planning/research/PITFALLS.md` — Buffer ownership, deadlock, flush ordering risks

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `TokioSource` class: Direct template for TokioSink — same start/stop/tryStop lifecycle pattern
- `BackpressureHandler` in NetworkSink: State machine for hysteresis — can be reused or adapted
- `EmitContext`/`ErrorContext` patterns: Callback context structs for FFI — same pattern for sink callbacks
- `TupleBufferHandle`: Already supports both directions — retain/release FFI is symmetric
- `SinkTaskHandle` (Phase 4): Ready to be held by C++ TokioSink operator

### Established Patterns
- `repeatTask(buffer, duration)` for non-blocking retry in pipeline stages
- `BackpressureController::applyPressure()`/`releasePressure()` for upstream flow control
- `tryStop()` returning `SourceReturnType::StopResult` (SUCCESS/TIMEOUT) for polling
- CXX bridge with C-linkage callbacks for async notifications
- `RustHandleImpl` PIMPL pattern for hiding CXX types from C++ headers

### Integration Points
- Shared Tokio runtime via `source_runtime()` — sinks spawn on same runtime
- `ExecutablePipelineStage` interface — TokioSink implements start/execute/stop
- `PipelineExecutionContext` — provides repeatTask, backpressure controller access
- CMake/Corrosion build — needs updating for crate rename

</code_context>

<specifics>
## Specific Ideas

- Crate rename from nes-source-bindings to nes-io-bindings — reflects that bindings serve both sources and sinks
- SendResult as CXX shared enum (Success/Full/Closed) — richer than uint8_t, type-safe on both sides
- Query failure = channel dropped → Rust sink sees broken channel → implicit clean termination (already works from Phase 4)
- "It's not really a flush but more a close" carried forward — stop drains pending buffers then sends Close

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 05-tokiosink-operator-and-ffi-bridge*
*Context gathered: 2026-03-16*
