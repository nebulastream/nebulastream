# Phase 4: Rust Sink Framework - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Pure Rust sink framework: AsyncSink trait, SinkContext, per-sink bounded channel, and spawn_sink/stop_sink lifecycle on the shared Tokio runtime. No C++ integration or FFI in this phase — that's Phase 5.

</domain>

<decisions>
## Implementation Decisions

### Trait shape
- Single `run(&mut self, ctx: &SinkContext)` method — mirrors AsyncSource::run(ctx) pattern
- No separate open/write/flush/close methods — sink author controls their own loop in run()
- Trait is `Send + 'static` for spawning on Tokio runtime
- Context is borrowed (`&SinkContext`), not owned — framework retains ownership for lifecycle
- `run()` returns `Result<(), SinkError>` — errors are fatal to the query

### Channel and message enum
- `SinkContext::recv().await` returns `SinkMessage` enum:
  - `SinkMessage::Data(TupleBufferHandle)` — buffer to process
  - `SinkMessage::Close` — no more data, sink should flush internal state and return
- Per-sink bounded `async_channel` (not shared) — no head-of-line blocking between sinks
- async_channel reused from v1.0 (already in Cargo.toml)
- C++ side uses `try_send()` (non-blocking) — if full, apply backpressure + repeatTask retry (NetworkSink pattern)

### Shutdown protocol
- C++ sends `SinkMessage::Close` through the data channel (ordering guaranteed: all data before close)
- Sink processes Close in run() — flushes whatever it needs, then returns
- Framework drops the Receiver after run() returns, closing the channel
- C++ detects completion via `try_send` returning `Closed` — no extra AtomicBool needed
- `tryStop()` polls by attempting `try_send` — `Closed` means done, otherwise `TIMEOUT`

### Error handling
- `run()` returns `Result<(), SinkError>` — on Err, framework calls on-error callback (passed from C++ at spawn time)
- On error: framework closes channel (drops receiver), C++ detects `Closed` on try_send
- Query engine handles failure via normal termination path
- Framework wraps run() in catch_unwind — panics treated as errors (same as source FFI-03 pattern)

### Claude's Discretion
- SinkMessage enum naming and exact fields
- Internal channel capacity default (will be configurable via C++ side in Phase 5)
- Test mock sink implementation details
- SinkError type definition

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source framework (pattern to mirror)
- `nes-sources/rust/nes-source-runtime/src/source.rs` — AsyncSource trait, spawn_source/stop_source lifecycle, monitoring task pattern
- `nes-sources/rust/nes-source-runtime/src/context.rs` — SourceContext design, cfg(test) substitution pattern for unit tests
- `nes-sources/rust/nes-source-runtime/src/bridge.rs` — BridgeMessage enum pattern, channel architecture

### Sink infrastructure (C++ integration target for Phase 5)
- `nes-plugins/Sinks/NetworkSink/NetworkSink.hpp` — BackpressureHandler hysteresis, try_send + repeatTask pattern
- `nes-plugins/Sinks/NetworkSink/NetworkSink.cpp` — execute/stop flow, flush semantics
- `nes-sinks/include/Sinks/Sink.hpp` — Sink base class, BackpressureController ownership
- `nes-executable/include/ExecutablePipelineStage.hpp` — start/execute/stop interface

### Research
- `.planning/research/ARCHITECTURE.md` — Sink architecture analysis
- `.planning/research/PITFALLS.md` — Buffer ownership, deadlock, flush ordering risks
- `.planning/research/FEATURES.md` — Table stakes vs differentiators

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SourceContext` pattern: borrowed context with cancellation_token, cfg(test) channel substitution — direct template for SinkContext
- `AsyncSource` trait: single run() method returning Result — direct template for AsyncSink
- `BridgeMessage` enum: Data | Eos — direct template for SinkMessage: Data | Close
- `async_channel` already in Cargo.toml — reuse for sink channels
- `CancellationToken` from tokio_util — reuse for cooperative shutdown

### Established Patterns
- `cfg(test)` substitution: SourceContext uses `Sender<OwnedSemaphorePermit>` in tests vs `Sender<BridgeMessage>` in production — same pattern for SinkContext
- `unsafe impl Sync` on context types when backing data is thread-safe
- `catch_unwind` wrapping at spawn boundary
- Monitoring task pattern for error detection and cleanup

### Integration Points
- Shared Tokio runtime via `source_runtime()` in bindings crate — sinks reuse same runtime
- Sink modules added to existing `nes-source-runtime` crate (not separate crate)
- SinkMessage enum in new `sink.rs` module alongside existing `source.rs`

</code_context>

<specifics>
## Specific Ideas

- "Use an enum" for recv() — explicit SinkMessage::Data | SinkMessage::Close, not Option
- "It's not really a flush but more a close" — Close is the right semantic: sink receives Close, handles its own internal flushing, then returns
- Channel closed detection for tryStop — C++ detects `Closed` from try_send, no extra flag
- Error callback passed at spawn time — same pattern as source error_fn/error_ctx

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 04-rust-sink-framework*
*Context gathered: 2026-03-16*
