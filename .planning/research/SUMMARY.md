# Project Research Summary

**Project:** Tokio-Based Rust I/O Framework for NebulaStream -- Sink Extension (v1.1)
**Domain:** Stream processing engine -- async Rust I/O framework (C++/Rust FFI)
**Researched:** 2026-03-16
**Confidence:** HIGH

## Executive Summary

The v1.1 sink framework extends the validated v1.0 async Rust source framework by inverting the data flow: C++ pipeline worker threads push TupleBuffers into bounded async channels, and Rust async tasks receive and write them to external systems via an `AsyncSink` trait. The critical architectural insight is that **sinks do not need a bridge thread** -- unlike sources, which serialize async Rust producers into a synchronous C++ consumer, sinks have synchronous C++ producers pushing into async Rust consumers. Each sink gets its own per-sink bounded channel with non-blocking `try_send` from C++ and `recv().await` on the Rust side.

The recommended approach requires **zero new Rust crate dependencies**. The existing stack (tokio 1.40.0, async-channel 2.3.1, cxx 1.0, DashMap 6.1, tokio-util 0.7.13) provides every primitive needed. The sink framework mirrors established C++ patterns from NetworkSink: `repeatTask`-based non-blocking retry when the channel is full, BackpressureController hysteresis (upper/lower thresholds), and flush-confirm polling on stop. New Rust modules (`sink.rs`, `sink_context.rs`, `sink_handle.rs`) are added to existing crates -- no new crates needed.

The primary risks center on the FFI buffer ownership inversion (C++ hands buffers to Rust instead of vice versa), flush ordering during shutdown (stop signals must not race with in-flight buffers), and the absolute prohibition on blocking C++ pipeline worker threads. All three have clear mitigation strategies drawn from v1.0 lessons and NetworkSink patterns. The flush-confirm protocol (channel close, drain, flush, AtomicBool signal, repeatTask polling) is the most complex lifecycle interaction and requires careful implementation.

## Key Findings

### Recommended Stack

No new dependencies. The v1.0 stack covers all sink requirements. The key reuse insight: `async_channel` provides both `send_blocking()` (for C++ pipeline threads not on Tokio) and `recv().await` (for Rust async tasks). This is the exact inverse of the source pattern.

**Core technologies (all reused from v1.0):**
- **tokio 1.40.0:** Shared async runtime for both source and sink tasks -- single runtime is a project constraint
- **async-channel 2.3.1:** Bounded MPSC channel; `try_send`/`send_blocking` from C++, `recv().await` from Rust
- **cxx 1.0:** FFI bridge for spawn/stop/send/flush operations -- same pattern as TokioSource
- **dashmap 6.1:** SINK_DISPATCH registry for per-sink channel lookup from any C++ thread
- **tokio-util 0.7.13:** CancellationToken for cooperative shutdown of sink tasks

**New capabilities built from existing primitives (no new crates):**
- `AsyncSink` trait (open/write/flush/close)
- `SinkContext` (cancellation token only -- simpler than SourceContext)
- `SinkTaskHandle` (channel sender, flush AtomicBool, cancellation)
- `SinkBridgeMessage` enum (Data, Flush, Stop)

### Expected Features

**Must have (table stakes):**
- AsyncSink trait with open/write/flush/close lifecycle
- Bounded channel with non-blocking try_send from C++ pipeline threads
- BackpressureController with hysteresis (upper=1000, lower=200, matching NetworkSink)
- repeatTask-based retry when channel is full (10ms interval)
- Guaranteed flush on stop -- all in-flight buffers written before pipeline teardown
- TokioSink C++ operator (Sink subclass with start/execute/stop)
- TokioSinkRegistry (mirrors TokioSourceRegistry + BaseRegistry pattern)
- Error propagation from async task to C++ pipeline via error callback
- Buffer data passing across FFI with correct retain/release lifecycle

**Should have (differentiators):**
- Configurable channel capacity per sink
- Buffer zero-copy with lifetime safety (TupleBufferHandle read-only for sinks)
- Structured tracing in sink task
- Write timeout for detecting hung sinks

**Defer (v2+):**
- Batch write optimization (write_batch method)
- Concurrent writes within one sink
- Partial write recovery / write-ahead logging

### Architecture Approach

Sinks invert the source data flow. Per-sink bounded channels connect C++ pipeline threads (producers) to Rust async tasks (consumers). No bridge thread, no shared channel, no Rust-side backpressure state. BackpressureController calls happen entirely on the C++ side. The flush-confirm protocol uses AtomicBool + repeatTask polling, mirroring NetworkSink's proven flush pattern.

**Major components:**
1. **AsyncSink trait + SinkContext** -- User-facing Rust API; four async lifecycle methods
2. **SinkTaskHandle** -- Opaque handle holding channel sender, flush status, cancellation token; returned to C++ from spawn_sink
3. **SinkTask (spawn_sink)** -- Tokio task running open/recv-loop/flush/close lifecycle
4. **TokioSink C++ operator** -- Sink subclass with state machine (Active/Flushing/Closed); uses repeatTask for retry and flush polling
5. **TokioSinkRegistry** -- String-name registration mirroring TokioSourceRegistry; integrates with SinkProvider
6. **CXX bridge (ffi_sink module)** -- spawn_sink, sink_try_send, request_stop, is_flushed, stop_sink

### Critical Pitfalls

1. **Buffer ownership inversion** -- C++ hands buffers to Rust (opposite of sources). Must use copy constructor (which calls retain) exactly once; Rust TupleBufferHandle Drop calls release. Document refcount at every transfer point. Double-retain causes buffer pool exhaustion.

2. **Backpressure deadlock** -- Pipeline worker threads must NEVER block on channel send. Use `try_send()` + `repeatTask` exclusively. Blocking a worker thread can deadlock the entire engine in single-worker configurations.

3. **Flush ordering race** -- Stop signals and data travel different paths. Send Flush sentinel through the data channel (not a separate signal). TokioSink state machine must transition Active->Flushing BEFORE sending the sentinel to prevent data arriving after flush.

4. **repeatTask re-entrance during flush** -- A repeated task from a full-channel retry may fire after stop is initiated. TokioSink must check state (Active/Flushing/Closed) before every try_send attempt.

5. **Error propagation gap** -- Sink write errors must stop the pipeline, not just log. Error callback sets flag; TokioSink checks flag before each execute(); notify QueryLifetimeController for pipeline stop.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Rust Sink Framework (Pure Rust, No FFI)
**Rationale:** AsyncSink trait + SinkContext + SinkTaskHandle + spawn_sink are the foundation. Everything else depends on them. Pure Rust enables fast iteration with `cargo test`.
**Delivers:** AsyncSink trait, SinkContext, SinkTaskHandle, spawn_sink lifecycle, SinkResult error types, unit tests with mock AsyncSink
**Addresses:** AsyncSink trait, SinkContext, bounded channel, buffer ownership model
**Avoids:** Buffer ownership inversion (P1) -- design refcount sequence here; Per-sink vs shared channel (P10) -- per-sink channels decided here

### Phase 2: Sink FFI Bridge
**Rationale:** CXX bridge must exist before C++ integration. Buffer lifecycle across FFI is the highest-risk area and must be validated with refcount assertions.
**Delivers:** CXX bridge functions (spawn, try_send, request_stop, is_flushed, stop), buffer retain/release across FFI, C++ wrapper functions
**Addresses:** Buffer data passing across FFI, error reporting via callback
**Avoids:** Double-retain bug (P8) -- write refcount sequence diagram before code; Channel closing semantics (P6) -- specify exact closing sequence

### Phase 3: TokioSink C++ Operator
**Rationale:** Depends on Phase 2 FFI. This is the most complex C++ component with the state machine, backpressure, and flush protocol.
**Delivers:** TokioSink class (start/execute/stop), BackpressureHandler with hysteresis, repeatTask retry, flush-confirm protocol, tryStop polling
**Addresses:** BackpressureController hysteresis, repeatTask retry, guaranteed flush on stop, TokioSink operator
**Avoids:** Backpressure deadlock (P2) -- non-blocking try_send only; Flush ordering race (P3) -- state machine enforces ordering; repeatTask re-entrance (P4) -- state check before every retry; tryStop semantics (P9) -- flushCompleted flag

### Phase 4: Registration + Provider Integration
**Rationale:** Straightforward pattern copy from TokioSourceRegistry. Independent of Phase 3 internals once TokioSink exists.
**Delivers:** TokioSinkRegistry, SinkProvider modification, generated registrar template
**Addresses:** TokioSinkRegistry feature
**Avoids:** No critical pitfalls -- established pattern

### Phase 5: Reference Sink + Integration Tests
**Rationale:** End-to-end validation. GeneratorSink (or NullSink) proves the framework works through the full pipeline.
**Delivers:** Reference AsyncSink implementation, C++ integration tests, system tests (source -> pipeline -> TokioSink)
**Addresses:** All features validated end-to-end
**Avoids:** Runtime starvation (P5) -- concurrent source+sink test measures throughput degradation; Error propagation gap (P7) -- inject errors, verify pipeline stops

### Phase Ordering Rationale

- **Phase 1 before Phase 2:** Pure Rust framework must be correct before adding FFI complexity. The AsyncSink trait API is the contract everything else builds on.
- **Phase 2 before Phase 3:** C++ operator cannot be built without FFI entry points. Buffer lifecycle across FFI is the highest-risk area and must be validated independently.
- **Phase 3 is the critical phase:** Contains the most pitfalls (4 of 10). The TokioSink state machine, flush protocol, and backpressure integration are tightly coupled and must be designed together.
- **Phase 4 is independent:** Registry is a mechanical copy of existing patterns. Can be built in parallel with Phase 5 if needed.
- **Phase 5 validates everything:** Integration tests catch issues that unit tests miss, especially lifecycle races and buffer ownership bugs.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2:** Buffer retain/release sequence across FFI needs a formal refcount diagram before implementation. The double-retain bug from v1.0 resurfaced here.
- **Phase 3:** TokioSink state machine + flush protocol is the most complex new component. Needs detailed design of all state transitions and their interactions with repeatTask.

Phases with standard patterns (skip research-phase):
- **Phase 1:** AsyncSink trait mirrors AsyncSource; spawn_sink mirrors spawn_source. Well-understood patterns.
- **Phase 4:** Direct copy of TokioSourceRegistry + BaseRegistry pattern. No research needed.
- **Phase 5:** Standard integration test patterns. Test scenarios are defined by the pitfalls checklist.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Zero new dependencies. Every primitive validated in v1.0 production-like stress tests. |
| Features | HIGH | Direct codebase analysis of NetworkSink, Sink base class, and v1.0 source framework. Clear table stakes vs differentiators. |
| Architecture | HIGH | Data flow inversion is well-understood. Per-sink channels, no bridge thread, repeatTask retry -- all grounded in existing patterns. |
| Pitfalls | HIGH | 10 pitfalls identified from v1.0 post-mortem lessons and NetworkSink reference. Each has concrete prevention strategy. |

**Overall confidence:** HIGH

### Gaps to Address

- **Channel type disagreement:** STACK.md recommends `async_channel` (send_blocking for non-Tokio threads), FEATURES.md recommends `tokio::sync::mpsc` (try_send for non-blocking). Both are valid; the choice depends on whether C++ pipeline threads use `try_send` (non-blocking, mpsc works) or `send_blocking` (blocking, needs async_channel). Since the architecture mandates non-blocking sends from pipeline threads, `tokio::sync::mpsc::try_send` is sufficient and avoids the `async_channel` dependency for the sink direction. Resolve during Phase 1 design.
- **BackpressureHandler extraction:** Whether to extract NetworkSink's BackpressureHandler into a shared header (Option A) or duplicate the ~60 lines (Option B). Recommendation: Option A (extract), but this touches NetworkSink code which is outside the Tokio framework scope. Resolve during Phase 3 planning.
- **Channel capacity defaults:** 1024 (STACK.md) vs 64 (FEATURES.md). NetworkSink uses configurable sender_queue_size. Hardcode a reasonable default and make configurable via SinkDescriptor. Resolve during Phase 1.
- **Runtime thread sizing:** Current default is 2 Tokio threads (sized for sources only). Adding sinks may require increasing this. No hard data on optimal count -- validate during Phase 5 performance testing.

## Sources

### Primary (HIGH confidence)
- Existing v1.0 codebase: `nes-sources/rust/` -- validated source framework, all patterns and version pins
- NetworkSink reference: `nes-plugins/Sinks/NetworkSink/` -- BackpressureHandler, repeatTask retry, flush polling
- Sink infrastructure: `nes-sinks/include/Sinks/Sink.hpp` -- BackpressureController ownership, ExecutablePipelineStage interface
- Pipeline execution: `nes-executable/include/PipelineExecutionContext.hpp` -- repeatTask API
- Registry patterns: `nes-sources/registry/`, `nes-sinks/registry/` -- BaseRegistry, generated registrar
- v1.0 post-mortem: double-retain bug, EOS ordering race, bridge thread design rationale

### Secondary (MEDIUM confidence)
- Tokio documentation -- async runtime behavior, CancellationToken patterns
- async-channel documentation -- closing semantics, send_blocking vs try_send behavior

---
*Research completed: 2026-03-16*
*Ready for roadmap: yes*
