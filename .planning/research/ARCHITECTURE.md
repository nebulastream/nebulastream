# Architecture Patterns: Async Rust Sinks in NebulaStream

**Domain:** Stream processing engine -- async Rust I/O framework extension (sinks)
**Researched:** 2026-03-16
**Confidence:** HIGH (analysis based on direct codebase examination of existing v1.0 source framework + C++ sink infrastructure)

## Executive Summary

The existing async Rust source framework establishes a clear pattern: Rust async tasks produce data, a bridge thread serializes it into the C++ pipeline via `async_channel`, and per-source registries dispatch callbacks. Sinks invert this data flow -- C++ pipeline threads push buffers into Rust async tasks that write them to external systems. This inversion has fundamental architectural consequences that prevent naive reuse of the source bridge thread and require new components purpose-built for the sink direction.

The key architectural insight is that **sinks do not need a bridge thread at all**. Sources needed one because async Rust producers had to serialize into a synchronous C++ consumer. Sinks have the opposite topology: synchronous C++ producers (pipeline worker threads) push into async Rust consumers (sink tasks). The C++ side uses `try_send` (non-blocking) with `repeatTask`-based retry -- the same pattern proven by NetworkSink.

## Recommended Architecture

### Data Flow Comparison

```
SOURCE FLOW (existing v1.0):
  Rust AsyncSource.run()
    -> SourceContext.emit(buffer).await
      -> async_channel::send (bounded, async)
        -> Bridge Thread (recv_blocking)        <-- single OS thread
          -> EMIT_REGISTRY dispatch
            -> bridge_emit() C-linkage callback
              -> EmitFunction (into Folly MPMC TaskQueue)

SINK FLOW (new v1.1):
  C++ Pipeline Worker Thread (Folly MPMC)
    -> TokioSink.execute(buffer, pec)
      -> sink_try_send() FFI call (non-blocking!)
        -> async_channel::try_send on per-sink bounded channel
        -> If Full: pec.repeatTask(buffer, interval)   <-- C++ retry
        -> If Ok: buffer enters channel
      -> Rust SinkTask (recv().await on Tokio runtime)
        -> AsyncSink.write(buffer).await
          -> External system I/O (async)

SINK STOP FLOW:
  C++ Pipeline calls TokioSink.stop(pec)
    -> sink_request_stop() FFI call: closes channel sender
    -> Rust SinkTask drains remaining buffers
    -> Rust SinkTask calls AsyncSink.flush().await
    -> Rust SinkTask calls AsyncSink.close().await
    -> Rust sets flush_complete AtomicBool
    -> C++ checks sink_is_flushed() FFI call
    -> If not flushed: pec.repeatTask({}, interval)    <-- retry until flush
    -> If flushed: cleanup complete
```

### Why Sinks Cannot Reuse the Source Bridge Thread

The source bridge thread is a **Rust-to-C++ serialization point**: it calls `recv_blocking()` on an `async_channel` and dispatches into C++ emit callbacks. Sinks need the **opposite direction: C++-to-Rust**. Three constraints make reuse impossible:

1. **Non-blocking requirement on C++ side.** Pipeline worker threads call `execute()` from the Folly MPMC thread pool. Blocking a worker thread waiting for Rust to drain a channel would stall the entire pipeline. NetworkSink proves this pattern: it uses `try_send_data` (non-blocking) + `repeatTask` for retry.

2. **Different threading topology.** Source bridge: single dedicated thread doing blocking recv from many async producers. Sink: multiple C++ worker threads doing non-blocking sends into per-sink channels consumed by per-sink async tasks. No serialization point exists or is needed.

3. **Different lifecycle semantics.** Sources send EOS downstream through the bridge channel. Sinks receive a `stop()` call from the pipeline and must guarantee flush before returning. The flush-confirm protocol is fundamentally different from the source EOS protocol.

**Decision: Sinks use per-sink bounded `async_channel` with `try_send` from C++ worker threads. No bridge thread.**

### Component Boundaries

| Component | Responsibility | Communicates With | New/Modified |
|-----------|---------------|-------------------|-------------|
| `AsyncSink` trait | User-facing sink interface: open/write/flush/close | SinkTask | **NEW** (Rust) |
| `SinkContext` | Provides cancellation token + metadata to sink authors | AsyncSink implementations | **NEW** (Rust) |
| `SinkTask` (spawn_sink) | Tokio task: recv from channel, call AsyncSink methods, signal completion | async_channel Receiver, AsyncSink | **NEW** (Rust) |
| `SinkTaskHandle` | Opaque handle: holds channel Sender, flush status, cancellation | TokioSink (C++) | **NEW** (Rust) |
| `sink_try_send` | C-linkage FFI: non-blocking buffer push into channel | TokioSink.execute(), SinkTask | **NEW** (Rust FFI) |
| `sink_request_stop` | C-linkage FFI: close channel, initiate drain+flush | TokioSink.stop() | **NEW** (Rust FFI) |
| `sink_is_flushed` | C-linkage FFI: check if flush completed | TokioSink.stop() | **NEW** (Rust FFI) |
| `TokioSink` (C++) | Sink subclass: start/execute/stop + backpressure | SinkTaskHandle via FFI, PipelineExecutionContext | **NEW** (C++) |
| `BackpressureHandler` | Hysteresis buffer management (reuse NetworkSink's) | TokioSink, BackpressureController | **REUSED** (extract from NetworkSink or duplicate) |
| `TokioSinkRegistry` (C++) | Maps sink type names to TokioSink factories | SinkProvider | **NEW** (C++) |
| `SinkProvider` (C++) | Creates sinks, check TokioSinkRegistry before SinkRegistry | TokioSinkRegistry, SinkRegistry | **MODIFIED** (C++) |
| Shared Tokio runtime | Runs both source and sink tasks | spawn_sink(), spawn_source() | **REUSED** (no changes needed) |
| `nes-source-bindings` crate | CXX bridge + shared runtime | Add sink FFI entry points | **MODIFIED** (Rust) |
| `nes-source-runtime` crate | Runtime framework | Add sink module tree | **MODIFIED** (Rust) |
| `nes-source-lib` crate | CXX re-exports | Add sink CXX bridge functions | **MODIFIED** (Rust) |

### What Does NOT Change

- `RunningSource`, `SourceHandle`, `SourceThread` -- source-side only
- `QueryEngine`, `TaskQueue` -- pipeline execution is sink-implementation-agnostic
- `BackpressureChannel` -- controller/listener mechanism unchanged
- `BufferManager`, `AbstractBufferProvider` -- buffer pool shared, no changes
- Bridge thread -- source-only, not used by sinks
- `EMIT_REGISTRY`, `BACKPRESSURE_REGISTRY` -- source-only global state
- `AsyncSource`, `SourceContext`, `SourceTaskHandle` -- source-only, unchanged

### Crate Organization: Extend Existing Crates

**Decision: Extend existing crates, not create separate nes-sink-* crates.**

Rationale:
1. **Shared runtime.** The Tokio runtime is in `nes-source-bindings` via `OnceLock<Runtime>`. A separate `nes-sink-bindings` crate would need cross-crate runtime sharing (additional complexity, no benefit).
2. **Shared FFI patterns.** `TupleBuffer`, `BufferProviderHandle`, CXX bridge types are already in `nes-source-bindings`. Sinks need the same FFI wrappers.
3. **No circular dependencies.** Sink code depends on the same crate internals (buffer, runtime). Adding modules is simpler than managing inter-crate deps.
4. **Naming.** Crates can be renamed later (e.g., `nes-io-runtime`) if desired. Premature separation adds build complexity now.

New module structure in `nes-source-runtime`:
```
src/
  lib.rs            -- add pub mod sink, sink_context, sink_handle
  sink.rs           -- AsyncSink trait + spawn_sink + SinkTask
  sink_context.rs   -- SinkContext (cancellation, metadata for sink authors)
  sink_handle.rs    -- SinkTaskHandle (channel sender, flush status, stop)
  ... (existing source modules unchanged)
```

New FFI in `nes-source-lib` and `nes-source-bindings`:
```rust
// nes-source-lib CXX bridge additions:
fn spawn_generator_sink(...) -> Box<SinkHandle>;
fn sink_try_send(handle: &SinkHandle, buffer_ptr: usize) -> u8;
fn sink_request_stop(handle: &SinkHandle);
fn sink_is_flushed(handle: &SinkHandle) -> bool;
fn stop_sink(handle: &SinkHandle);
```

## Detailed Component Design

### 1. AsyncSink Trait (Rust)

```rust
/// Primary trait for sink implementations.
///
/// Sink authors implement open/write/flush/close lifecycle methods.
/// The framework handles channel management, backpressure signaling, and FFI.
pub trait AsyncSink: Send + 'static {
    /// Initialize the sink (connect to external system).
    fn open(&mut self) -> impl Future<Output = SinkResult> + Send;

    /// Write a single buffer to the external system.
    /// Called once per buffer received from the C++ pipeline.
    /// The buffer is borrowed -- sink must extract data before returning.
    fn write(
        &mut self,
        buffer: &TupleBufferHandle,
    ) -> impl Future<Output = SinkResult> + Send;

    /// Flush all pending data. Called during stop before close.
    /// Must guarantee all previously written data is durable/sent.
    fn flush(&mut self) -> impl Future<Output = SinkResult> + Send;

    /// Close the sink (disconnect, release resources).
    fn close(&mut self) -> impl Future<Output = SinkResult> + Send;
}
```

### 2. SinkTaskHandle (Rust)

```rust
/// Opaque handle returned to C++ from spawn_sink.
///
/// Contains:
/// - Channel sender for try_send from C++ worker threads
/// - Shared AtomicBool for flush completion status
/// - CancellationToken for cooperative shutdown
pub struct SinkTaskHandle {
    sender: Option<async_channel::Sender<TupleBufferHandle>>,
    flush_complete: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
    sink_id: u64,
}

impl SinkTaskHandle {
    /// Non-blocking try_send. Returns 0=Ok, 1=Full, 2=Closed.
    pub fn try_send(&self, buffer: TupleBufferHandle) -> u8 { ... }

    /// Close the channel sender, initiating drain+flush.
    pub fn request_stop(&mut self) {
        self.sender.take(); // Drop sender -> channel closes
        self.cancellation_token.cancel();
    }

    /// Check if the Rust sink task has completed flush.
    pub fn is_flushed(&self) -> bool {
        self.flush_complete.load(Ordering::Acquire)
    }
}
```

### 3. SinkTask Lifecycle (Rust)

```rust
pub fn spawn_sink(
    sink_id: u64,
    mut sink: impl AsyncSink,
    channel_capacity: usize,
) -> Box<SinkTaskHandle> {
    let (sender, receiver) = async_channel::bounded(channel_capacity);
    let flush_complete = Arc::new(AtomicBool::new(false));
    let cancel_token = CancellationToken::new();

    let flush_flag = flush_complete.clone();
    let runtime = crate::runtime::source_runtime();

    runtime.spawn(async move {
        // 1. Open
        if let Err(e) = sink.open().await { /* error handling */ return; }

        // 2. Receive loop: drain channel, write each buffer
        while let Ok(buffer) = receiver.recv().await {
            if let Err(e) = sink.write(&buffer).await { /* error handling */ break; }
            // buffer dropped here -> release() called on TupleBuffer
        }
        // Channel closed (sender dropped by request_stop)

        // 3. Flush -- guarantee all data written
        let _ = sink.flush().await;

        // 4. Close
        let _ = sink.close().await;

        // 5. Signal completion
        flush_flag.store(true, Ordering::Release);
    });

    Box::new(SinkTaskHandle {
        sender: Some(sender),
        flush_complete,
        cancellation_token: cancel_token,
        sink_id,
    })
}
```

### 4. TokioSink C++ Class

```cpp
class TokioSink final : public Sink {
public:
    struct RustSinkHandleImpl; // PIMPL for rust::Box<SinkHandle>

    using SpawnFn = std::function<std::unique_ptr<RustSinkHandleImpl>(/* params */)>;

    TokioSink(BackpressureController controller, SpawnFn spawnFn,
              size_t upperThreshold, size_t lowerThreshold);

    void start(PipelineExecutionContext& pec) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

private:
    SpawnFn spawnFn;
    std::unique_ptr<RustSinkHandleImpl> rustHandle;
    BackpressureHandler backpressureHandler; // hysteresis from NetworkSink pattern
    bool stopRequested{false};
};
```

**execute() -- mirrors NetworkSink pattern:**

```cpp
void TokioSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) {
    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer) {
        // Copy buffer for Rust (retain/release lifecycle)
        auto result = ::sink_try_send(*rustHandle->handle, &*currentBuffer);
        switch (result) {
            case 0: // Ok
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                break;
            case 1: // Full
                if (auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController)) {
                    pec.repeatTask(*retry, RETRY_INTERVAL);
                }
                return;
            case 2: // Closed
                throw CannotOpenSink("TokioSink channel closed unexpectedly");
        }
    }
}
```

**stop() -- mirrors NetworkSink flush pattern:**

```cpp
void TokioSink::stop(PipelineExecutionContext& pec) {
    if (!stopRequested) {
        INVARIANT(backpressureHandler.empty(), "BackpressureHandler not empty at stop");
        ::sink_request_stop(*rustHandle->handle);
        stopRequested = true;
    }

    if (!::sink_is_flushed(*rustHandle->handle)) {
        pec.repeatTask({}, FLUSH_RETRY_INTERVAL);
        return;
    }
    // Flush complete -- cleanup
}
```

### 5. BackpressureController Ownership -- No Changes Needed

The existing `BackpressureController` ownership model works unchanged for TokioSink:

```
createBackpressureChannel()
  -> (BackpressureController, BackpressureListener) pair

BackpressureController
  -> moved into SinkRegistryArguments
  -> moved into Sink base class (backpressureController member)
  -> TokioSink inherits it

BackpressureListener
  -> given to sources in the same query plan
  -> For C++ SourceThread: listener.wait(stop_token) blocks source thread
  -> For TokioSource: translated to async via BackpressureState + AtomicWaker
```

**The TokioSink calls `backpressureController.applyPressure()` and `releasePressure()` from C++ code (in execute/stop), not from Rust.** The backpressure is a C++ mechanism that throttles C++ or Rust sources through the existing listener infrastructure. No new Rust backpressure code is needed for sinks.

The `BackpressureHandler` class (currently defined inside `NetworkSink.cpp`) provides hysteresis logic. TokioSink needs the same pattern:
- Upper threshold: channel full + backlog count >= threshold -> `applyPressure()`
- Lower threshold: backlog drains below threshold -> `releasePressure()`

**Option A:** Extract `BackpressureHandler` from NetworkSink into a shared header. Both NetworkSink and TokioSink use it.
**Option B:** Duplicate the logic (it is ~60 lines). Keeps NetworkSink unchanged.

Recommendation: **Option A** -- extract to shared header in `nes-sinks/include/Sinks/BackpressureHandler.hpp`. The hysteresis logic is generic and both sinks need identical behavior.

### 6. Flush Semantics -- The Flush-Confirm Protocol

The flush guarantee requires a protocol between C++ `TokioSink::stop()` and the Rust `SinkTask`:

```
Timeline:
1. Pipeline calls TokioSink::stop(pec) on a worker thread
2. TokioSink calls sink_request_stop(handle):
   - Drops the async_channel::Sender (closes channel)
   - Sets CancellationToken (for cooperative abort if needed)
3. TokioSink calls sink_is_flushed(handle):
   - Reads shared AtomicBool -- initially false
4. Not flushed: pec.repeatTask({}, FLUSH_RETRY_INTERVAL)
   - stop() returns; worker thread does other work
   - TaskQueue re-executes stop() after interval
5. Meanwhile, Rust SinkTask:
   - recv() returns Err (channel closed)
   - Calls sink.flush().await (user implementation)
   - Calls sink.close().await
   - Sets flush_complete = true (AtomicBool::store, Release)
6. Next stop() call:
   - sink_is_flushed() reads true
   - TokioSink proceeds with cleanup
```

This mirrors NetworkSink exactly: `stop()` calls `flush_sender_channel()` and uses `repeatTask` to retry if flush is incomplete. The critical property: `stop()` never blocks a pipeline worker thread.

### 7. Buffer Lifecycle Across FFI for sink_try_send

When C++ calls `sink_try_send(handle, &buffer)`:

1. C++ passes a `const TupleBuffer&` reference
2. Rust FFI receives it as a raw pointer to `TupleBuffer`
3. Rust copies the buffer via CXX `cloneTupleBuffer` (which calls retain internally)
4. The `TupleBufferHandle` wrapping the clone is sent through the channel
5. After `AsyncSink::write()` processes it, `TupleBufferHandle` is dropped (calls release)
6. Net effect: buffer refcount incremented on try_send, decremented after write -- correct lifecycle

This is the inverse of the source bridge pattern where `bridge_emit` copies the buffer via `TupleBuffer(*rawBuffer)`.

## Patterns to Follow

### Pattern 1: SpawnFn Indirection (from TokioSource)
**What:** TokioSink takes a `SpawnFn` that captures sink-specific config and calls the appropriate Rust FFI entry point. Different sink types provide different SpawnFns.
**Why:** Avoids exposing Rust FFI types in the TokioSink header. The SpawnFn encapsulates the CXX bridge call.

### Pattern 2: PIMPL for Rust Handles (from TokioSource)
**What:** `TokioSink::RustSinkHandleImpl` wraps `rust::Box<SinkHandle>`, defined in .cpp only.
**Why:** Avoids `#include <rust/cxx.h>` in public headers.

### Pattern 3: BackpressureHandler with Hysteresis (from NetworkSink)
**What:** Upper threshold triggers `applyPressure()`, lower threshold triggers `releasePressure()`. Buffer backlog stored in `folly::Synchronized<deque>`.
**Why:** Prevents oscillation. Without hysteresis, pressure toggles on every buffer.

### Pattern 4: repeatTask for Non-Blocking Retry (from NetworkSink)
**What:** When `try_send` returns Full, `pec.repeatTask(buffer, interval)` re-enqueues the buffer. When `stop()` can't flush yet, same pattern.
**Why:** Pipeline worker threads must never block. This is an engine invariant.

### Pattern 5: Registry + Generated Registrar (from TokioSourceRegistry)
**What:** `TokioSinkRegistry` mirrors `TokioSourceRegistry` structure. Uses `BaseRegistry` + `.inc.in` template.
**Why:** Consistent registration pattern. SinkProvider checks TokioSinkRegistry before SinkRegistry, mirroring SourceProvider's TokioSourceRegistry-first pattern.

### Pattern 6: EmitContext / ErrorContext PIMPL (from TokioSource)
**What:** TokioSource uses `EmitContext` and `ErrorContext` structs owned by the C++ class, passed as opaque `void*` to Rust callbacks.
**Why:** For sinks, an analogous `SinkErrorContext` may be needed if error reporting from the Rust sink task to C++ is required. However, sinks are simpler: errors during write/flush can be surfaced when `stop()` polls for flush completion.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Blocking C++ Worker Threads
**What:** Using `channel.send_blocking()` from `execute()`.
**Why bad:** Pipeline worker threads are shared across all queries. Blocking one stalls the entire engine.
**Instead:** `try_send` + `repeatTask` for retry (NetworkSink pattern).

### Anti-Pattern 2: Shared Bridge Thread for Sinks
**What:** Routing all sink buffers through a single bridge thread (like sources do).
**Why bad:** Creates unnecessary serialization. Source bridge works because it serializes async->sync (many Rust producers, one C++ consumer). Sinks have many C++ producers and independent Rust consumers -- per-sink channels are the natural topology.
**Instead:** Per-sink channels with direct try_send from worker threads.

### Anti-Pattern 3: Rust-Side Backpressure State for Sinks
**What:** Creating a Rust `BackpressureState` + `AtomicWaker` for sinks analogous to the source-side `backpressure.rs`.
**Why bad:** Sinks own the `BackpressureController` (the "apply/release" side), not the `BackpressureListener` (the "wait" side). The `BackpressureController` is a C++ object in the `Sink` base class. Apply/release calls happen from C++ `execute()` -- no Rust involvement needed.
**Instead:** Call `backpressureController.applyPressure()` from C++ when channel+backlog is full, `releasePressure()` when drained.

### Anti-Pattern 4: Separate Rust Crates for Sinks
**What:** Creating `nes-sink-bindings`, `nes-sink-runtime`, `nes-sink-lib`.
**Why bad:** Duplicates shared infrastructure (runtime, TupleBuffer wrappers, CXX bridge). Creates inter-crate dependency complexity for the shared Tokio runtime `OnceLock`.
**Instead:** Add sink modules to existing `nes-source-runtime` and sink FFI to `nes-source-bindings`/`nes-source-lib`.

### Anti-Pattern 5: Forgetting Buffer Retain/Release in FFI
**What:** Passing a raw `TupleBuffer*` to Rust without calling retain, or forgetting to release.
**Why bad:** Use-after-free or memory leak.
**Instead:** Use `cloneTupleBuffer` (calls retain) to create a Rust-owned copy. Drop calls release.

### Anti-Pattern 6: Polling for Flush with sleep() in C++
**What:** `while (!sink_is_flushed) { sleep(10ms); }` in `stop()`.
**Why bad:** Blocks the pipeline worker thread, preventing other work.
**Instead:** Use `pec.repeatTask({}, interval)` which re-enqueues the stop task and returns immediately.

## Scalability Considerations

| Concern | At 10 sinks | At 100 sinks | At 1000 sinks |
|---------|-------------|--------------|---------------|
| Channel memory | 10 * capacity buffers -- minimal | 100 * capacity -- moderate | Consider smaller per-sink capacity |
| Tokio tasks | 10 tasks -- trivial for shared runtime | 100 tasks -- fine for I/O-bound | 1000 I/O-bound tasks -- Tokio handles this well |
| Pipeline workers | Unchanged (try_send is non-blocking) | Unchanged | Unchanged |
| Backpressure controllers | 10 controllers -- O(1) each | 100 -- fine | 1000 -- fine (shared Channel is cheap) |
| Flush latency | Depends on sink I/O | Same | Same |

## Build Order (Suggested Phase Structure)

Based on dependency analysis, components should be built in this order:

### Phase 1: Rust Sink Framework (no FFI)
1. `AsyncSink` trait + `SinkResult` error types
2. `SinkContext` (cancellation token, sink_id)
3. `SinkTaskHandle` (channel sender, flush AtomicBool, try_send/request_stop/is_flushed)
4. `spawn_sink` function + SinkTask lifecycle (open/recv-loop/flush/close)
5. Unit tests with mock `AsyncSink` (pure Rust, `cargo test`)

### Phase 2: Sink FFI Bridge
1. CXX bridge additions in `nes-source-lib` (spawn, try_send, request_stop, is_flushed, stop)
2. C++ wrapper functions in `nes-source-bindings/src/SourceBindings.cpp` (or new `SinkBindings.cpp`)
3. Buffer lifecycle: `cloneTupleBuffer` on try_send, TupleBufferHandle drop on write completion

### Phase 3: TokioSink C++ Integration
1. `TokioSink` class (Sink subclass, ExecutablePipelineStage impl)
2. BackpressureHandler extraction/reuse from NetworkSink
3. `execute()` with try_send + repeatTask retry
4. `stop()` with flush-confirm protocol + repeatTask polling

### Phase 4: Registration + Provider
1. `TokioSinkRegistry` (mirroring TokioSourceRegistry)
2. `SinkProvider::lower()` modification (check TokioSinkRegistry first)
3. Generated registrar template (`TokioSinkGeneratedRegistrar.inc.in`)

### Phase 5: Generator Test Sink + Integration Tests
1. `GeneratorSink` (Rust) -- writes to in-memory buffer or file, for testing
2. C++ integration tests -- pipeline with TokioSink
3. System tests -- full query with source -> pipeline -> TokioSink

## Sources

- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/bridge.rs` -- bridge thread, EMIT_REGISTRY, BridgeMessage
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/source.rs` -- AsyncSource trait, spawn_source lifecycle
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/context.rs` -- SourceContext, emit() three-step gating
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/backpressure.rs` -- BackpressureState, BackpressureFuture
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/handle.rs` -- SourceTaskHandle, stop_source
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/runtime.rs` -- shared Tokio runtime delegation
- Direct codebase analysis: `nes-sources/rust/nes-source-bindings/lib.rs` -- CXX bridge, OnceLock<Runtime>
- Direct codebase analysis: `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` -- bridge_emit, retain/release, buffer FFI
- Direct codebase analysis: `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` -- EmitContext, ErrorContext
- Direct codebase analysis: `nes-sources/src/TokioSource.cpp` -- PIMPL, SpawnFn, start/stop/tryStop lifecycle
- Direct codebase analysis: `nes-sources/src/SourceHandle.cpp` -- std::variant dispatch, SourceThread vs TokioSource
- Direct codebase analysis: `nes-sources/src/SourceProvider.cpp` -- TokioSourceRegistry-first lookup, runtime init
- Direct codebase analysis: `nes-sources/registry/include/TokioSourceRegistry.hpp` -- BaseRegistry pattern
- Direct codebase analysis: `nes-plugins/Sinks/NetworkSink/NetworkSink.hpp` -- BackpressureHandler, hysteresis thresholds
- Direct codebase analysis: `nes-plugins/Sinks/NetworkSink/NetworkSink.cpp` -- execute() retry with repeatTask, stop() flush polling
- Direct codebase analysis: `nes-sinks/include/Sinks/Sink.hpp` -- Sink base class, BackpressureController member
- Direct codebase analysis: `nes-sinks/registry/include/SinkRegistry.hpp` -- SinkRegistryArguments, BaseRegistry
- Direct codebase analysis: `nes-sinks/src/SinkProvider.cpp` -- SinkRegistry::instance().create()
- Direct codebase analysis: `nes-executable/include/ExecutablePipelineStage.hpp` -- start/execute/stop interface
- Direct codebase analysis: `nes-executable/include/PipelineExecutionContext.hpp` -- repeatTask signature
- Direct codebase analysis: `nes-executable/include/BackpressureChannel.hpp` -- Controller/Listener, createBackpressureChannel
- `.planning/PROJECT.md` -- project context, v1.1 requirements, key decisions from v1.0

---
*Architecture research for: Async Rust Sinks in NebulaStream (v1.1 milestone)*
*Researched: 2026-03-16*
