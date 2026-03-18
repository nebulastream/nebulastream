# Technology Stack: Async Rust Sinks (v1.1)

**Project:** Tokio-Based Rust I/O Framework for NebulaStream -- Sink Extension
**Researched:** 2026-03-16
**Overall confidence:** HIGH

## Executive Summary

The sink framework requires **zero new Rust crate dependencies**. The existing stack (tokio 1.40.0, async-channel 2.3.1, cxx 1.0, DashMap 6.1, atomic-waker 1.1, tokio-util 0.7.13) already provides every primitive needed for async sinks. The sink's data flow is the mirror image of the source -- C++ pipeline threads push TupleBuffers into a bounded async_channel, and a Rust async task receives and writes them via an AsyncSink trait. Backpressure uses the same AtomicWaker + AtomicBool pattern but with hysteresis thresholds (mirroring C++ BackpressureHandler). The shared Tokio runtime, CXX bridge, and Corrosion/CMake integration are reused as-is.

## Recommended Stack

### Reused from v1.0 (NO changes needed)

| Technology | Version | Purpose | Why Reuse |
|------------|---------|---------|-----------|
| tokio | 1.40.0 | Shared async runtime | Sinks run as Tokio tasks on the same runtime as sources. No version bump needed. |
| async-channel | 2.3.1 | Bounded MPSC channel C++ -> Rust | `send_blocking()` from C++ pipeline threads, `recv().await` on Rust side. Same channel type, reversed direction vs sources. |
| cxx | 1.0 | Rust <-> C++ FFI bridge | CXX bridge for TokioSink C++ operator calling Rust spawn/stop/send/flush. Same pattern as TokioSource. |
| dashmap | 6.1 | Per-sink registry | SINK_REGISTRY mirrors EMIT_REGISTRY using DashMap for concurrent sink task lookup. |
| atomic-waker | 1.1 | Async-safe signaling | Reused for flush confirmation signal (Rust sink task signals C++ that flush is complete). |
| tokio-util | 0.7.13 | CancellationToken for cooperative shutdown | Same cancel pattern as sources: token cancelled on stop, sink flushes remaining data before exit. |
| tracing | 0.1 | Structured logging | Consistent with source framework logging. |
| Corrosion | (cmake) | Rust/CMake integration | Existing Corrosion setup handles the staticlib build. No changes. |

### New Capabilities Using Existing Primitives

No new crate dependencies. The following are new **modules/traits** built from existing primitives:

| Capability | Built From | Purpose |
|------------|-----------|---------|
| `AsyncSink` trait | New Rust trait (open/write/flush/close) | Sink author interface -- mirror of AsyncSource |
| `SinkContext` | async_channel::Receiver, CancellationToken | Receives buffers from C++, provides cancel signal and flush trigger |
| `SinkTaskHandle` | CancellationToken (tokio-util) | Opaque handle for C++ to stop/flush a sink task |
| Flush confirmation | AtomicBool (std) | Sink task sets flag when flush completes; C++ polls via repeatTask |
| `SINK_DISPATCH` | DashMap<u64, Sender> | Per-sink channel sender registry for C++ execute() to find the right channel |
| `SinkBridgeMessage` | enum { Data(TupleBufferHandle), Flush, Stop } | Message type for C++ -> Rust channel |

## Stack Architecture: Source vs Sink Data Flow

```
SOURCE (v1.0 -- validated):
  Rust AsyncSource --emit()--> async_channel --recv_blocking()--> Bridge Thread --bridge_emit()--> C++ TaskQueue

SINK (v1.1 -- new):
  C++ Pipeline Thread --send_blocking()--> async_channel --recv().await--> Rust AsyncSink::write()
```

**Key architectural difference: No bridge thread needed for sinks.** Sources required a dedicated bridge thread because the C++ task queue needs synchronous dispatch from a non-Tokio thread. Sinks receive directly -- the Rust async task calls `receiver.recv().await` on the Tokio runtime. The C++ pipeline thread calls `sender.send_blocking()` which blocks only that pipeline worker (acceptable -- pipeline threads are already dedicated to processing).

### Why async_channel for the Sink Direction

async_channel 2.3.1 provides both `send_blocking()` (for C++ callers on non-Tokio threads) and `recv().await` (for Rust async tasks). This is the exact inverse of the source pattern (`send().await` from Rust, `recv_blocking()` from bridge thread). Using the same channel library avoids adding tokio::sync::mpsc which would require the sender to be on a Tokio context. Pipeline threads are not Tokio-managed, so `send_blocking()` is the correct API.

### Why No Bridge Thread for Sinks

Sources needed a bridge thread because:
1. The destination (C++ Folly MPMC task queue) requires synchronous calls
2. Multiple sources share one channel to one bridge thread

Sinks do NOT need a bridge thread because:
1. The destination (Rust async task) natively supports `.await`
2. Each sink has its own dedicated channel and Tokio task
3. The C++ pipeline thread calling `send_blocking()` is already a dedicated worker

## C++ Integration Points (New)

### TokioSink C++ Operator

Mirrors TokioSource but implements the `Sink` interface (`start`/`execute`/`stop`):

| C++ Component | Existing Pattern | Sink Adaptation |
|---------------|------------------|-----------------|
| `TokioSink : public Sink` | `TokioSource` | Holds SpawnFn, BackpressureController, closed flag |
| `TokioSink::start(PEC&)` | `TokioSource::start()` | Calls Rust `spawn_sink()` via CXX; creates per-sink channel; registers sender in SINK_DISPATCH |
| `TokioSink::execute(buffer, PEC&)` | `NetworkSink::execute()` | Calls Rust FFI to try_send buffer; if channel full, uses `repeatTask(buffer, 10ms)` |
| `TokioSink::stop(PEC&)` | `NetworkSink::stop()` | Sends Flush via FFI; polls flush_complete via `repeatTask({}, 10ms)` until confirmed |
| `TokioSinkRegistry` | `TokioSourceRegistry` (BaseRegistry) | Same registry pattern with SinkDescriptor + BackpressureController args |

### CXX Bridge Additions (in nes-source-lib)

New `ffi_sink` bridge module alongside existing `ffi_source`:

```rust
#[cxx::bridge]
pub mod ffi_sink {
    extern "Rust" {
        type SinkHandle;

        /// Spawn a sink task on the shared Tokio runtime.
        /// Returns opaque handle for stop/flush operations.
        fn spawn_sink(
            sink_id: u64,
            channel_capacity: u32,
            // ... config params as usize (same CXX pointer pattern)
        ) -> Result<Box<SinkHandle>>;

        /// Try to send a buffer to the sink's channel.
        /// Returns 0=Ok, 1=Full, 2=Closed.
        fn send_buffer_to_sink(sink_id: u64, buffer_ptr: usize) -> u8;

        /// Request flush. Non-blocking, returns immediately.
        fn request_flush(sink_id: u64);

        /// Check if flush is complete. Returns true when all buffers written.
        fn is_flush_complete(sink_id: u64) -> bool;

        /// Stop the sink (cancel token + close channel).
        fn stop_sink(handle: &SinkHandle);
    }
}
```

**Why `send_buffer_to_sink` uses a global registry lookup instead of the SinkHandle:** The C++ `execute()` method is called by pipeline worker threads that do not hold the SinkHandle (owned by TokioSink). The SINK_DISPATCH DashMap maps sink_id to the async_channel::Sender, so any thread can send. This mirrors how EMIT_REGISTRY maps source_id to emit callbacks.

### repeatTask-Based Retry Pattern (from NetworkSink)

The existing `PipelineExecutionContext::repeatTask(buffer, interval)` re-enqueues the current task after a delay without blocking the pipeline thread. TokioSink uses this for:

1. **Channel full in execute():** `send_buffer_to_sink` returns `Full` -> TokioSink calls `pec.repeatTask(inputBuffer, 10ms)` and returns. Identical to NetworkSink's `SendResult::Full` handling.
2. **Flush on stop():** `request_flush()` sends a Flush sentinel through the channel -> TokioSink polls `is_flush_complete()` via `pec.repeatTask({}, 10ms)` until true. Identical to NetworkSink's `flush_sender_channel` polling.

No new C++ mechanisms needed. The 10ms interval matches `NetworkSink::BACKPRESSURE_RETRY_INTERVAL`.

### BackpressureController with Hysteresis

NetworkSink's `BackpressureHandler` implements hysteresis:
- **Upper threshold (default 1000):** When buffered count >= upper, `backpressureController.applyPressure()`
- **Lower threshold (default 200):** When buffered count <= lower, `backpressureController.releasePressure()`

For TokioSink, the Rust async task monitors its async_channel fill level:
- Channel `len()` reaches upper threshold -> call FFI to `backpressureController.applyPressure()`
- Channel `len()` drops to lower threshold -> call FFI to `backpressureController.releasePressure()`

**No Rust-side deque needed.** The async_channel itself provides the buffering. The hysteresis thresholds control when the C++ BackpressureController signals upstream sources, not additional queueing.

The BackpressureController is owned by the C++ Sink base class (same as NetworkSink). Rust calls back into C++ to apply/release pressure -- the reverse direction of v1.0 where C++ called into Rust for source backpressure.

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Channel (C++ -> Rust) | async_channel 2.3.1 | tokio::sync::mpsc | mpsc Sender requires Tokio context; pipeline threads are not on Tokio. async_channel::send_blocking() works from any thread. |
| Channel (C++ -> Rust) | async_channel 2.3.1 | crossbeam-channel | No async receiver. Would need spawn_blocking wrapper, adding unnecessary complexity. |
| Backpressure signaling | AtomicBool polled by Rust task | tokio::sync::Notify | Channel len() can be checked inline during recv() loop. No need for a separate notification mechanism. |
| Sink task model | One Tokio task per sink | Thread-per-sink | Defeats the purpose of the async framework. Tokio tasks support concurrent I/O without thread-per-connection. |
| Flush confirmation | AtomicBool polled via repeatTask | std::future/promise across FFI | repeatTask is the engine's non-blocking retry pattern (proven by NetworkSink). Cross-FFI futures add complexity. |
| Bridge thread for sinks | No bridge thread | Bridge thread (like sources) | Sources need it because C++ task queue requires sync dispatch. Sinks receive into async Rust -- direct recv() is simpler. |
| New crates for sinks | Add modules to existing crates | Separate nes-sink-* crates | Same runtime, same buffer types, same CXX patterns. Separate crates create unnecessary dependency management. Crate names are historical. |

## What NOT to Add

| Do NOT Add | Reason |
|------------|--------|
| New Rust crate dependencies | Existing stack covers all sink requirements. Zero additions to Cargo.toml. |
| Separate Tokio runtime for sinks | Sinks share the same SOURCE_RUNTIME singleton. Single runtime is a project constraint. |
| tokio::sync::mpsc | Requires sender on Tokio context; C++ pipeline threads are not Tokio-managed. |
| crossbeam-channel | Adds a dependency for functionality async_channel already provides. |
| Bridge thread for sinks | Unnecessary indirection -- Rust async task receives directly via recv().await. |
| New buffer allocation in sinks | Sinks receive buffers from C++ pipeline (read-only). TupleBufferHandle reused as-is. |
| Folly::Synchronized in Rust | Use async_channel bounded capacity + AtomicBool. No Folly-style locks in Rust. |
| cxx-async | Young crate (0.1.x), requires C++20 coroutines throughout calling code. The simpler AtomicBool + repeatTask pattern is proven by NetworkSink. |

## Crate Structure

No new crates. Sink modules added to existing crates:

| Crate | New Modules | Purpose |
|-------|-------------|---------|
| `nes_source_runtime` | `sink.rs`, `sink_context.rs` | AsyncSink trait, SinkContext, spawn_sink/stop_sink lifecycle, SINK_DISPATCH registry |
| `nes_source_lib` | `ffi_sink` bridge module in `lib.rs` | CXX bridge declarations for sink spawn/stop/send/flush |
| `nes_source_bindings` | No changes expected | Existing TupleBuffer bindings sufficient for read-only sink access |

**Rationale for not creating new crates:** The sink framework shares the same runtime, same TupleBufferHandle, same CancellationToken patterns, and same CXX bridge infrastructure. Splitting into separate crates would create unnecessary dependency management overhead. The crate names (`nes_source_*`) are historical -- they serve as the general Tokio I/O framework crates.

## TupleBufferHandle Reuse for Sinks

Sinks receive TupleBufferHandle from C++ (constructed from the raw TupleBuffer pointer passed via `send_buffer_to_sink`). The existing handle provides:

- `as_slice()` -- read buffer data (sinks read, not write)
- `number_of_tuples()` -- how many tuples to process
- `capacity()` -- buffer size in bytes
- Clone via `cloneTupleBuffer` (retain/release ref counting)
- Drop calls `release()` -- buffer recycled when sink task is done

**No new buffer operations needed.** Sinks consume buffers; they do not allocate or modify them. The C++ side calls `retain()` before passing to Rust (same pattern as bridge_emit but in reverse).

## Configuration

| Parameter | Source | Default | Purpose |
|-----------|--------|---------|---------|
| Tokio runtime threads | `WorkerConfiguration::tokioRuntimeThreads` | 2 | Shared with sources. No change. |
| Channel capacity | SinkDescriptor config | 1024 | Bounded async_channel capacity per sink |
| Backpressure upper threshold | SinkDescriptor config | 1000 | Channel fill level triggering pressure (from NetworkSink defaults) |
| Backpressure lower threshold | SinkDescriptor config | 200 | Channel fill level releasing pressure (from NetworkSink defaults) |
| Retry interval | Constant | 10ms | repeatTask interval, matches NetworkSink::BACKPRESSURE_RETRY_INTERVAL |

## Installation

No changes to any Cargo.toml files. Existing dependencies cover all sink needs:

```toml
# nes_source_runtime/Cargo.toml -- UNCHANGED
[dependencies]
nes_source_bindings = { path = "../nes-source-bindings" }
cxx = "1.0"
tokio = { version = "1.40.0", features = ["rt-multi-thread", "sync", "time", "macros"] }
tokio-util = { version = "0.7.13", features = ["rt"] }
async-channel = "2.3.1"
tracing = "0.1"
atomic-waker = "1.1"
dashmap = "6.1"
```

## Sources

- **Existing codebase (PRIMARY):** `nes-sources/rust/` -- validated v1.0 stack, all version pins confirmed in Cargo.toml files
- **NetworkSink reference:** `nes-plugins/Sinks/NetworkSink/NetworkSink.{hpp,cpp}` -- BackpressureHandler hysteresis (upper=1000, lower=200), repeatTask retry pattern, flush polling
- **Sink base class:** `nes-sinks/include/Sinks/Sink.hpp` -- BackpressureController ownership, ExecutablePipelineStage interface
- **PipelineExecutionContext:** `nes-executable/include/PipelineExecutionContext.hpp` -- repeatTask(buffer, interval) API
- **SinkRegistry:** `nes-sinks/registry/include/SinkRegistry.hpp` -- BaseRegistry pattern for TokioSinkRegistry
- **TokioSourceRegistry:** `nes-sources/registry/include/TokioSourceRegistry.hpp` -- Template for TokioSinkRegistry
- **async-channel:** send_blocking/recv API confirmed in v1.0 source bridge usage

## Confidence Assessment

| Area | Confidence | Reason |
|------|------------|--------|
| No new dependencies needed | HIGH | Every primitive exists in validated v1.0 stack; each has been exercised in production-like stress tests |
| async_channel for sink direction | HIGH | send_blocking + async recv is the exact inverse of the source pattern, both APIs used in v1.0 |
| No bridge thread for sinks | HIGH | Rust async task receives directly; no sync dispatch target like C++ task queue |
| repeatTask for retry/flush | HIGH | Direct pattern copy from NetworkSink; PipelineExecutionContext API confirmed in header |
| Hysteresis backpressure | HIGH | BackpressureHandler code reviewed; upper/lower threshold pattern clear |
| Crate structure (no new crates) | MEDIUM | Functionally correct; crate naming (source vs io) is cosmetic and can be addressed later |
| Channel capacity defaults | MEDIUM | 1024 is reasonable but may need tuning; NetworkSink uses sender_queue_size which varies |
