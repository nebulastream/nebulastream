# Tokio-Based Rust I/O Framework for NebulaStream

## What This Is

An async Rust I/O framework for NebulaStream that allows writing data sources and sinks as Tokio async tasks instead of blocking C++ implementations. Sources and sinks share a single configurable Tokio runtime and integrate with the existing engine through bounded channels that connect the async world to the C++ pipeline. The framework provides simple `async fn run(ctx)` / `async fn write(buffer)` interfaces while handling backpressure, buffer lifecycle, and FFI internally.

## Current Milestone: v1.1 Async Rust Sinks

**Goal:** Extend the framework with async Rust sinks — TokioSink C++ operator routes buffers to user-defined AsyncSink implementations on the shared Tokio runtime, with backpressure (hysteresis), guaranteed flush on stop, and TokioSinkRegistry.

**Target features:**
- AsyncSink trait (open/write/flush/close) for sink authors
- Bounded channel between C++ pipeline and Rust async task
- BackpressureController with hysteresis thresholds (NetworkSink pattern)
- repeatTask-based retry when channel is full (non-blocking pipeline)
- Guaranteed flush on pipeline stop
- TokioSinkRegistry mirroring TokioSourceRegistry
- Shared Tokio runtime with sources (single I/O runtime)

## Core Value

I/O authors can write async Rust sources and sinks with simple trait methods — all complexity (backpressure, buffer lifecycle, FFI, channel management) is hidden by the framework.

## Requirements

### Validated

- ✓ C++ Source interface (`fillTupleBuffer`, `open`, `close`) — existing
- ✓ Thread-per-source model via `SourceThread` — existing
- ✓ `RunningSource` with counting semaphore for inflight buffer tracking — existing
- ✓ `BackpressureChannel` (controller/listener) for sink-to-source pressure — existing
- ✓ `SourceHandle` wrapping source lifecycle (start/stop/tryStop) — existing
- ✓ Folly MPMC task queue for work distribution — existing
- ✓ `TupleBuffer` zero-copy reference-counted buffer management — existing
- ✓ `WorkerConfiguration` with `defaultMaxInflightBuffers` — existing
- ✓ Tokio runtime already in codebase for network layer — existing
- ✓ Shared Tokio runtime configured via `WorkerConfiguration` — v1.0
- ✓ `AsyncSource` Rust trait that handles backpressure, semaphore, and channel send internally — v1.0
- ✓ Simple source interface: `async fn run(ctx)` where ctx provides `allocate_buffer()` and `emit(buffer).await` — v1.0
- ✓ Safe Rust wrappers around `TupleBuffer` and `AbstractBufferProvider` — v1.0
- ✓ Per-source async Tokio semaphore for inflight buffer tracking — v1.0
- ✓ Async-aware backpressure via future/promise on C++ `BackpressureListener`, translated to Rust awaitable — v1.0
- ✓ Single bridge thread: reads `async_channel` (blocking recv), pushes to Folly MPMC task queue — v1.0
- ✓ Bridge receives `(buffer, semaphore_ref)` tuples; converts semaphore ref to `OnComplete` callback for release — v1.0
- ✓ `SourceHandle` variant that wraps Tokio sources and plugs into `RunningSource` like existing C++ sources — v1.0
- ✓ Abstraction layer so `RunningSource` can use both C++ `SourceThread` and Rust async sources — v1.0
- ✓ Generator test source using async sleeps to prove the framework works end-to-end — v1.0

### Active

- [ ] AsyncSink trait with open/write/flush/close lifecycle
- [ ] Bounded channel between C++ pipeline thread and Rust async sink task
- [ ] BackpressureController with hysteresis thresholds (NetworkSink pattern)
- [ ] repeatTask-based retry for full channel and flush (non-blocking pipeline)
- [ ] Guaranteed flush on pipeline stop — sink confirms all data written
- [ ] TokioSink C++ operator integrating into existing pipeline infrastructure
- [ ] TokioSinkRegistry mirroring TokioSourceRegistry
- [ ] Shared Tokio runtime for both sources and sinks

### Out of Scope

- Rewriting existing C++ sources in Rust — this adds a parallel path, not a replacement
- Rust-side task queue or scheduler — the C++ Folly MPMC queue remains the execution engine
- Multi-runtime support — all Rust sources and sinks share one Tokio runtime
- Rewriting existing C++ sinks (NetworkSink etc.) in Rust — new sinks only

## Context

Shipped v1.0 with ~3k Rust + ~4.4k C++ LOC across 3 crates (nes-source-bindings, nes-source-runtime, nes-source-lib).
Tech stack: cxx 1.0, tokio 1.40.0, async-channel 2.3.1, DashMap, Corrosion/CMake integration.
Stress tested with 1000 concurrent sources processing 10M tuples on 2 Tokio worker threads.
Architecture evolved during development: EOS through bridge channel (not separate path), async buffer allocation (tryGetBuffer + Notify), inflight semaphore moved from engine to sources with per-buffer OnComplete callback.

## Constraints

- **FFI boundary**: Rust ↔ C++ via `cxx` or raw FFI. TupleBuffer and BufferProvider wrappers must be safe.
- **No Tokio thread blocking**: The bridge thread is the only place blocking calls happen. Rust source code must never call blocking C++ APIs directly.
- **Existing engine unchanged**: `RunningSource`, `QueryEngine`, task queue, `WorkEmitter` remain as-is. The new code plugs in alongside, not replacing.
- **Single bridge thread**: One thread handles all Rust source emissions to keep resource usage minimal.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Single shared Tokio runtime for all sources | Efficient thread reuse for I/O-bound sources; configured centrally via WorkerConfiguration | ✓ Good |
| async_channel for bridge | Provides both sync recv (bridge thread) and async send (Rust sources) on same channel | ✓ Good |
| Per-source Tokio semaphore (not C++ semaphore) | Avoids blocking Tokio threads; semaphore ref sent with buffer so bridge can build OnComplete callback | ✓ Good |
| Future/promise on BackpressureListener | Allows translating C++ backpressure signal to Rust .await without blocking | ✓ Good |
| AsyncSource handles all complexity | Source authors only see allocate_buffer + emit; backpressure, semaphore, channel are internal | ✓ Good |
| Bridge thread is dumb pipe | Only moves (buffer, sem_ref) from async_channel to Folly MPMC; no logic beyond converting sem_ref to OnComplete | ✓ Good |
| EOS through bridge channel | Guarantees ordering — EOS arrives after all data buffers. Bridge thread clears emitFunction to release successor refs | ✓ Good |
| Inflight semaphore in sources (not engine) | Per-buffer OnComplete callback via Arc::into_raw/from_raw. Semaphore released when pipeline finishes, not when bridge dispatches | ✓ Good |
| Async buffer allocation via tryGetBuffer + Notify | Static LazyLock<Notify> signaled by recyclePooledBuffer callback. No spawn_blocking needed | ✓ Good |

---
*Last updated: 2026-03-16 after v1.1 milestone start*
