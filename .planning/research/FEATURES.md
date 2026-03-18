# Feature Landscape

**Domain:** Async Rust sink framework for NebulaStream (v1.1 milestone)
**Researched:** 2026-03-16
**Confidence:** HIGH (based on direct codebase analysis of v1.0 source framework, NetworkSink, and C++ sink infrastructure)

## Context: What Already Exists (v1.0)

The following are shipped and do NOT need to be rebuilt:
- AsyncSource trait with SourceContext (allocate_buffer + emit)
- Bridge thread for async-to-sync buffer delivery (source direction)
- TokioSource C++ class integrating via SourceHandle variant
- TokioSourceRegistry for string-name source registration
- BackpressureFuture for source-side backpressure translation
- Per-source inflight semaphore with OnComplete callback
- GeneratorSource reference implementation
- Shared Tokio runtime configured via WorkerConfiguration

This research covers ONLY the new sink features.

## Table Stakes

Features sink authors expect. Missing any of these = the framework is broken or unusable for real async sinks.

| Feature | Why Expected | Complexity | Dependencies | Notes |
|---------|--------------|------------|--------------|-------|
| AsyncSink trait (open/write/flush/close) | The primary author-facing API. Sink authors implement these four methods. Everything else is hidden by the framework. | Med | None (new Rust trait) | `write(&self, data: &[u8], num_tuples: u64)` receives read-only buffer data. `flush()` is called on stop to guarantee delivery. `open()` and `close()` handle connection lifecycle. All methods are async. |
| Bounded channel (C++ pipeline -> Rust task) | Core data path. C++ `execute()` cannot block the pipeline thread with async I/O. Channel decouples sync pipeline from async Rust task. | Med | Shared Tokio runtime (v1.0) | Use `tokio::sync::mpsc` -- sink has exactly one producer (pipeline thread via `try_send`) and one consumer (Rust task via `recv().await`). NOT `async_channel` -- no need for sync recv on consumer side. |
| BackpressureController with hysteresis | Prevents unbounded buffering when sink is slow. Must use existing `BackpressureController` (Sink base class already owns one). Hysteresis prevents oscillation. | Med | Existing BackpressureChannel infra, Sink base class | Mirror NetworkSink exactly: upper threshold triggers `applyPressure()`, lower threshold triggers `releasePressure()`. Track in-channel count on C++ side. Configurable via SinkDescriptor. |
| repeatTask-based retry for full channel | When `try_send` returns Full, must not block the pipeline thread. Use `pec.repeatTask(buffer, interval)` to re-enqueue after delay -- identical to NetworkSink pattern. | Low | PipelineExecutionContext::repeatTask | Established NES pattern for non-blocking sink retries. Constraint: repeatTask can only be called once per execute() invocation. Same 10ms retry interval as NetworkSink. |
| Guaranteed flush on stop | Pipeline stop must wait for all in-flight buffers to be written before returning. Without this, data loss on shutdown. | High | Channel + async task coordination | Hardest feature. TokioSink::stop() must: (1) signal Rust task to flush, (2) Rust task drains channel + calls AsyncSink::flush(), (3) Rust task signals completion, (4) C++ polls completion via repeatTask. |
| TokioSink C++ operator (extends Sink) | C++ class implementing ExecutablePipelineStage (start/execute/stop). Plugs into existing pipeline infrastructure. | High | All above features | Implements: `start()` spawns Rust task, `execute()` sends buffer via try_send with backpressure, `stop()` initiates flush and polls completion via repeatTask. |
| TokioSinkRegistry | String-name registration of TokioSink implementations, matching TokioSourceRegistry and SinkRegistry patterns. | Low | TokioSink, BaseRegistry | Direct copy of TokioSourceRegistry pattern. Uses CMake code-generated registrar include files. Arguments: BackpressureController, SinkDescriptor. |
| Error reporting from async task | When async sink encounters an error, it must propagate to C++ for query failure. | Med | ErrorFnPtr pattern from v1.0 | Reuse monitoring task + ErrorFnPtr callback pattern from spawn_source. Monitoring task awaits JoinHandle, calls error callback on failure/panic. |
| SinkContext (Rust-side) | Context object passed to AsyncSink methods. Provides cancellation_token for cooperative shutdown. | Low | CancellationToken | Much simpler than SourceContext -- no semaphore, no backpressure state, no buffer provider, no emit. Just: cancellation_token. Buffer data arrives as method arguments to write(). |
| Buffer data passing across FFI | TupleBuffer content must reach Rust AsyncSink::write() without copying if possible. | Med | TupleBuffer FFI wrappers from v1.0 | retain() on C++ side before channel send. Rust task reads buffer data as &[u8] slice. release() after write() completes. |
| Shared Tokio runtime with sources | Sinks must share the same Tokio runtime as sources. No second runtime. | Low | v1.0 runtime module | Already exists. Sink tasks are spawned on the same runtime as source tasks. |

## Differentiators

Features that add value beyond basic correctness. Not blocking for MVP but significantly useful.

| Feature | Value Proposition | Complexity | Dependencies | Notes |
|---------|-------------------|------------|--------------|-------|
| Configurable channel capacity per sink | Different sinks have different buffering needs (high-throughput network vs. slow database). | Low | SinkDescriptor config | Default to 64. Config parameter like NetworkSink's sender_queue_size. |
| Buffer zero-copy with lifetime safety | Pass TupleBuffer data to Rust as &[u8] without memcpy. Rust wrapper ensures buffer is not accessed after release. | Med | Existing TupleBufferHandle | v1.0 already has TupleBufferHandle. For sinks, buffer is read-only -- simpler than sources (no setNumberOfTuples). |
| Batch write optimization | Drain multiple available buffers from channel and pass as batch to optional `write_batch()` method. | Med | Channel receiver, trait extension | Optional trait method with default impl that calls write() in loop. Useful for database sinks that benefit from batch inserts. |
| Structured tracing in sink task | Sink task emits structured spans (buffer received, write latency, flush duration). | Low | tracing crate (already in deps) | `#[instrument]` on the recv loop. Low effort, high debugging value. |
| Write timeout | Framework-level timeout on individual write() calls to detect hung sinks. | Low | tokio::time::timeout | Optional config. If write() exceeds timeout, treat as error and fail the query. Prevents silent hangs. |

## Anti-Features

Features to explicitly NOT build. These represent scope creep or architectural mistakes.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Sink-side buffer allocation | Sinks receive buffers from pipeline, they do not produce new ones. Adding allocation to SinkContext creates confusion. | SinkContext provides only cancellation_token. Buffer data arrives as write() arguments. |
| Separate bridge thread for sinks | Sources use a shared bridge thread because many sources multiplex onto one thread with dispatch by origin_id. Sinks have opposite data flow (C++ -> Rust) and each has its own channel -- no shared dispatch needed. | Direct tokio::sync::mpsc channel per sink instance. No bridge thread. |
| Rust-side inflight semaphore | Sources need semaphores because they push buffers and must limit in-flight count. Sinks receive buffers -- the bounded channel capacity IS the flow control. | Bounded channel + BackpressureController hysteresis on C++ side (exactly like NetworkSink). |
| Automatic retry of failed writes | If write() returns error, framework should NOT silently retry. Retry semantics are sink-specific (idempotent vs. non-idempotent). | Propagate error via monitoring task -> error callback -> query failure. Sink authors implement retry inside write() if appropriate. |
| Multi-consumer sink (fan-out) | One sink consuming from multiple pipeline threads. Existing model is one pipeline -> one sink. | Keep 1:1 pipeline-to-sink mapping. Use multiple sink instances for parallelism. |
| Rewriting existing C++ sinks | Out of scope per PROJECT.md. NetworkSink, FileSink, PrintSink stay as C++. | New async sinks only via TokioSink framework. |
| Partial write recovery | Framework-level tracking of which buffers were written and which failed, with resume capability. | Too complex for the framework layer. Individual sink implementations handle their own write-ahead logging or checkpointing if needed. |
| Concurrent writes within one sink | Spawning multiple write() calls concurrently from a single sink's recv loop. | Sequential writes preserve ordering. Future optimization: optional write_concurrent() with configurable parallelism. Explicitly deferred. |
| Sink-to-source backpressure in Rust | The existing C++ BackpressureChannel already handles this. Rust v1.0 BackpressureState translates it for sources. | TokioSink uses C++ BackpressureController (inherited from Sink base class). Same mechanism as NetworkSink. No new Rust-side backpressure. |

## Feature Dependencies

```
Shared Tokio runtime (v1.0, EXISTS)
    |
    v
SinkContext (NEW, simple) ----> AsyncSink trait (NEW)
                                     |
                                     v
Bounded mpsc channel (NEW) ---> spawn_sink() lifecycle (NEW)
    |                                |
    v                                v
TokioSink C++ operator (NEW) <--+-- Error reporting (REUSE v1.0 pattern)
    |
    +---> BackpressureController hysteresis (NEW, C++ side)
    |
    +---> repeatTask retry for full channel (NEW, C++ side)
    |
    +---> Guaranteed flush on stop (NEW, C++ + Rust coordination)
    |
    v
TokioSinkRegistry (NEW, mirrors TokioSourceRegistry)
```

Key ordering constraints:
1. AsyncSink trait + SinkContext must exist before spawn_sink can be written
2. Bounded channel must exist before TokioSink::execute can send buffers
3. BackpressureController hysteresis and repeatTask retry are in TokioSink::execute
4. Flush-on-stop requires channel close + Rust task drain + repeatTask polling
5. TokioSinkRegistry is independent once TokioSink exists

## MVP Recommendation

Build in this order (each step is testable):

1. **AsyncSink trait + SinkContext** -- Define the author-facing API. Four async methods: open(), write(data, num_tuples), flush(), close(). SinkContext provides cancellation_token only.

2. **spawn_sink() + Rust recv loop** -- Spawn a Tokio task that opens the sink, receives buffers from mpsc channel, calls write() for each, handles errors. Monitoring task for error reporting (reuse v1.0 pattern).

3. **TokioSink C++ operator: start + execute** -- start() spawns Rust task via FFI. execute() calls try_send on mpsc channel. On Full, use repeatTask (copy NetworkSink). Track in-channel count for backpressure hysteresis.

4. **Guaranteed flush on stop** -- TokioSink::stop() drops channel sender (signals no more buffers). Rust task drains remaining, calls flush(), signals completion via shared atomic flag. C++ polls via repeatTask.

5. **TokioSinkRegistry** -- Copy TokioSourceRegistry pattern. Wire into SinkProvider / query compiler.

6. **Reference implementation (PrintTokioSink or NullTokioSink)** -- Simple sink proving end-to-end correctness, analogous to GeneratorSource.

Defer:
- **Batch write optimization**: Add when a real sink needs it.
- **Configurable channel capacity**: Hardcode sensible default, make configurable later.
- **Write timeout**: Add when debugging hung sinks becomes a problem.
- **Structured tracing**: Add after core framework is stable.

## Key Design Decisions

### Why tokio::sync::mpsc, not async_channel

Sources use `async_channel` because the bridge thread needs `recv_blocking()` (sync consumer) while source tasks need `send().await` (async producer). Sinks have the opposite constraint: the C++ pipeline thread needs `try_send()` (sync producer, must not block) while the Rust task needs `recv().await` (async consumer). `tokio::sync::mpsc` provides exactly this: `try_send()` for the sync side, `recv().await` for the async side. No `async_channel` needed.

### Why no bridge thread for sinks

The source-side bridge thread exists because many sources multiplex onto one thread -- the bridge dispatches by origin_id via EMIT_REGISTRY. Sinks have the opposite architecture: each sink instance has its own channel with a single producer (pipeline) and single consumer (Rust task). There is nothing to multiplex or dispatch. The mpsc channel connects them directly.

### Flush coordination mechanism

Use `Arc<AtomicBool>` for flush completion signaling:
- TokioSink::stop() drops the mpsc Sender (closes channel), then enters repeatTask poll loop.
- Rust recv loop detects channel close (recv returns None), drains remaining buffers, calls flush(), sets AtomicBool to true.
- TokioSink::stop() checks AtomicBool on each repeatTask invocation. When true, returns (stop complete).

This is simpler than a oneshot channel and avoids additional FFI complexity. The AtomicBool is allocated as `Arc<AtomicBool>` shared between C++ (via raw pointer) and Rust task.

### Buffer ownership model

In execute(), the TupleBuffer must be retained (refcount incremented) before sending through the channel, because the C++ pipeline may drop its reference after execute() returns. The Rust task releases the buffer after write() completes. This mirrors the source-side pattern where TupleBufferHandle manages C++ refcounts.

The buffer data is passed to write() as `&[u8]` (read-only slice) -- sink authors never see raw pointers or TupleBuffer internals.

### Error handling model

1. AsyncSink::write() returns `Result<(), SinkError>`.
2. On Err, the Rust recv loop exits.
3. Monitoring task detects task completion, calls error_fn callback.
4. C++ side fails the query.
5. Remaining buffers in channel are dropped (not written).

No automatic retry. No partial failure recovery. Sink authors implement their own retry logic inside write() if their destination supports idempotent writes.

### Ordering guarantees

Buffers are delivered to write() in FIFO order (mpsc channel preserves order). The single-consumer Rust task calls write() sequentially. Strong ordering with no additional mechanism needed.

## Sources

- Direct codebase: NetworkSink.hpp/cpp -- backpressure hysteresis with upper/lower thresholds, repeatTask retry pattern, flush on stop
- Direct codebase: Sink.hpp -- base class owning BackpressureController
- Direct codebase: PipelineExecutionContext.hpp -- repeatTask(buffer, interval) API
- Direct codebase: source.rs, context.rs -- v1.0 AsyncSource/SourceContext patterns to adapt for sinks
- Direct codebase: bridge.rs -- v1.0 bridge architecture (explains why sinks do NOT need a bridge)
- Direct codebase: TokioSource.hpp -- SpawnFn + lifecycle pattern to mirror
- Direct codebase: TokioSourceRegistry.hpp -- registry pattern to copy
- Direct codebase: SinkRegistry.hpp -- existing sink registry infrastructure
- Direct codebase: BackpressureChannel.hpp -- BackpressureController/Listener architecture
- PROJECT.md -- requirements, constraints, out-of-scope items

---
*Feature research for: Async Rust sink framework for NebulaStream (v1.1 milestone)*
*Researched: 2026-03-16*
