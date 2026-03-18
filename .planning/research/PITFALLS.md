# Pitfalls Research

**Domain:** Async Rust sink framework for NebulaStream (v1.1 -- adding sinks to existing source framework)
**Researched:** 2026-03-16
**Confidence:** HIGH (based on deep codebase analysis of v1.0 source architecture, v1.0 post-mortem lessons, and understanding of the C++ pipeline execution model)

---

## Critical Pitfalls

### Pitfall 1: Buffer Ownership Inversion -- C++ Pipeline Hands Buffer to Rust, Not the Other Way

**What goes wrong:**
Sources flow Rust-to-C++: Rust allocates a buffer, fills it, sends it through the bridge channel to C++ where `bridge_emit` calls `retain()` before passing to the Folly task queue. The TupleBufferHandle in Rust calls `release()` on drop. This works because Rust owns the buffer first.

Sinks invert the direction: C++ pipeline already owns the buffer (it arrives from an upstream operator). The C++ TokioSink operator must transfer the buffer to Rust for async writing. The buffer's refcount was incremented by the pipeline when it dispatched work to the sink. If the C++ side calls `release()` before Rust is done (e.g., the C++ pipeline node is destroyed during shutdown), Rust holds a dangling pointer. If Rust creates a TupleBufferHandle via `retain()` but the C++ side also holds its copy, a double-retain occurs and the buffer is never recycled.

**Why it happens:**
The v1.0 source bridge was designed for one direction. Developers will copy the `bridge_emit` pattern -- calling `retain()` then passing the raw pointer -- but the ownership semantics are reversed. In `bridge_emit`, Rust drops the handle (calling `release()`) immediately after C++ calls `retain()`, so net refcount is correct. For sinks, the C++ pipeline thread pushes the buffer into a channel; the buffer must stay alive until the Rust async task has finished writing it. The pipeline's `onComplete` callback fires when the pipeline "finishes" the buffer -- but for a sink, "finish" means the buffer was accepted into the channel, not written to the external system.

**How to avoid:**
- The C++ TokioSink operator must `retain()` the buffer when sending it to the Rust channel and rely on the Rust TupleBufferHandle `Drop` to call `release()`. The C++ pipeline's copy is released by the normal pipeline lifecycle (TaskCallback::OnComplete). This gives two independent refcount increments, one for each side.
- The Rust `SinkContext::write()` method must take ownership of the TupleBufferHandle and drop it after `AsyncSink::write()` returns. Do NOT hold the buffer across multiple async operations.
- Write a test that verifies `getReferenceCounter()` is exactly 1 at the moment the Rust async sink receives the buffer (C++ pipeline has already released its copy via onComplete before the Rust side processes).
- IMPORTANT: Consider whether the pipeline's `onComplete` fires before or after the sink channel send. If `onComplete` fires immediately (buffer "consumed" by channel send), the C++ refcount decrements then, and only the Rust-side retain keeps the buffer alive. This is the correct pattern -- mirror what the network sink does.

**Warning signs:**
- ASAN reports use-after-free during sink shutdown.
- Buffer pool exhaustion (buffers never recycled because refcount stuck at 1).
- `getReferenceCounter()` is 2 when it should be 1 in the Rust async task.

**Phase to address:** Phase 1 (sink bridge design). This is the foundational ownership model; every subsequent component depends on getting it right.

---

### Pitfall 2: Backpressure Deadlock -- Bounded Channel Full but Pipeline Thread Cannot Yield

**What goes wrong:**
The C++ pipeline executes operators synchronously on a worker thread from the Folly task queue. When TokioSink tries to push a buffer into the bounded Rust channel and the channel is full (Rust sink is slow), the pipeline thread blocks. But the pipeline thread is also responsible for processing other work items -- including draining buffers that would free the slow sink. In a single-worker configuration, this is a guaranteed deadlock: the worker thread is blocked in channel send, no other thread processes the task queue, the Rust sink never finishes its current write (which might depend on another pipeline stage running).

Even with multiple workers, if all workers are blocked on full sink channels, the system deadlocks.

**Why it happens:**
The source bridge avoids this because the bridge thread is a dedicated thread that only does recv_blocking -- it never processes pipeline work. But sinks are called FROM pipeline workers. Using a blocking channel send from a pipeline worker thread violates the fundamental constraint: pipeline workers must never block indefinitely.

The `emitWork` pattern in RunningSource already handles this: `while (!emitter.emitWork(...))` spins with a stop_token check. The admission queue's `tryWriteUntil` has a 100ms timeout. But a direct blocking channel send has no such timeout.

**How to avoid:**
- NEVER use blocking channel send from the C++ pipeline thread. Use `try_send()` (non-blocking).
- When `try_send()` returns full, use the `repeatTask` mechanism: return from the current pipeline execution and re-enqueue the same task to retry later. This yields the worker thread to process other tasks.
- Implement hysteresis thresholds (like NetworkSink): pause above high watermark, resume below low watermark. This prevents oscillation between "channel full, repeat" and "channel has one slot, send, immediately full again."
- The BackpressureController (C++ side) should signal upstream sources to slow down when the channel is consistently full, not just repeat indefinitely.

**Warning signs:**
- Worker thread CPU goes to 0% while the system has pending work.
- All Folly worker threads show the same backtrace (blocked in channel send).
- System throughput drops to zero intermittently under sustained load.

**Phase to address:** Phase 1 (channel design) and Phase 2 (repeatTask integration). The non-blocking send pattern must be the very first design decision.

---

### Pitfall 3: Flush Ordering -- Stop Signal Races with In-Flight Buffers in Channel

**What goes wrong:**
When a pipeline stops, the TokioSink operator receives a stop signal. But there may be buffers already in the bounded channel waiting to be written by the Rust async task. If TokioSink immediately closes the channel on stop, those buffers are dropped -- data loss. If TokioSink does not close the channel, the Rust task never knows to flush and exit -- hang.

This is the sink equivalent of the v1.0 EOS ordering bug: in v1.0, sending EOS on a separate path from data caused EOS to arrive before all data was processed. The fix was sending EOS through the same channel as data.

**Why it happens:**
Stop signals and data travel on different paths. The stop signal comes from the query engine (via `tryStop`). Data comes from upstream pipeline operators (via the sink's `execute` method). If `tryStop` closes the channel while `execute` is still sending buffers, either:
1. Buffers after the close are dropped (data loss), or
2. `execute` panics/errors on a closed channel (crash).

**How to avoid:**
- Use the same pattern as v1.0 EOS: send a Flush/EOS sentinel message through the same bounded channel after the last data buffer. The Rust async task sees data, data, ..., Flush. On Flush, it calls `AsyncSink::flush()` then `AsyncSink::close()`.
- The C++ TokioSink::stop() must NOT close the channel directly. Instead: (1) stop accepting new buffers, (2) send a Flush sentinel into the channel, (3) wait for the Rust task to acknowledge flush completion.
- Use an atomic flag or stop_token to prevent new `execute()` calls from sending more buffers after stop is initiated.
- The Rust task's main loop: `while let Ok(msg) = receiver.recv().await { match msg { Data(buf) => sink.write(buf).await, Flush => { sink.flush().await; sink.close().await; break; } } }`
- TokioSink::tryStop() must wait for flush acknowledgment (similar to how v1.0 waits for `eosProcessed.store(true)`).

**Warning signs:**
- Data loss on shutdown: fewer records written to sink than produced by source.
- `tryStop()` returning TIMEOUT even though the sink is idle.
- Rust async task exits before all channel buffers are consumed.

**Phase to address:** Phase 2 (flush protocol design). This is the most complex lifecycle interaction and must be designed before implementation.

---

### Pitfall 4: repeatTask Re-entrance -- Pipeline Repeats Task but Sink Is Already Flushing

**What goes wrong:**
When the sink channel is full, the C++ operator uses `repeatTask` to re-enqueue the work item. But if a stop/flush has been initiated between the first attempt and the repeat, the repeated task finds the sink in a flushing state. If the repeated task tries to send a buffer into a channel that is being drained for flush, the behavior depends on channel state: the send might succeed (breaking flush ordering), or the channel might be closed (losing the buffer).

**Why it happens:**
`repeatTask` operates on the assumption that retrying later will succeed because the channel will have space. But during shutdown, the channel might be closed or in a special state. The repeated task does not know about the state transition that happened between its first attempt and its retry.

**How to avoid:**
- The TokioSink operator must maintain a state machine: `Active -> Flushing -> Closed`. The `execute()` method checks the state before attempting to send.
- In `Active` state: try_send, and if full, repeatTask.
- In `Flushing` state: do NOT send more data. Drop the buffer (or return an error). The buffer was already "in flight" from the pipeline's perspective; the stop signal means the query is ending and data loss is acceptable for non-final buffers.
- In `Closed` state: error/no-op.
- Use `std::atomic<State>` for the state machine. The stop() method transitions `Active -> Flushing` atomically. The repeated task checks state BEFORE trying to send.
- The transition to `Flushing` must happen BEFORE the flush sentinel is sent into the channel. Otherwise, a repeated task could send data after the flush sentinel.

**Warning signs:**
- Data arriving at the external sink after the flush marker.
- Rust task receives data after processing the Flush message.
- Non-deterministic test failures where sink output has different record counts on each run.

**Phase to address:** Phase 2 (TokioSink operator state machine). Design the state machine formally before coding.

---

### Pitfall 5: Shared Runtime Starvation -- Slow Sink Starves Sources

**What goes wrong:**
Sources and sinks share a single Tokio runtime (e.g., 2 worker threads). A sink's `AsyncSink::write()` performs a slow network call (e.g., 500ms HTTP POST). If multiple sinks are writing simultaneously, all Tokio worker threads are occupied waiting for I/O completion. Source tasks cannot make progress -- they cannot allocate buffers, emit, or check cancellation tokens. The system appears deadlocked but is actually I/O-starved.

This is different from blocking (which Tokio cannot detect). Async I/O operations yield at `.await` points, so Tokio should interleave. The real risk is when sink write operations are CPU-bound (e.g., serialization before network send) or use `spawn_blocking` that saturates the blocking thread pool.

**Why it happens:**
v1.0 was designed with only sources in mind. Sources are relatively lightweight: allocate buffer, fill with data, emit. Sinks may do heavy work: serialize buffer contents, compress, make network calls with retries. The runtime thread count was tuned for source I/O patterns.

**How to avoid:**
- Size the Tokio runtime based on total concurrent sources AND sinks, not just sources. The `WorkerConfiguration::tokioRuntimeThreads` should account for sink workloads.
- Sink implementations that do CPU-heavy work (serialization, compression) MUST use `tokio::task::spawn_blocking` for the CPU-bound portion. Only the actual async I/O (network write) should run on the Tokio worker threads.
- Add `tokio::task::yield_now().await` in tight loops within sink implementations as a defensive measure.
- Monitor per-task execution time with `tokio-console`. Flag any task that holds a worker thread for > 10ms without yielding.
- Consider using `tokio::task::spawn` for each individual write rather than running the sink loop on a single long-lived task. This allows Tokio's work-stealing scheduler to interleave sink and source work.

**Warning signs:**
- Source throughput drops when sinks are added, even though source and sink I/O are independent.
- `tokio-console` shows sink tasks with high poll times (> 1ms average).
- Adding more sinks causes all sources to slow down proportionally.

**Phase to address:** Phase 1 (runtime configuration) and Phase 3 (performance testing). Validate with a test that runs sources and sinks concurrently and measures source throughput degradation.

---

### Pitfall 6: Channel Closing Semantics -- Who Closes, When, What About In-Flight Buffers

**What goes wrong:**
The bounded channel between C++ and Rust has two ends: the C++ sender (in TokioSink operator) and the Rust receiver (in the async sink task). Several failure modes:

1. **C++ closes sender while Rust is mid-write:** Rust finishes current write, calls `recv().await`, gets `Err` (channel closed). If Rust interprets this as "all done" and closes the external sink, any buffers that were in the channel but not yet received are lost (they are in the channel's internal buffer, dropped when both sides close).
2. **Rust task panics/errors, receiver dropped:** C++ try_send returns error (channel closed). If C++ does not detect this and keeps repeating the task, it spins forever.
3. **Both sides close simultaneously:** Race between flush sentinel and channel close. Flush sentinel might be dropped.

This is subtler than the v1.0 bridge channel because v1.0 used a shared channel for all sources with a single bridge thread controlling the receiver lifetime. Sinks have per-sink channels with independent sender/receiver lifecycles.

**Why it happens:**
`async_channel` semantics: when all senders are dropped, `recv()` returns `Err` after the channel is drained. When the receiver is dropped, `send()` returns `Err` immediately. These are different behaviors and developers often conflate them.

**How to avoid:**
- Use a single sender (not cloned). The C++ TokioSink operator is the sole sender. When TokioSink is destroyed, the sender drops, the channel drains, and the Rust task exits cleanly after processing all remaining buffers.
- The Rust task must drain the channel on `recv() -> Err`: `while let Ok(msg) = receiver.try_recv() { process(msg) }` after the `recv()` loop exits.
- If the Rust task errors, it should signal the C++ side via an error callback (similar to source error_fn) BEFORE dropping the receiver. The C++ side checks this flag before attempting to send.
- Channel closure order: (1) C++ stops accepting new execute() calls, (2) C++ sends Flush sentinel, (3) C++ drops sender, (4) Rust processes remaining messages including Flush, (5) Rust calls sink.close(), (6) Rust drops receiver, (7) C++ confirms via atomic flag or callback.

**Warning signs:**
- Rust task exits but C++ sender still holds a reference to the channel.
- `try_send()` returns `Err` but the TokioSink operator does not handle it (infinite repeatTask loop).
- Buffers in the channel's internal buffer are dropped without being processed.

**Phase to address:** Phase 1 (channel design) and Phase 2 (lifecycle protocol). Specify the exact closing sequence in a design doc before implementation.

---

### Pitfall 7: Error Propagation Gap -- Async Sink Error Cannot Stop the Pipeline

**What goes wrong:**
An `AsyncSink::write()` call fails (e.g., network error, disk full). The Rust async task detects the error and... what? In v1.0 sources, errors are reported via `error_fn` callback, which logs them. But sinks need stronger semantics: a sink error should stop the pipeline (or at least stop the source feeding the sink). If the error is only logged, the pipeline keeps sending buffers to a dead sink, filling the channel until it is full, then repeatTask-ing indefinitely.

The existing C++ pipeline has no mechanism for a downstream operator to signal "stop" back to the source. `BackpressureController` slows sources down but does not stop them. The error must flow through the query lifecycle controller.

**Why it happens:**
The v1.0 error path was designed for sources, which are the origin of data. Source errors naturally stop the data flow. Sink errors are at the END of the data flow -- the data keeps arriving even after the sink fails. The pipeline execution model (fire-and-forget work items in the Folly queue) has no built-in mechanism for downstream-to-upstream error signaling.

**How to avoid:**
- The Rust async sink task must call an error callback (similar to source `error_fn`) when `write()` fails. This callback must:
  1. Log the error (as today).
  2. Set a per-sink error flag that the C++ TokioSink operator checks before each `execute()`.
  3. Initiate pipeline stop via `QueryLifetimeController::initializeSourceFailure()` (or a new sink failure path).
- The C++ TokioSink operator's `execute()` method must check the error flag FIRST, before attempting to send. If error flag is set, discard the buffer and return immediately.
- Consider a dedicated error channel (separate from the data channel) so errors are delivered immediately, not queued behind data.
- The Rust task should stop receiving new buffers after a write error: close the receiver, let the channel drain (buffers are dropped), signal error, then exit.

**Warning signs:**
- Sink write fails but pipeline keeps running, queueing buffers indefinitely.
- `repeatTask` loops forever because the channel is full and will never drain (sink task has exited on error).
- No error visible in logs when the sink destination is unreachable.

**Phase to address:** Phase 2 (error handling) and Phase 3 (integration with QueryLifetimeController).

---

### Pitfall 8: v1.0 Double-Retain Bug Resurfaces in Sink Direction

**What goes wrong:**
v1.0 lesson: `bridge_emit` copies the TupleBuffer via `TupleBuffer buffer(*rawBuffer)` -- the copy constructor calls `retain()`. If the code ALSO called `retain()` explicitly before the copy, the refcount is incremented twice, and the buffer is never recycled (leak). In the sink direction, the same pattern applies but in reverse: the C++ pipeline already holds a retained buffer. If the sink bridge code calls `retain()` and THEN creates a TupleBufferHandle in Rust (whose constructor or the CXX bridge's UniquePtr wrapper might also call `retain()`), the double-retain happens again.

**Why it happens:**
Developers are aware that buffers need `retain()` when crossing the FFI boundary, but forget that some paths already increment the refcount. The CXX bridge's `make_unique<TupleBuffer>(buf)` calls the copy constructor (which calls `retain()`). If you also call `retain()` explicitly before passing the pointer to Rust, you get a double-retain.

**How to avoid:**
- Document the EXACT refcount at every point in the sink buffer transfer. Write it as a numbered sequence:
  1. Pipeline calls TokioSink::execute(buffer) -- refcount = N (pipeline's copy)
  2. TokioSink creates a copy for the channel: `TupleBuffer channelCopy(buffer)` -- refcount = N+1
  3. Channel copy is sent to Rust as raw pointer -- NO additional retain needed
  4. Rust creates TupleBufferHandle from raw pointer (NO copy constructor, direct pointer wrap)
  5. Pipeline's copy drops at end of execute() -- refcount = N
  6. Rust drops TupleBufferHandle after write -- calls `release()` -- refcount = N-1
- Use the `retain()`/`release()` assertion test from v1.0: verify refcount at each transition point.
- Consider passing the buffer to Rust as a move-only type (UniquePtr) so the copy/retain happens exactly once, at a single point in the bridge code.

**Warning signs:**
- Buffer pool exhaustion after running for a while (buffers leaked, never recycled).
- `getReferenceCounter()` returns values > expected at any checkpoint.
- Memory usage grows linearly with time under sustained throughput.

**Phase to address:** Phase 1 (sink bridge buffer transfer). Write the refcount sequence diagram BEFORE writing the code.

---

### Pitfall 9: tryStop Polling Semantics -- TokioSink Must Confirm Flush Before Returning SUCCESS

**What goes wrong:**
In v1.0, `TokioSource::tryStop()` checks `eosProcessed.load()` to know when the bridge thread has fully processed the EOS. If it returns TIMEOUT, `RunningSource::~RunningSource` logs "stop will happen asynchronously." But for sinks, the semantics are more important: `tryStop(SUCCESS)` means "all data has been flushed to the external system." If tryStop returns SUCCESS prematurely (before flush completes), the query lifecycle controller proceeds to tear down the pipeline, destroying shared resources that the still-flushing sink task depends on.

Specifically: the `EmitFunction` captures `shared_ptr<RunningQueryPlanNode>` (see `RunningSource::create`). For sinks, if there is an analogous pattern where the TokioSink holds references to pipeline nodes, those references must be released before the pipeline teardown. In v1.0, clearing `ctx->emitFunction = {}` on EOS was critical -- without it, the captured shared_ptrs prevented node destruction and hung the query lifecycle.

**Why it happens:**
`tryStop(timeout=0)` is called in a polling loop by the query engine. Returning SUCCESS before flush completes is tempting because it "unblocks" the shutdown path. But it creates a race: the sink is still writing while the pipeline is being torn down.

**How to avoid:**
- TokioSink::tryStop() must check a `flushCompleted` atomic flag, set by the Rust async task AFTER `sink.flush()` AND `sink.close()` have returned.
- The Rust task sets the flag via a C-linkage callback (similar to how `eosProcessed` works in v1.0).
- If the flag is not set, return TIMEOUT. The query engine will retry.
- The C++ TokioSink must release any captured references (like EmitFunction equivalents) AFTER flush is confirmed, not when stop() is called.
- Test: start a slow-flushing sink, call tryStop(0), verify it returns TIMEOUT. Then let the flush complete, call tryStop(0) again, verify SUCCESS.

**Warning signs:**
- Query teardown hangs because pipeline node shared_ptrs are still held by a destroyed sink.
- Sink data partially written (flush did not complete before resources were torn down).
- `tryStop()` always returns SUCCESS immediately, even for sinks with slow flush.

**Phase to address:** Phase 2 (tryStop integration). Mirror the v1.0 `eosProcessed` pattern exactly.

---

### Pitfall 10: Per-Sink Channel vs Shared Channel Design Decision

**What goes wrong:**
v1.0 uses a single shared bridge channel for all sources, with origin_id-based dispatch. If sinks also use a shared channel, sink backpressure on one sink backs up the channel for ALL sinks (head-of-line blocking). A slow S3 sink prevents a fast local-file sink from receiving buffers. If sinks use per-sink channels, the architecture is cleaner but requires per-sink bridge threads or Tokio receiver tasks.

**Why it happens:**
The shared-channel pattern worked for sources because sources push INTO the channel (backpressure is per-source via semaphore). Sinks pull FROM the channel, and the push comes from the pipeline. With a shared channel, the pipeline would need to tag each buffer with its destination sink -- but the pipeline does not know about sinks at the execution layer (operators are generic).

**How to avoid:**
- Use per-sink channels. Each TokioSink operator owns a `Sender<SinkMessage>`. Each Rust async sink task owns the corresponding `Receiver<SinkMessage>`. No shared channel needed.
- This eliminates head-of-line blocking and simplifies the lifecycle (channel closes when the TokioSink operator is destroyed).
- Do NOT reuse the source bridge channel or bridge thread for sinks. The bridge thread is designed for Rust-to-C++ direction; sinks are C++-to-Rust direction. Mixing the two makes the bridge thread bidirectional and significantly complicates shutdown ordering.
- The Rust async sink task runs directly on the shared Tokio runtime (no bridge thread needed). It simply does `while let Ok(msg) = receiver.recv().await { ... }`.

**Warning signs:**
- Design doc proposes a shared channel for sinks -- this is the wrong pattern.
- Bridge thread modifications for bidirectional operation -- adds unnecessary complexity.
- Head-of-line blocking in testing where one slow sink affects all sinks.

**Phase to address:** Phase 1 (architecture decision). This is a design-time choice that affects every subsequent component.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Blocking channel send from pipeline worker thread | Simpler than repeatTask | Deadlock under load; worker thread starvation | Never |
| Single state (no state machine) in TokioSink operator | Less code | Race conditions between stop, flush, and repeated tasks | Never -- state machine is essential |
| Ignoring sink write errors | Sink "always works" in testing | Pipeline runs forever feeding a dead sink; data loss undetected | Never |
| Reusing source bridge channel for sinks | Less new code | Bidirectional complexity; head-of-line blocking; shutdown ordering nightmare | Never |
| Skipping flush on shutdown | Faster shutdown | Data loss for last batch of buffers | Only for "at-most-once" delivery guarantees (acceptable for metrics, never for logs/events) |
| Polling tryStop in a tight loop | Simple query engine integration | CPU waste; delays other query lifecycle operations | Acceptable with a 10ms+ sleep between polls |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Pipeline worker thread -> sink channel | Blocking send | Non-blocking `try_send()` + repeatTask on full |
| Sink error -> pipeline stop | Only log the error | Error callback sets flag; TokioSink stops accepting; notify QueryLifetimeController |
| tryStop -> flush confirmation | Return SUCCESS immediately | Poll `flushCompleted` atomic flag; return TIMEOUT until confirmed |
| Buffer transfer C++ -> Rust | Double retain (explicit + copy constructor) | Copy constructor only; pass raw pointer to Rust for wrapping; document refcount at each step |
| Shared Tokio runtime | Same thread count as source-only config | Size for sources + sinks; monitor with tokio-console |
| repeatTask + state machine | Retry without checking sink state | Check state BEFORE retry; drop buffer if Flushing/Closed |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| repeatTask spinning on full channel | High CPU with low throughput; worker threads busy but no data flowing | BackpressureController hysteresis; signal sources to slow down | Channel consistently full (sink slower than source) |
| Per-buffer Tokio task spawn in sink | Thread pool overhead dominates for small buffers | Single long-lived task per sink with recv loop | > 1000 buffers/sec per sink |
| Serialization on Tokio worker thread | Worker thread blocked during JSON/protobuf serialization; source starvation | `spawn_blocking` for CPU-heavy serialization | Buffers > 1KB or serialization > 100us |
| Channel bound too small | Bursty source overwhelms sink; constant repeatTask churn | Bound = 2x expected in-flight; hysteresis at 75%/25% | Bursty workloads with > 2x peak/average ratio |
| Channel bound too large | High memory usage; delayed backpressure signal to sources | Bound = inflight limit; match existing source semantics | Sink write latency > 100ms with large buffers |

---

## "Looks Done But Isn't" Checklist

- [ ] **Flush ordering:** Send Flush sentinel through data channel, not a separate signal -- verify by test: stop pipeline, check all buffered data written before close
- [ ] **repeatTask state check:** Every repeatTask callback checks sink state before retry -- verify by grep for `try_send` without adjacent state check
- [ ] **Error propagation:** Sink write error stops the pipeline, not just logs -- verify by test: inject write error, check pipeline stops within timeout
- [ ] **Buffer refcount:** Document refcount at each transfer point; verify with `getReferenceCounter()` assertions at channel send and Rust receive
- [ ] **tryStop flush confirmation:** `tryStop` returns SUCCESS only after flush completes -- verify by test: slow flush, tryStop returns TIMEOUT until done
- [ ] **No blocking in pipeline thread:** Zero blocking calls from TokioSink::execute() -- verify by grep for `send_blocking`, `recv_blocking`, `wait()` in sink operator code
- [ ] **Channel drain on close:** Rust task processes all buffered messages before exiting -- verify by test: fill channel, close sender, check all messages processed
- [ ] **Runtime thread sizing:** WorkerConfiguration accounts for sinks -- verify config includes sink count in thread calculation

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Deadlock from blocking channel send | HIGH | Rewrite to non-blocking + repeatTask; requires understanding pipeline execution model |
| Data loss from premature channel close | MEDIUM | Add flush sentinel; requires lifecycle protocol redesign but no API changes |
| Double-retain buffer leak | MEDIUM | Audit refcount sequence; fix the single retain/copy point; add assertion tests |
| Sink error not stopping pipeline | MEDIUM | Add error callback and flag; modify execute() to check flag; wire to QueryLifetimeController |
| Runtime starvation | LOW | Increase thread count in WorkerConfiguration; add tokio-console monitoring |
| repeatTask re-entrance during flush | MEDIUM | Add state machine; modify execute() to check state; straightforward but requires testing all transitions |
| tryStop returning SUCCESS too early | LOW | Add flushCompleted atomic flag; copy v1.0 eosProcessed pattern |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Buffer ownership inversion (P1) | Phase 1: Sink bridge design | Refcount assertion tests at each transfer point |
| Backpressure deadlock (P2) | Phase 1: Channel design + Phase 2: repeatTask | Test: fill channel, verify worker thread not blocked |
| Flush ordering (P3) | Phase 2: Flush protocol | Test: stop pipeline with in-flight buffers, verify all written |
| repeatTask re-entrance (P4) | Phase 2: TokioSink state machine | Test: stop during repeatTask retry, verify no data after flush |
| Shared runtime starvation (P5) | Phase 1: Runtime config + Phase 3: perf test | Concurrent source+sink test; measure source throughput degradation < 10% |
| Channel closing semantics (P6) | Phase 1: Channel design + Phase 2: lifecycle | Test: all closing sequences; verify no message loss, no hang |
| Error propagation gap (P7) | Phase 2: Error handling + Phase 3: integration | Test: inject sink error; verify pipeline stops |
| Double-retain (P8) | Phase 1: Sink bridge buffer transfer | Refcount = expected at each checkpoint |
| tryStop semantics (P9) | Phase 2: tryStop integration | Test: slow flush; tryStop returns TIMEOUT then SUCCESS |
| Per-sink vs shared channel (P10) | Phase 1: Architecture decision | Design review; no shared channel for sinks |

---

## Sources

- Codebase inspection: v1.0 source bridge architecture (`bridge.rs`, `source.rs`, `context.rs`, `buffer.rs`, `TokioSource.cpp`, `SourceBindings.cpp`)
- Codebase inspection: Pipeline execution model (`RunningSource.cpp`, `TaskQueue.hpp`, `SourceReturnType.hpp`)
- Codebase inspection: Buffer lifecycle (`TupleBuffer.hpp`, `BufferManager.cpp`, `AbstractBufferProvider.hpp`)
- v1.0 lessons learned (from milestone context): double-retain bug, semaphore timing, EOS race, CXX bridge limitations
- [Tokio -- Async: What is blocking? (Alice Ryhl)](https://ryhl.io/blog/async-what-is-blocking/)
- [Tokio -- Graceful Shutdown](https://tokio.rs/tokio/topics/shutdown)
- [async_channel documentation -- closing semantics](https://docs.rs/async-channel/latest/async_channel/)
- NetworkSink pattern in NebulaStream (hysteresis backpressure model)

---
*Pitfalls research for: Async Rust sink framework for NebulaStream v1.1*
*Researched: 2026-03-16*
