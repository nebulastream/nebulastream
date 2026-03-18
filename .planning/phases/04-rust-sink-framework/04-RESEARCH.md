# Phase 4: Rust Sink Framework - Research

**Researched:** 2026-03-16
**Domain:** Async Rust sink trait, channel-based message passing, Tokio task lifecycle
**Confidence:** HIGH

## Summary

Phase 4 implements a pure Rust sink framework that mirrors the existing source framework (`AsyncSource`, `SourceContext`, `spawn_source`/`stop_source`). The sink framework introduces `AsyncSink` trait with a single `run()` method, `SinkContext` providing `recv()` that returns `SinkMessage` enum variants (`Data`/`Close`), and `spawn_sink`/`stop_sink` lifecycle functions. All code lives in the existing `nes_source_runtime` crate as new modules alongside the source modules.

The existing source framework provides a near-complete template. The key architectural difference is directionality: sources produce data outward (emit to bridge channel), while sinks consume data inward (receive from a per-sink bounded channel). The sink has no bridge thread, no emit registry, no backpressure state, and no semaphore gating -- it is fundamentally simpler than the source path.

**Primary recommendation:** Mirror the source framework's module structure, `cfg(test)` substitution patterns, error callback mechanism, and monitoring task pattern. Create four new modules: `sink.rs` (trait + spawn/stop), `sink_context.rs` (SinkContext + recv), `sink_error.rs` (SinkError + SinkResult), `sink_handle.rs` (SinkTaskHandle). Reuse existing `async_channel`, `CancellationToken`, and `TupleBufferHandle` types.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Single `run(&mut self, ctx: &SinkContext)` method -- mirrors AsyncSource::run(ctx) pattern
- No separate open/write/flush/close methods -- sink author controls their own loop in run()
- Trait is `Send + 'static` for spawning on Tokio runtime
- Context is borrowed (`&SinkContext`), not owned -- framework retains ownership for lifecycle
- `run()` returns `Result<(), SinkError>` -- errors are fatal to the query
- `SinkContext::recv().await` returns `SinkMessage` enum: `SinkMessage::Data(TupleBufferHandle)` / `SinkMessage::Close`
- Per-sink bounded `async_channel` (not shared) -- no head-of-line blocking between sinks
- async_channel reused from v1.0 (already in Cargo.toml)
- C++ side uses `try_send()` (non-blocking) -- if full, apply backpressure + repeatTask retry (NetworkSink pattern)
- C++ sends `SinkMessage::Close` through the data channel (ordering guaranteed: all data before close)
- Sink processes Close in run() -- flushes whatever it needs, then returns
- Framework drops the Receiver after run() returns, closing the channel
- C++ detects completion via `try_send` returning `Closed` -- no extra AtomicBool needed
- `tryStop()` polls by attempting `try_send` -- `Closed` means done, otherwise `TIMEOUT`
- `run()` returns `Result<(), SinkError>` -- on Err, framework calls on-error callback (passed from C++ at spawn time)
- On error: framework closes channel (drops receiver), C++ detects `Closed` on try_send
- Framework wraps run() in catch_unwind -- panics treated as errors (same as source FFI-03 pattern)
- Sink modules added to existing `nes-source-runtime` crate (not separate crate)

### Claude's Discretion
- SinkMessage enum naming and exact fields
- Internal channel capacity default (will be configurable via C++ side in Phase 5)
- Test mock sink implementation details
- SinkError type definition

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| SNK-01 | AsyncSink Rust trait with open/write/flush/close lifecycle methods | Decision overrides requirement text: single `run()` method instead of four methods. Mirrors `AsyncSource` trait pattern in `source.rs`. |
| SNK-02 | SinkContext providing CancellationToken for cooperative shutdown | Mirrors `SourceContext` pattern in `context.rs`. SinkContext holds `Receiver<SinkMessage>` + `CancellationToken`. `cfg(test)` substitution avoids TupleBufferHandle linker deps. |
| SNK-03 | Per-sink bounded async channel from C++ pipeline thread to Rust async task | Uses `async_channel::bounded` (already in Cargo.toml). Per-sink channel created in `spawn_sink`. `Sender` returned to C++ via handle, `Receiver` owned by SinkContext. |
| SNK-04 | spawn_sink/stop_sink lifecycle management on shared Tokio runtime | Mirrors `spawn_source`/`stop_source` pattern. Spawns sink task + monitoring task. Error callback mechanism identical to source. `SinkTaskHandle` holds `Sender` for sending Close + detecting completion. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.40.0 | Async runtime (shared with sources) | Already in Cargo.toml, project standard |
| tokio-util | 0.7.13 | CancellationToken for cooperative shutdown | Already in Cargo.toml, project standard |
| async-channel | 2.3.1 | Bounded MPSC channel for sink message delivery | Already in Cargo.toml, used by source framework |
| tracing | 0.1 | Structured logging | Already in Cargo.toml, project standard |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| cxx | 1.0 | TupleBufferHandle FFI types (existing) | Referenced by SinkMessage::Data variant |

### Alternatives Considered
None -- all libraries are already in use by the source framework. No new dependencies needed.

**Installation:**
No new dependencies required. All libraries already in `nes_source_runtime/Cargo.toml`.

## Architecture Patterns

### Recommended Module Structure
```
nes-sources/rust/nes-source-runtime/src/
├── lib.rs              # Add pub mod + pub use for sink modules
├── sink.rs             # AsyncSink trait, spawn_sink, monitoring_task, tests
├── sink_context.rs     # SinkContext, recv(), cfg(test) channel substitution
├── sink_error.rs       # SinkError, SinkResult (or reuse Result<(), SinkError>)
├── sink_handle.rs      # SinkTaskHandle, stop_sink
├── source.rs           # (existing -- pattern template)
├── context.rs          # (existing -- pattern template)
├── error.rs            # (existing -- pattern template)
├── handle.rs           # (existing -- pattern template)
└── ...                 # (other existing modules unchanged)
```

### Pattern 1: AsyncSink Trait (mirrors AsyncSource)
**What:** Single `run()` method trait with borrowed context
**When to use:** All sink implementations
**Example:**
```rust
// Mirrors source.rs AsyncSource pattern
pub trait AsyncSink: Send + 'static {
    fn run(
        &mut self,
        ctx: &SinkContext,
    ) -> impl std::future::Future<Output = Result<(), SinkError>> + Send;
}
```

### Pattern 2: SinkMessage Enum (mirrors BridgeMessage)
**What:** Typed enum for channel messages instead of Option
**When to use:** All data delivery through the per-sink channel
**Example:**
```rust
// Mirrors bridge.rs BridgeMessage pattern
pub enum SinkMessage {
    Data(TupleBufferHandle),
    Close,
}
```

### Pattern 3: SinkContext with cfg(test) Substitution
**What:** Context holds Receiver with different types in test vs production
**When to use:** Avoids C++ linker deps in pure Rust tests
**Example:**
```rust
// Mirrors context.rs SourceContext cfg(test) pattern
pub struct SinkContext {
    #[cfg(not(test))]
    receiver: async_channel::Receiver<SinkMessage>,
    #[cfg(test)]
    receiver: async_channel::Receiver<TestSinkMessage>,
    cancellation_token: CancellationToken,
}
```

**Key insight for cfg(test):** In test builds, `SinkMessage::Data` would contain `TupleBufferHandle` which is the test variant (Vec<u8> backed, no C++ symbols). So unlike the source side where BridgeMessage contains semaphore_ptr, the sink's `SinkMessage` may actually work without cfg(test) substitution since the test `TupleBufferHandle` doesn't need C++ linker symbols. Verify during implementation -- if `SinkMessage` compiles cleanly in test builds, skip the cfg(test) split for simplicity.

### Pattern 4: spawn_sink with Monitoring Task
**What:** Spawns sink task + monitoring task, returns opaque handle
**When to use:** All sink lifecycle management
**Example:**
```rust
// Mirrors source.rs spawn_source pattern
pub fn spawn_sink(
    sink_id: u64,
    mut sink: impl AsyncSink,
    channel_capacity: usize,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Box<SinkTaskHandle> {
    let cancel_token = CancellationToken::new();
    let (sender, receiver) = async_channel::bounded(channel_capacity);

    let ctx = SinkContext::new(receiver, cancel_token.clone());

    let runtime = crate::runtime::source_runtime();
    let join_handle = runtime.spawn(async move {
        // catch_unwind wrapping
        let result = std::panic::AssertUnwindSafe(sink.run(&ctx))
            .catch_unwind()
            .await;
        // ... handle result
    });

    // Spawn monitoring task (same pattern as source)
    runtime.spawn(async move {
        monitoring_task(join_handle, sink_id, err_cb).await;
    });

    Box::new(SinkTaskHandle::new(sender, cancel_token, sink_id))
}
```

### Pattern 5: SinkTaskHandle with Sender for Stop Detection
**What:** Handle holds the channel Sender so C++ can send Close and detect completion
**When to use:** Stop/shutdown protocol
**Example:**
```rust
pub struct SinkTaskHandle {
    sender: async_channel::Sender<SinkMessage>,
    cancellation_token: CancellationToken,
    sink_id: u64,
}

// Send Close message through the channel
pub fn stop_sink(handle: &SinkTaskHandle) {
    // try_send Close -- if channel full, caller retries (Phase 5 repeatTask)
    let _ = handle.sender.try_send(SinkMessage::Close);
}

// Check if sink has completed (for Phase 5 tryStop polling)
pub fn is_sink_done(handle: &SinkTaskHandle) -> bool {
    handle.sender.is_closed()
}
```

### Pattern 6: catch_unwind at Spawn Boundary
**What:** Wrap the async sink run() in catch_unwind to convert panics to errors
**When to use:** spawn_sink task
**Example:**
```rust
// Source framework uses this pattern for FFI safety
use std::panic::AssertUnwindSafe;
use futures::FutureExt; // for .catch_unwind() on futures

let join_handle = runtime.spawn(async move {
    let result = AssertUnwindSafe(sink.run(&ctx))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(())) => SinkResult::Complete,
        Ok(Err(e)) => SinkResult::Error(e),
        Err(panic) => SinkResult::Error(SinkError::from_panic(panic)),
    }
});
```

**Note:** `std::panic::catch_unwind` works on synchronous code. For async, use `futures::FutureExt::catch_unwind` or wrap the entire async block. The source framework uses `JoinHandle` which already captures panics -- `join_handle.await` returns `Err(JoinError)` on panic. So catch_unwind may be redundant if we rely on `JoinHandle`'s built-in panic capture (which the source monitoring task already does). **Recommendation:** Use the same pattern as sources -- rely on `JoinHandle::await` returning `Err(JoinError)` for panics, handle in monitoring task. No explicit catch_unwind needed.

### Anti-Patterns to Avoid
- **Shared channel for multiple sinks:** Creates head-of-line blocking. Each sink gets its own bounded channel.
- **Option-based recv instead of enum:** `SinkMessage::Close` is explicit -- don't use `None` to signal shutdown.
- **Unbounded channel:** Hides backpressure failures, allows OOM.
- **Separate error types from source error types when not needed:** SinkError should follow the same structure as SourceError but be a distinct type (different Display prefix, different semantic).
- **Dropping the Sender to signal close:** Loses ordering guarantee. Close must be sent through the data channel so all data is processed first.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cancellation signaling | Custom AtomicBool flag | `CancellationToken` from tokio-util | Race-free, integrates with `select!`, already used by sources |
| Channel completion detection | Custom completion flag | `try_send` returning `Closed` error | async_channel already provides this signal when receiver is dropped |
| Panic safety at FFI boundary | Custom panic hooks | `JoinHandle::await` + `Err(JoinError)` | Tokio JoinHandle already captures panics, source framework uses this |
| Bounded async channel | Custom ring buffer | `async_channel::bounded` | Battle-tested, already in use |

**Key insight:** The sink framework is simpler than the source framework. Sources need bridge thread, emit registry, backpressure state, semaphore gating, buffer provider. Sinks need only: channel, context, trait, handle. Don't add complexity that isn't needed.

## Common Pitfalls

### Pitfall 1: TupleBufferHandle Drop in Wrong Build Mode
**What goes wrong:** SinkMessage contains TupleBufferHandle. In production builds, TupleBufferHandle::Drop calls C++ release(). If any code path drops a production TupleBufferHandle in a test build, it triggers linker errors.
**Why it happens:** cfg(test) type substitution must be consistent across all types that touch TupleBufferHandle.
**How to avoid:** TupleBufferHandle already has cfg(test) variants (Vec<u8> in tests, UniquePtr in prod). SinkMessage should work in both modes as-is. Verify early by adding a compile-time test.
**Warning signs:** Linker errors mentioning `release`, `cloneTupleBuffer`, or `ffi::TupleBuffer` symbols in `cargo test`.

### Pitfall 2: Monitoring Task Not Cleaning Up on Error
**What goes wrong:** If sink run() returns Err, the monitoring task must call the error callback. If it doesn't, C++ never learns the sink failed.
**Why it happens:** Forgetting to handle all branches in the monitoring task match.
**How to avoid:** Mirror the source's monitoring_task exactly. Match on Ok(Ok), Ok(Err), and Err(JoinError) variants.
**Warning signs:** Sink errors are silently swallowed, C++ hangs waiting for completion.

### Pitfall 3: Channel Ordering with Close
**What goes wrong:** If Close is sent through a different mechanism than data (e.g., cancellation token), the sink might see Close before processing all data.
**Why it happens:** Separate signaling paths don't preserve FIFO ordering.
**How to avoid:** Decision is locked: Close goes through the same bounded channel as Data. Channel FIFO guarantees ordering.
**Warning signs:** Tests show Close received before all Data messages processed.

### Pitfall 4: Sender Lifetime in SinkTaskHandle
**What goes wrong:** If SinkTaskHandle is dropped (destroying the Sender), the channel closes before Close is sent. The sink sees a broken recv() instead of an orderly Close.
**Why it happens:** SinkTaskHandle holds the only Sender. If C++ drops the handle without calling stop_sink first, no Close message is sent.
**How to avoid:** Document that stop_sink MUST be called before dropping the handle. In the monitoring task, distinguish between "recv returned None (channel broken)" and "received Close message" -- the first is an error condition.
**Warning signs:** Sink run() receives None from channel instead of Close message.

### Pitfall 5: catch_unwind and async
**What goes wrong:** `std::panic::catch_unwind` cannot wrap an async fn directly because the Future returned from an async fn may not be `UnwindSafe`.
**Why it happens:** Async functions return futures that capture references; `catch_unwind` requires `UnwindSafe`.
**How to avoid:** Use `AssertUnwindSafe` wrapper OR rely on JoinHandle's built-in panic capture (recommended -- it's what the source framework does). The monitoring task's `join_handle.await` returns `Err(JoinError)` on panic.
**Warning signs:** Compiler error about `UnwindSafe` bound not satisfied.

### Pitfall 6: SinkContext Must Be Send + Sync
**What goes wrong:** `&SinkContext` must be `Send` for the async run() future to be `Send` (required by `tokio::spawn`). This means `SinkContext` must be `Sync`.
**Why it happens:** Rust's auto-trait rules: `&T` is `Send` iff `T` is `Sync`.
**How to avoid:** `unsafe impl Sync for SinkContext {}` with safety justification (same as SourceContext). All fields must be thread-safe: async_channel::Receiver is Send+Sync, CancellationToken is Send+Sync.
**Warning signs:** Compiler error about `SinkContext: Sync` not satisfied when trying to `tokio::spawn`.

## Code Examples

### Mock Sink for Testing (matches source test pattern)
```rust
// Source: source.rs test patterns (TestEndOfStreamSource, TestErrorSource, etc.)

struct MockCollectorSink {
    collected: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl AsyncSink for MockCollectorSink {
    async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
        loop {
            match ctx.recv().await {
                SinkMessage::Data(buf) => {
                    let data = buf.as_slice().to_vec();
                    self.collected.lock().unwrap().push(data);
                }
                SinkMessage::Close => {
                    return Ok(());
                }
            }
        }
    }
}
```

### spawn_sink Test Helper (matches spawn_test_source pattern)
```rust
// Source: source.rs spawn_test_source pattern

fn spawn_test_sink(
    sink_id: u64,
    sink: impl AsyncSink,
    channel_capacity: usize,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> (Box<SinkTaskHandle>, async_channel::Sender<SinkMessage>) {
    let cancel_token = CancellationToken::new();
    let (sender, receiver) = async_channel::bounded(channel_capacity);

    let ctx = SinkContext::new(receiver, cancel_token.clone());

    let mut sink = sink;
    let join_handle = tokio::spawn(async move {
        sink.run(&ctx).await
    });

    let err_cb = ErrorCallback { error_fn, error_ctx: SendVoidPtr(error_ctx) };
    tokio::spawn(async move {
        sink_monitoring_task(join_handle, sink_id, err_cb).await;
    });

    let handle = Box::new(SinkTaskHandle::new(sender.clone(), cancel_token, sink_id));
    (handle, sender)
}
```

### Full Lifecycle Test
```rust
// Verifies: open called (implicit in run start), write per buffer,
// flush/close on stop (implicit when sink processes Close)
#[tokio::test]
async fn full_lifecycle_open_write_close() {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = MockCollectorSink { collected: collected.clone() };

    let (handle, sender) = spawn_test_sink(40001, sink, 8, mock_error_callback, error_ctx);

    // Send 3 data buffers
    for i in 0..3u64 {
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..8].copy_from_slice(&i.to_le_bytes());
        sender.send(SinkMessage::Data(buf)).await.unwrap();
    }

    // Send Close
    sender.send(SinkMessage::Close).await.unwrap();

    // Wait for sink to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all 3 buffers were collected
    assert_eq!(collected.lock().unwrap().len(), 3);

    // Verify no error callback
    assert!(error_log.lock().unwrap().is_empty());
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Four-method trait (open/write/flush/close) | Single run() method with SinkMessage enum | Design decision (this phase) | Simpler trait surface, sink author controls own loop |
| Option-based recv (None = closed) | Explicit SinkMessage::Close variant | Design decision (this phase) | No ambiguity, explicit shutdown semantics |
| Separate completion flag (AtomicBool) | Channel closed detection via try_send | Design decision (this phase) | Fewer moving parts, leverages async_channel built-in |

**Note on REQUIREMENTS.md vs CONTEXT.md:** REQUIREMENTS.md says "SNK-01: AsyncSink Rust trait with open/write/flush/close lifecycle methods" but the user decision in CONTEXT.md overrides this to a single `run()` method. The planner MUST follow the CONTEXT.md decision.

## Open Questions

1. **SinkError: new type or reuse SourceError?**
   - What we know: SourceError has Display with "SourceError: " prefix. SinkError should say "SinkError: ".
   - What's unclear: Whether to create a separate SinkError type or a shared error type.
   - Recommendation: Create a separate `SinkError` type in `sink_error.rs` for clarity. Same structure as SourceError but different Display prefix. Keep types distinct -- they represent different failure domains.

2. **Default channel capacity**
   - What we know: CONTEXT.md says configurable via C++ in Phase 5. Phase 4 needs a default.
   - What's unclear: Optimal default value.
   - Recommendation: Use 64 (same as `BRIDGE_CHANNEL_CAPACITY`). Expose as a constant `DEFAULT_SINK_CHANNEL_CAPACITY`. Phase 5 makes it configurable via spawn parameter.

3. **Should SinkContext::recv() return the enum directly or Result?**
   - What we know: Decision says `recv().await` returns `SinkMessage`. Channel recv() can fail if sender is dropped without Close.
   - What's unclear: Whether to wrap in Result or handle broken channel internally.
   - Recommendation: Have `recv()` return `SinkMessage` directly. If the channel is broken (sender dropped without Close), treat it as if Close was received -- the sink should shut down cleanly. Log a warning internally. This keeps the sink author API simple.

4. **catch_unwind: explicit or via JoinHandle?**
   - What we know: CONTEXT.md says "Framework wraps run() in catch_unwind -- panics treated as errors". Source framework uses JoinHandle::await which returns Err(JoinError) on panic.
   - What's unclear: Whether to add explicit catch_unwind or rely on JoinHandle.
   - Recommendation: Rely on JoinHandle's built-in panic capture (same as source framework). The monitoring task handles `Err(JoinError)` identically. This is simpler and proven.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + tokio::test macro |
| Config file | nes-sources/rust/nes-source-runtime/Cargo.toml (dev-dependencies already include tokio test-util) |
| Quick run command | `cargo test -p nes_source_runtime sink` |
| Full suite command | `cargo test -p nes_source_runtime` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SNK-01 | AsyncSink trait is implementable with run() method | unit (compile-time) | `cargo test -p nes_source_runtime async_sink_trait_is_implementable` | Wave 0 |
| SNK-01 | AsyncSink requires Send + 'static | unit (compile-time) | `cargo test -p nes_source_runtime sink_is_send` | Wave 0 |
| SNK-02 | SinkContext provides CancellationToken | unit | `cargo test -p nes_source_runtime sink_context_has_cancellation_token` | Wave 0 |
| SNK-02 | SinkContext::recv() returns SinkMessage | unit | `cargo test -p nes_source_runtime sink_recv_returns_data_then_close` | Wave 0 |
| SNK-03 | Per-sink bounded channel delivers messages in order | unit | `cargo test -p nes_source_runtime sink_channel_fifo_ordering` | Wave 0 |
| SNK-03 | Channel backpressure: send blocks when full | unit | `cargo test -p nes_source_runtime sink_channel_blocks_when_full` | Wave 0 |
| SNK-04 | spawn_sink creates task that runs to completion | unit | `cargo test -p nes_source_runtime spawn_sink_runs_to_completion` | Wave 0 |
| SNK-04 | stop_sink sends Close and task exits | unit | `cargo test -p nes_source_runtime stop_sink_sends_close` | Wave 0 |
| SNK-04 | Error in sink triggers error callback | unit | `cargo test -p nes_source_runtime sink_error_triggers_callback` | Wave 0 |
| SNK-04 | Full lifecycle: open, write per buffer, close on stop | unit | `cargo test -p nes_source_runtime full_lifecycle_open_write_close` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test -p nes_source_runtime sink`
- **Per wave merge:** `cargo test -p nes_source_runtime`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `sink.rs` -- AsyncSink trait, spawn_sink, monitoring_task, tests
- [ ] `sink_context.rs` -- SinkContext struct, recv(), cfg(test) variants, tests
- [ ] `sink_error.rs` -- SinkError type, tests
- [ ] `sink_handle.rs` -- SinkTaskHandle, stop_sink, tests
- [ ] `lib.rs` updates -- pub mod + pub use for sink modules

## Sources

### Primary (HIGH confidence)
- `nes-sources/rust/nes-source-runtime/src/source.rs` -- AsyncSource trait pattern, spawn_source lifecycle, monitoring task, test patterns
- `nes-sources/rust/nes-source-runtime/src/context.rs` -- SourceContext pattern, cfg(test) substitution, Send/Sync unsafe impl
- `nes-sources/rust/nes-source-runtime/src/bridge.rs` -- BridgeMessage enum pattern, channel architecture
- `nes-sources/rust/nes-source-runtime/src/error.rs` -- SourceError type structure
- `nes-sources/rust/nes-source-runtime/src/handle.rs` -- SourceTaskHandle pattern, stop_source
- `nes-sources/rust/nes-source-runtime/src/buffer.rs` -- TupleBufferHandle cfg(test) variants
- `nes-sources/rust/nes-source-runtime/src/generator.rs` -- Concrete AsyncSource example, test patterns
- `nes-sources/rust/nes-source-runtime/Cargo.toml` -- Dependencies (async-channel 2.3.1, tokio 1.40.0, tokio-util 0.7.13)

### Secondary (MEDIUM confidence)
- `.planning/phases/04-rust-sink-framework/04-CONTEXT.md` -- User decisions constraining implementation
- `.planning/REQUIREMENTS.md` -- Phase requirement definitions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in use, no new dependencies
- Architecture: HIGH -- direct mirror of existing source framework with well-understood simplifications
- Pitfalls: HIGH -- all pitfalls derived from patterns already encountered in source implementation

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable -- no external dependency changes expected)
