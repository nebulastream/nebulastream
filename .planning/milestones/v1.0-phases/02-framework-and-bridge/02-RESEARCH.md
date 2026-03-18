# Phase 2: Framework and Bridge - Research

**Researched:** 2026-03-15
**Domain:** Async Rust source framework, bridge thread, backpressure translation, C++ integration
**Confidence:** HIGH

## Summary

Phase 2 wires the complete async-to-C++ pipeline. A Rust source task calls `ctx.emit(buf).await`, which acquires a semaphore permit, checks backpressure, and sends the buffer through a bounded `async_channel` to a single shared bridge thread. The bridge thread calls `recv_blocking()`, then invokes the C++ `EmitFunction` (via function pointer + void* context) to push the buffer into the Folly MPMC task queue. The `OnComplete` callback releases the semaphore permit. C++ backpressure is translated to an async-aware `BackpressureFuture` using `AtomicWaker` + `AtomicBool`, signaled from C++ via a new CXX bridge callback `on_backpressure_released(source_id)`. A `TokioSource` C++ class wraps the Rust lifecycle and plugs into the existing `SourceHandle` via `std::variant`.

The key technical challenges are: (1) correct BackpressureFuture poll ordering to avoid lost wakes, (2) function pointer + void* context pattern for the emit callback since CXX does not support `std::function`, (3) bridge thread lifecycle management via `OnceLock` + drain-on-drop, and (4) the `TokioSource` C++ class that implements the same `start`/`stop`/`tryStop` interface as `SourceThread`.

**Primary recommendation:** Use `atomic-waker` crate (or `futures-util::task::AtomicWaker`) for BackpressureFuture, `DashMap` for per-source future storage, a single `std::thread` for the bridge, and C-style `extern "C" fn` + `*mut c_void` for the emit callback across FFI. Do not change `RunningSource` or `QueryEngine` -- all integration flows through `SourceHandle`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- AtomicBool + Waker pattern for BackpressureFuture -- not spawn_blocking, not bridge-thread-owned
- Per-source BackpressureFuture: each source gets its own future instance, stored in a concurrent map (DashMap or similar)
- C++ signals Rust via new CXX bridge callback: `on_backpressure_released(source_id)` -- C++ BackpressureController calls into Rust, which sets AtomicBool and wakes the future
- BackpressureFuture lives inside `SourceContext::emit()` -- source authors never see backpressure, emit() just takes longer when pressure is applied
- Initial state: flag=true (no pressure) -- source can emit immediately on startup, C++ applies pressure later if needed
- Poll ordering must be correct: store waker first, then check flag -- prevents lost-wake race
- Single shared bridge thread for all sources -- not per-source
- Emit callback is already source-agnostic (takes origin_id parameter), so no routing needed
- Single shared `async_channel::bounded` channel -- all sources send to the same channel via `Sender::clone()`
- Channel messages carry `(OriginId, TupleBufferHandle, SemaphorePermit)`
- Bridge thread calls `receiver.recv_blocking()` -- sync interface on the async channel
- Small fixed channel capacity (e.g., 32 or 64) -- channel is primarily a sync/async adapter, not a deep buffer. Configurable later.
- Bridge thread started lazily on first source registration via OnceLock -- avoids thread overhead if no async sources are used
- New C++ class `TokioSource` (not "AsyncSourceThread" -- it's a task, not a thread)
- `TokioSource` implements the same interface as `SourceThread` (start, stop, tryStop)
- `SourceHandle` holds `std::variant<SourceThread, TokioSource>` -- RunningSource and QueryEngine unchanged
- `spawn_source()` FFI call returns an opaque `Box<SourceTaskHandle>` to C++ -- handle-based lifecycle, not global registry
- Emit callback passed through FFI via C-style function pointer + void* context (standard FFI pattern since CXX doesn't support std::function)
- C++ wraps `EmitFunction` into an `EmitContext` struct, passes pointer; Rust bridge thread calls back through the function pointer
- `stop_source()` is non-blocking: just cancels the CancellationToken and returns immediately to C++ -- matches existing SourceThread async stop model
- No timeout fallback for unresponsive sources -- trust the source author, consistent with existing C++ behavior
- Cleanup via Rust ownership: task exits -> SourceContext dropped -> Sender dropped -> channel closes -> bridge drains remaining items via Drop -> TupleBufferHandle::drop() releases buffers -> permits dropped
- Source panics/errors surfaced via C++ FFI callback: `on_source_error(source_id, msg)` -- engine gets immediate notification
- A background Tokio monitoring task watches the JoinHandle and calls on_source_error if it completes with Err or panic

### Claude's Discretion
- Exact BackpressureFuture implementation details (AtomicBool ordering, waker storage)
- DashMap vs RwLock<HashMap> for the futures map
- Exact channel capacity number
- EmitContext struct layout and lifetime management
- SourceTaskHandle internal fields
- Bridge thread's exact recv loop structure
- How TokioSource discovers the bridge channel sender

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FWK-02 | `AsyncSource` trait that handles backpressure, semaphore, and channel internally | Trait wraps user `run()` with semaphore-gated emit + BackpressureFuture. Pattern: `async_trait`-style or `impl Future` directly. User sees only `ctx.allocate_buffer()` and `ctx.emit(buf).await`. |
| FWK-04 | Per-source Tokio Semaphore for inflight buffer tracking (capacity from defaultMaxInflightBuffers) | `tokio::sync::Semaphore` with capacity from `SourceRuntimeConfiguration::inflightBufferLimit`. Permit acquired in `emit()`, released via C++ `OnComplete` callback through bridge. `OwnedSemaphorePermit` carried in channel message. |
| FWK-05 | C++ BackpressureListener translated to Rust awaitable future | `BackpressureFuture` using `AtomicWaker` + `AtomicBool`. C++ `on_backpressure_released(source_id)` calls into Rust to set flag and wake. `atomic-waker` crate provides the canonical implementation. |
| FWK-06 | CancellationToken per source for cooperative graceful shutdown | Already partially implemented in Phase 1 `SourceContext`. Phase 2 wires it: `stop_source()` FFI cancels the token, monitoring task calls `on_source_error` on task exit. |
| FWK-07 | Error surfacing via JoinHandle -- panics/errors logged, affected source shut down | Background monitoring task (`tokio::spawn`) awaits the source's `JoinHandle`. On error/panic, calls C++ `on_source_error(source_id, msg)` via FFI. |
| BRG-01 | Single bridge thread receiving from async_channel with blocking recv, pushing to Folly MPMC task queue | `std::thread::spawn` started via `OnceLock`. Calls `receiver.recv_blocking()` in loop. On recv, invokes C++ emit function pointer with `(OriginId, Data{buffer}, stop_token)`. |
| BRG-02 | Channel message carries (buffer, semaphore_permit) so bridge constructs OnComplete callback for permit release | Message type: `(OriginId, TupleBufferHandle, OwnedSemaphorePermit)`. Bridge drops permit after C++ emit returns, or passes permit ownership into `OnComplete` callback via the context pointer. |
| BRG-03 | Bounded async_channel with capacity tied to defaultMaxInflightBuffers | `async_channel::bounded(capacity)` where capacity is a small fixed number (32 or 64). The channel is an async/sync adapter, not the primary backpressure mechanism (semaphore handles that). |
| BRG-04 | SourceHandle variant wrapping async Tokio sources, plugging into RunningSource unchanged | New C++ class `TokioSource` with `start(EmitFunction&&)`, `stop()`, `tryStop(timeout)`. `SourceHandle` uses `std::variant<SourceThread, TokioSource>`. `RunningSource` unchanged. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.40.0 | Async runtime, Semaphore, spawn, JoinHandle | Already used in Phase 1; provides `sync::Semaphore` for inflight tracking |
| tokio-util | 0.7.13 | CancellationToken | Already used in Phase 1 for cooperative shutdown |
| async-channel | 2.3.1 | Bounded MPMC channel between Tokio tasks and bridge thread | Already in Cargo.toml; `recv_blocking()` is critical for bridge thread |
| cxx | 1.0 | Rust/C++ FFI bridge | Already used in Phase 1 |
| tracing | 0.1 | Structured logging | Already used in Phase 1 |

### New for Phase 2
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| atomic-waker | 1.1 | AtomicWaker for BackpressureFuture | Stores/wakes the waker in a thread-safe way from C++ callback |
| dashmap | 6.1 | Concurrent map for per-source BackpressureFuture state | Stores Arc<BackpressureState> keyed by source_id, accessed from both Tokio tasks and C++ callback thread |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| atomic-waker | futures-util AtomicWaker | Identical impl; atomic-waker is smaller (single-purpose crate, fewer deps). Either works. |
| dashmap | RwLock<HashMap> | RwLock is stdlib, no extra dep, but higher contention under concurrent register/release. DashMap is sharded, lower contention. |
| async_channel::bounded for bridge | crossbeam-channel | crossbeam is sync-only; async_channel supports both async Sender and blocking Receiver which is exactly the bridge pattern. |

**Installation:**
```toml
# nes-source-runtime/Cargo.toml (additions to existing)
[dependencies]
atomic-waker = "1.1"
dashmap = "6.1"
```

## Architecture Patterns

### Recommended Module Structure
```
nes-source-runtime/src/
├── lib.rs                  # pub mod declarations, re-exports
├── runtime.rs              # OnceLock runtime (Phase 1, unchanged)
├── buffer.rs               # TupleBufferHandle, BufferProviderHandle (Phase 1, unchanged)
├── context.rs              # SourceContext -- updated: emit() adds backpressure + semaphore
├── error.rs                # SourceResult, SourceError (Phase 1, unchanged)
├── source.rs               # NEW: AsyncSource trait, spawn_source()
├── backpressure.rs         # NEW: BackpressureFuture, BackpressureState, registry
├── bridge.rs               # NEW: Bridge thread, channel setup, OnceLock<BridgeHandle>
└── handle.rs               # NEW: SourceTaskHandle (opaque handle returned to C++)

nes-source-bindings/
├── lib.rs                  # cxx bridge -- expanded with Phase 2 FFI functions
└── src/SourceBindings.cpp  # C++ implementations -- expanded

nes-sources/ (C++ side)
├── include/Sources/
│   ├── SourceHandle.hpp    # MODIFIED: std::variant<SourceThread, TokioSource>
│   └── TokioSource.hpp     # NEW: C++ wrapper for Rust async source lifecycle
├── private/
│   └── TokioSource.cpp     # NEW
└── src/
    ├── SourceHandle.cpp    # MODIFIED: dispatch via variant visitor
    └── TokioSource.cpp     # NEW
```

### Pattern 1: BackpressureFuture with AtomicWaker
**What:** A custom `Future` that yields until C++ releases backpressure, using `AtomicWaker` to avoid blocking Tokio worker threads.
**When to use:** Inside `SourceContext::emit()`, before sending to the channel.
**Example:**
```rust
// Source: atomic-waker crate docs + Rust async book pattern
use atomic_waker::AtomicWaker;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct BackpressureState {
    waker: AtomicWaker,
    /// true = no pressure (source can proceed), false = pressure applied
    released: AtomicBool,
}

impl BackpressureState {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            // Initial state: no pressure -- source can emit immediately
            released: AtomicBool::new(true),
        }
    }

    /// Called from C++ via FFI when backpressure is released.
    pub fn signal_released(&self) {
        self.released.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Called from C++ via FFI when backpressure is applied.
    pub fn signal_applied(&self) {
        self.released.store(false, Ordering::Release);
    }

    /// Returns a future that resolves when backpressure is released.
    pub fn wait_for_release(&self) -> BackpressureFuture<'_> {
        BackpressureFuture { state: self }
    }
}

pub struct BackpressureFuture<'a> {
    state: &'a BackpressureState,
}

impl<'a> Future for BackpressureFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path: check if already released
        if self.state.released.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        // Register waker BEFORE checking condition (prevents lost-wake race)
        self.state.waker.register(cx.waker());

        // Re-check after registration
        if self.state.released.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
```

### Pattern 2: Bridge Thread with Blocking Recv
**What:** A single OS thread that bridges async Rust and synchronous C++ by calling `recv_blocking()` on an `async_channel::Receiver`.
**When to use:** The bridge thread is the single point where async buffers cross into the C++ Folly MPMC task queue.
**Example:**
```rust
use std::sync::OnceLock;

struct BridgeHandle {
    sender: async_channel::Sender<BridgeMessage>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct BridgeMessage {
    origin_id: u64,
    buffer: TupleBufferHandle,
    permit: tokio::sync::OwnedSemaphorePermit,
}

static BRIDGE: OnceLock<BridgeHandle> = OnceLock::new();

fn ensure_bridge(emit_fn: EmitFnPtr, emit_ctx: *mut std::ffi::c_void) -> &'static BridgeHandle {
    BRIDGE.get_or_init(|| {
        let (sender, receiver) = async_channel::bounded::<BridgeMessage>(64);
        let thread = std::thread::Builder::new()
            .name("nes-bridge".into())
            .spawn(move || {
                bridge_loop(receiver, emit_fn, emit_ctx);
            })
            .expect("Failed to spawn bridge thread");
        BridgeHandle {
            sender,
            thread: Some(thread),
        }
    })
}

fn bridge_loop(
    receiver: async_channel::Receiver<BridgeMessage>,
    emit_fn: EmitFnPtr,
    emit_ctx: *mut std::ffi::c_void,
) {
    while let Ok(msg) = receiver.recv_blocking() {
        // Call C++ emit function through function pointer
        // SAFETY: emit_fn and emit_ctx are valid for the bridge's lifetime
        unsafe {
            (emit_fn)(emit_ctx, msg.origin_id, /* buffer ptr */);
        }
        // permit is dropped here, releasing the semaphore slot
        // buffer is consumed by C++ (ownership transferred via retain)
    }
    // Channel closed -- all senders dropped, bridge shuts down
}
```

### Pattern 3: Function Pointer + void* Context for FFI Callbacks
**What:** Standard C FFI callback pattern since CXX does not support `std::function`.
**When to use:** For the emit callback from Rust bridge thread into C++ task queue.
**Example (C++ side):**
```cpp
// EmitContext holds everything the bridge needs to call into the C++ engine
struct EmitContext {
    SourceReturnType::EmitFunction emitFunction;
    std::stop_source stopSource;  // For creating stop_tokens
};

// C-linkage function that the Rust bridge calls
extern "C" uint8_t bridge_emit(
    void* context,
    uint64_t origin_id,
    NES::TupleBuffer* buffer_ptr,
    void (*on_complete)(void*),
    void* on_complete_ctx
) {
    auto* ctx = static_cast<EmitContext*>(context);
    auto buffer = NES::TupleBuffer(*buffer_ptr);  // Copy (retain)

    // Build the SourceReturnType
    auto result = ctx->emitFunction(
        OriginId(origin_id),
        SourceReturnType::Data{std::move(buffer)},
        ctx->stopSource.get_token()
    );

    // Call OnComplete to release semaphore permit in Rust
    if (on_complete) {
        on_complete(on_complete_ctx);
    }

    return static_cast<uint8_t>(result);
}
```

### Pattern 4: TokioSource C++ Class
**What:** A C++ class that wraps the Rust async source lifecycle, implementing the same interface as `SourceThread`.
**When to use:** For integrating async Rust sources into the existing `SourceHandle`/`RunningSource` system.
**Example:**
```cpp
class TokioSource {
public:
    TokioSource(OriginId originId, /* Rust source params */);
    ~TokioSource();

    [[nodiscard]] bool start(SourceReturnType::EmitFunction&& emitFunction);
    void stop();
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout);
    [[nodiscard]] OriginId getOriginId() const;

private:
    OriginId originId;
    // Opaque Rust handle: Box<SourceTaskHandle>
    rust::Box<SourceTaskHandle> rustHandle;
    std::unique_ptr<EmitContext> emitContext;
};
```

### Pattern 5: Semaphore Permit in Channel Message
**What:** The `OwnedSemaphorePermit` is sent through the channel alongside the buffer, ensuring the permit lifetime is tied to the buffer's processing lifecycle.
**When to use:** In `SourceContext::emit()` and the bridge thread.
**Example:**
```rust
impl SourceContext {
    pub async fn emit(&self, buffer: TupleBufferHandle) -> Result<(), SourceError> {
        // 1. Wait for backpressure release (async, non-blocking)
        self.backpressure_state.wait_for_release().await;

        // 2. Acquire semaphore permit (async, non-blocking)
        let permit = self.semaphore.clone()
            .acquire_owned()
            .await
            .map_err(|_| SourceError::new("semaphore closed"))?;

        // 3. Send buffer + permit through channel
        let msg = BridgeMessage {
            origin_id: self.origin_id,
            buffer,
            permit,
        };
        self.sender.send(msg).await
            .map_err(|_| SourceError::new("bridge channel closed"))?;

        Ok(())
    }
}
```

### Anti-Patterns to Avoid
- **Blocking on backpressure from a Tokio worker thread:** The whole point of BackpressureFuture is to yield the thread. Never call `BackpressureListener::wait()` from async context -- use the AtomicWaker future instead.
- **Per-source bridge threads:** Creates unnecessary OS threads. One shared bridge thread is sufficient since the emit callback is source-agnostic (takes origin_id).
- **Dropping the semaphore permit before C++ finishes processing:** The permit must live until the `OnComplete` callback fires, or the inflight buffer count becomes incorrect. The simplest approach: drop the permit in the bridge thread after the C++ emit call returns (synchronous in the bridge thread context).
- **Using `tokio::sync::mpsc` for the bridge channel:** `mpsc::Receiver` has no `recv_blocking()`. `async_channel` supports both async send and blocking receive, which is exactly the bridge pattern.
- **Putting the bridge OnceLock inside Tokio runtime init:** The bridge is independent of the runtime. It should be lazily initialized on first source registration, not on runtime init.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Atomic waker storage | Manual `UnsafeCell<Option<Waker>>` | `atomic-waker` crate (`AtomicWaker`) | Subtle memory ordering requirements; reference impl from futures-rs maintainers |
| Concurrent per-source map | `Mutex<HashMap>` with manual locking | `DashMap` | Sharded concurrent map, lower contention for register/lookup/remove |
| Inflight buffer tracking | Manual AtomicUsize counter | `tokio::sync::Semaphore` + `OwnedSemaphorePermit` | Correct async yielding, automatic release on drop, no manual decrement bugs |
| Async cancellation | Manual AtomicBool polling in source loop | `tokio_util::sync::CancellationToken` | Already established in Phase 1; composable with `tokio::select!` |
| Async/sync channel bridge | Custom ring buffer + condvar | `async_channel::bounded` | Already in deps; supports async Sender + blocking Receiver natively |

**Key insight:** Every piece of the bridge pipeline has a well-tested crate solution. The complexity is in wiring them together correctly, not in implementing any single piece.

## Common Pitfalls

### Pitfall 1: Lost Waker in BackpressureFuture
**What goes wrong:** The C++ side releases backpressure between the `released.load()` check and the `waker.register()` call. The future never wakes up.
**Why it happens:** Classic TOCTOU race if poll ordering is wrong.
**How to avoid:** Always: (1) register waker first, (2) then check the flag. If the flag was set between registration and check, the future returns Ready immediately. This is the standard pattern documented in `AtomicWaker`.
**Warning signs:** Source hangs indefinitely after backpressure is released.

### Pitfall 2: Semaphore Permit Leak on Channel Send Failure
**What goes wrong:** If `sender.send(msg).await` returns `Err` (channel closed), the `OwnedSemaphorePermit` inside the message is dropped. This is actually correct behavior (permit released), but if the error path doesn't properly drain the channel, permits may leak.
**Why it happens:** The bridge thread may have exited before all senders are dropped.
**How to avoid:** Ensure the bridge channel receiver is drained during shutdown. When the receiver detects channel closure, it drains all remaining messages. Each drained message drops its permit (correct). The bridge thread's Drop impl should handle this.
**Warning signs:** Tests report leaked semaphore permits after shutdown.

### Pitfall 3: EmitContext Lifetime Mismatch
**What goes wrong:** The `EmitContext` (holding the `EmitFunction` closure and `stop_source`) is destroyed while the bridge thread still holds a pointer to it.
**Why it happens:** The `TokioSource` destructor runs before the bridge thread finishes processing remaining messages for that source.
**How to avoid:** The `EmitContext` must outlive all bridge activity for that source. Two approaches: (1) `stop_source()` cancels the token, waits for the Rust task to exit, ensures no more messages will be sent for this source, then destroys the context. (2) Make `EmitContext` reference-counted (`shared_ptr`) and pass a reference into the channel message. Approach (1) is simpler and matches the SourceThread model where `stop()` blocks until the thread exits.
**Warning signs:** Use-after-free crash in bridge thread callback.

### Pitfall 4: Bridge Thread Deadlock on Shutdown
**What goes wrong:** The bridge thread calls `recv_blocking()` which blocks. If no one closes the channel, the thread never exits.
**Why it happens:** The channel is only closed when all `Sender` handles are dropped. If a `SourceContext` leaks its sender, the channel stays open.
**How to avoid:** The bridge thread's shutdown is driven by channel closure: all sources stop -> all `SourceContext` dropped -> all `Sender` clones dropped -> channel closes -> `recv_blocking()` returns `Err` -> bridge thread exits. For process shutdown, use a separate mechanism (e.g., close the receiver explicitly) or accept that the bridge thread is killed with the process.
**Warning signs:** Process hangs on shutdown.

### Pitfall 5: CXX Cannot Bridge std::function or std::stop_token
**What goes wrong:** Attempting to declare `std::function<EmitResult(...)>` or `std::stop_token` in the CXX bridge fails at compile time.
**Why it happens:** CXX only supports a limited set of C++ types. `std::function`, `std::stop_token`, `std::variant`, and most STL templates are not supported.
**How to avoid:** Use C-style function pointers (`extern "C" fn(...)`) with `void*` context for callbacks. For `stop_token`, create a C++ wrapper that exposes `stop_requested()` as a free function on an opaque type. Alternatively, since the Rust side manages cancellation via `CancellationToken`, only the bridge thread needs to pass a stop indication to C++, which can be done via a simple boolean check.
**Warning signs:** CXX compilation errors about unsupported types.

### Pitfall 6: SourceHandle Variant Breaks RunningSource
**What goes wrong:** Changing `SourceHandle` from owning a `unique_ptr<SourceThread>` to `variant<SourceThread, TokioSource>` breaks the existing API.
**Why it happens:** `SourceHandle` currently delegates all calls to `SourceThread`. With a variant, each method needs a `std::visit` dispatch.
**How to avoid:** Change `SourceHandle`'s internal `unique_ptr<SourceThread>` to `std::variant<std::unique_ptr<SourceThread>, std::unique_ptr<TokioSource>>`. Use `std::visit` with `Overloaded{}` pattern (already used in RunningSource.cpp) for each method. The public API of `SourceHandle` remains identical -- `start()`, `stop()`, `tryStop()`, `getSourceId()`. `RunningSource` does not change at all.
**Warning signs:** Compile errors in RunningSource or QueryEngine after SourceHandle changes.

## Code Examples

### New CXX Bridge Declarations (Phase 2 additions to lib.rs)
```rust
#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("SourceBindings.hpp");

        // Phase 1 types (unchanged)
        type TupleBuffer;
        type AbstractBufferProvider;
        type BackpressureListener;
        // ... existing functions ...

        // Phase 2: Opaque Rust handle visible to C++
        // (CXX can pass Box<T> across the boundary)
    }

    extern "Rust" {
        // Phase 1 (unchanged)
        fn init_source_runtime(thread_count: u32) -> Result<()>;

        // Phase 2: Source lifecycle
        type SourceTaskHandle;
        fn spawn_source(
            source_id: u64,
            buffer_provider: *mut AbstractBufferProvider,
            inflight_limit: u32,
            emit_fn: unsafe extern "C" fn(*mut c_void, u64, *mut TupleBuffer) -> u8,
            emit_ctx: *mut c_void,
            error_fn: unsafe extern "C" fn(*mut c_void, u64, *const c_char),
            error_ctx: *mut c_void,
        ) -> Result<Box<SourceTaskHandle>>;
        fn stop_source(handle: &SourceTaskHandle);

        // Phase 2: Backpressure callbacks (called from C++)
        fn on_backpressure_applied(source_id: u64);
        fn on_backpressure_released(source_id: u64);
    }
}
```

### SourceContext::emit() with Backpressure and Semaphore
```rust
impl SourceContext {
    pub async fn emit(&self, buffer: TupleBufferHandle) -> Result<(), SourceError> {
        // 1. Await backpressure release (non-blocking to Tokio)
        self.backpressure.wait_for_release().await;

        // 2. Acquire inflight permit (non-blocking to Tokio)
        let permit = self.semaphore.clone()
            .acquire_owned()
            .await
            .map_err(|_| SourceError::new("semaphore closed -- source shutting down"))?;

        // 3. Send through bridge channel
        self.sender
            .send(BridgeMessage {
                origin_id: self.origin_id,
                buffer,
                permit,
            })
            .await
            .map_err(|_| SourceError::new("bridge channel closed -- source shutting down"))
    }
}
```

### AsyncSource Trait
```rust
/// Trait that source authors implement. Only `run()` is required.
/// The framework handles backpressure, semaphores, channels, and lifecycle.
pub trait AsyncSource: Send + 'static {
    /// Run the source. Use `ctx.allocate_buffer()` and `ctx.emit(buf).await`.
    /// Return `EndOfStream` when done, or `Error` on failure.
    /// Cancellation is handled via `ctx.cancellation_token()` in `tokio::select!`.
    fn run(
        &mut self,
        ctx: SourceContext,
    ) -> impl std::future::Future<Output = SourceResult> + Send;
}
```

### Bridge Thread OnComplete Callback
```rust
// The permit is dropped synchronously in the bridge thread after C++ returns.
// This is the simplest correct approach: the bridge thread is a sync context,
// so dropping the permit (which releases the semaphore) is safe.
fn bridge_loop(receiver: async_channel::Receiver<BridgeMessage>, /* ... */) {
    while let Ok(msg) = receiver.recv_blocking() {
        // Call C++ emit function
        let result = unsafe { (emit_fn)(emit_ctx, msg.origin_id, /* buffer */) };
        // msg.permit is dropped here, releasing the semaphore
        // msg.buffer ownership transferred to C++ via retain in the callback
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| BackpressureListener::wait() (blocking) | AtomicWaker + AtomicBool future | This phase | Non-blocking for Tokio worker threads |
| SourceThread (one OS thread per source) | Tokio task + shared bridge thread | This phase | Thousands of sources without thread explosion |
| std::function for emit callback across FFI | C-style function pointer + void* context | This phase (CXX constraint) | Works with CXX bridge; same performance |
| SourceHandle owns SourceThread directly | std::variant<SourceThread, TokioSource> | This phase | Backward compatible; RunningSource unchanged |

**Important version note:** `tokio::sync::Semaphore::acquire_owned()` requires `Arc<Semaphore>`. Available since tokio 1.0. The `OwnedSemaphorePermit` type is `Send`, which is required for sending through the channel.

## Open Questions

1. **Buffer Metadata (OriginId, SequenceNumber, Timestamps)**
   - What we know: `SourceThread::runningRoutine` adds metadata (originId, sequenceNumber, timestamps, chunkNumber) via `addBufferMetaData()` before emitting. TokioSource must do the same.
   - What's unclear: Should metadata be added on the Rust side (in `SourceContext::emit()`) or on the bridge thread (before calling C++ emit)?
   - Recommendation: Add metadata in the bridge thread, matching the SourceThread pattern. The bridge thread has access to a per-source sequence number counter. This keeps `SourceContext::emit()` simple for source authors.

2. **Bridge Thread Ownership of EmitFunction**
   - What we know: The bridge is shared across all sources. The EmitFunction is per-source (captures per-query state). The bridge cannot own one EmitFunction.
   - What's unclear: How the bridge thread gets the correct EmitFunction for each message.
   - Recommendation: Each `BridgeMessage` should carry a function pointer + context, OR the bridge should own a map of `source_id -> EmitContext`. The simpler approach: since `TokioSource::start()` receives the `EmitFunction`, it wraps it in an `EmitContext` and registers it in a global map. The bridge thread looks up the context by `source_id` for each message. On source stop, the context is unregistered.

3. **stop_token for C++ EmitFunction**
   - What we know: The C++ `EmitFunction` signature takes `const std::stop_token&`. SourceThread uses its jthread's stop_token.
   - What's unclear: What stop_token TokioSource provides. It doesn't have a jthread.
   - Recommendation: `TokioSource` owns a `std::stop_source`. `stop()` calls `stop_source.request_stop()` (in addition to cancelling the Rust token). The bridge thread passes `stop_source.get_token()` to the EmitFunction. This matches the SourceThread behavior where stop is signaled via stop_token.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust `#[cfg(test)]` + `#[tokio::test]` for async tests |
| Config file | Cargo.toml `[dev-dependencies]` section |
| Quick run command | `cargo test -p nes_source_runtime --lib` |
| Full suite command | `cargo test --workspace` (from nes-sources/rust/) |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FWK-02 | AsyncSource trait can be implemented and spawned | unit | `cargo test -p nes_source_runtime source::tests` | No -- Wave 0 |
| FWK-04 | Semaphore blocks emit when inflight limit reached, resumes on permit release | unit | `cargo test -p nes_source_runtime context::tests::test_emit_semaphore_backpressure` | No -- Wave 0 |
| FWK-05 | BackpressureFuture resolves on signal, yields when pressure applied | unit | `cargo test -p nes_source_runtime backpressure::tests` | No -- Wave 0 |
| FWK-06 | CancellationToken stops source task | unit | `cargo test -p nes_source_runtime source::tests::test_cancellation` | No -- Wave 0 |
| FWK-07 | Monitoring task reports error on source panic/error | unit | `cargo test -p nes_source_runtime source::tests::test_error_surfacing` | No -- Wave 0 |
| BRG-01 | Bridge thread receives and processes messages | unit | `cargo test -p nes_source_runtime bridge::tests::test_bridge_recv` | No -- Wave 0 |
| BRG-02 | Permit released after bridge processes message | unit | `cargo test -p nes_source_runtime bridge::tests::test_permit_release` | No -- Wave 0 |
| BRG-03 | Bounded channel applies backpressure when full | unit | `cargo test -p nes_source_runtime bridge::tests::test_bounded_channel` | No -- Wave 0 |
| BRG-04 | TokioSource C++ class compiles and links | build | CMake build + ctest | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --workspace` (from nes-sources/rust/)
- **Per wave merge:** `cargo test --workspace` + CMake build verification
- **Phase gate:** Full Rust test suite green + CMake integration builds + manual verification of emit flow

### Wave 0 Gaps
- [ ] `nes-source-runtime/src/backpressure.rs` -- BackpressureFuture tests (FWK-05)
- [ ] `nes-source-runtime/src/bridge.rs` -- Bridge thread tests (BRG-01, BRG-02, BRG-03)
- [ ] `nes-source-runtime/src/source.rs` -- AsyncSource trait + spawn tests (FWK-02, FWK-06, FWK-07)
- [ ] `nes-source-runtime/src/context.rs` -- Updated emit() tests with semaphore (FWK-04)
- [ ] `atomic-waker` and `dashmap` dependency additions to Cargo.toml
- [ ] Note: BRG-04 (TokioSource C++ class) requires CMake integration test, not pure Rust test

## Sources

### Primary (HIGH confidence)
- Codebase: `nes-query-engine/RunningSource.cpp` -- confirmed EmitFunction construction, counting_semaphore usage, OnComplete callback pattern, WorkEmitter::emitWork interface
- Codebase: `nes-sources/private/SourceThread.hpp` -- confirmed SourceThread interface: start(EmitFunction&&), stop(), tryStop(timeout)
- Codebase: `nes-sources/include/Sources/SourceHandle.hpp` -- confirmed SourceHandle delegates to SourceThread, SourceRuntimeConfiguration has inflightBufferLimit
- Codebase: `nes-executable/src/BackpressureChannel.cpp` -- confirmed Channel struct (OPEN/CLOSED/DESTROYED), condition_variable_any, releasePressure() notifies all waiters
- Codebase: `nes-sources/include/Sources/SourceReturnType.hpp` -- confirmed EmitFunction = std::function<EmitResult(OriginId, SourceReturnType, const stop_token&)>
- Codebase: `nes-query-engine/Task.hpp` -- confirmed TaskCallback::OnComplete pattern used for semaphore release
- [atomic-waker docs](https://docs.rs/atomic-waker/latest/atomic_waker/struct.AtomicWaker.html) -- complete API and usage example with AtomicBool

### Secondary (MEDIUM confidence)
- [CXX extern "Rust"](https://cxx.rs/extern-rust.html) -- Box<T> can be passed across FFI boundary for opaque Rust types
- [Rust async book - wakeups](https://rust-lang.github.io/async-book/02_execution/03_wakeups.html) -- register-waker-before-check pattern
- [CXX issue #895](https://github.com/dtolnay/cxx/issues/895) -- confirms CXX does not support std::function, function pointers with void* context is the standard workaround

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all core libraries already in use from Phase 1; new additions (atomic-waker, dashmap) are widely used and well-documented
- Architecture: HIGH -- patterns derived directly from existing codebase (RunningSource emit pattern, SourceThread interface, BackpressureChannel implementation)
- Pitfalls: HIGH -- derived from actual codebase analysis (CXX limitations, BackpressureChannel internals, EmitFunction signature, stop_token requirements)

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable domain, no fast-moving dependencies)
