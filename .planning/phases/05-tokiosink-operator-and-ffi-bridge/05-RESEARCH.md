# Phase 5: TokioSink Operator and FFI Bridge - Research

**Researched:** 2026-03-16
**Domain:** C++ sink operator with CXX FFI bridge to Rust async sink runtime, backpressure hysteresis, flush-confirm stop protocol
**Confidence:** HIGH

## Summary

Phase 5 bridges the Rust sink framework (Phase 4) to the C++ pipeline execution engine. The TokioSink C++ operator implements `ExecutablePipelineStage` (start/execute/stop) and communicates with Rust `SinkTaskHandle` via CXX bridge functions. The core patterns are well-established: TokioSource provides the lifecycle/PIMPL template, NetworkSink provides the backpressure hysteresis + repeatTask template. The main complexity is the buffer ownership transfer (C++ retains before FFI, Rust releases on TupleBufferHandle drop) and the flush-confirm stop protocol (drain pending buffers, send Close, poll is_sink_done via repeatTask).

This phase also renames the `nes-source-bindings` crate to `nes-io-bindings` (directory, Cargo.toml, CMakeLists, all imports) and the corresponding C++ class from `SourceBindings` to `IoBindings`. This is a mechanical rename but touches many files.

**Primary recommendation:** Mirror NetworkSink's execute/stop pattern exactly for the C++ TokioSink operator, and mirror TokioSource's PIMPL/SpawnFn/lifecycle pattern for the Rust handle management. The CXX bridge adds `sink_send_buffer` returning a shared enum SendResult{Success, Full, Closed}, plus `spawn_sink`, `stop_sink`, and `is_sink_done` wrappers.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Buffer ownership across FFI:**
- C++ retains() TupleBuffer before sending through FFI, Rust releases on TupleBufferHandle drop
- Reuse existing TupleBufferHandle as-is for sink buffers (same type as sources)
- FFI function takes raw pointer (void*) -- C++ retains, passes raw ptr, Rust wraps in TupleBufferHandle
- Mirrors source pattern but in reverse direction (C++ -> Rust instead of Rust -> C++)

**Backpressure & repeatTask strategy:**
- Full hysteresis backpressure matching NetworkSink pattern (upper/lower thresholds)
- Hysteresis thresholds configurable per TokioSink (like NetworkSink's BackpressureHandler constructor params)
- Channel capacity also configurable per sink
- repeatTask retry when try_send fails (non-blocking pipeline thread)
- 10ms retry interval matching NetworkSink's BACKPRESSURE_RETRY_INTERVAL

**Flush-confirm stop protocol:**
- On stop(): drain BackpressureHandler's internal buffer into channel (may need repeatTask if channel full), then send Close -- no data loss
- No waiting for new execute() calls -- stop means "flush pending + close"
- tryStop() polls is_sink_done() via repeatTask, returns TIMEOUT if not done -- matches TokioSource::tryStop() pattern
- No hard timeout -- trust the sink to finish after receiving Close
- Query failure scenario: C++ drops sender, Rust sink sees broken channel -> implicit clean termination

**CXX bridge surface & bindings layout:**
- Full crate rename: nes-source-bindings -> nes-io-bindings (directory, Cargo.toml, CMakeLists, all imports)
- Header/class rename: SourceBindings -> IoBindings (or similar)
- sink_send_buffer(handle, buffer_ptr) returns CXX shared enum SendResult { Success, Full, Closed }
- spawn_sink called through CXX bridge, returns Box<SinkTaskHandle> -- mirrors spawn_source pattern
- stop_sink and is_sink_done exposed through same CXX bridge

### Claude's Discretion
- Exact BackpressureHandler state machine internals (can mirror or simplify NetworkSink's)
- Default hysteresis threshold values
- Default channel capacity
- TokioSink class member layout and constructor signature
- Error callback integration details (reuse ErrorContext pattern from sources)
- Whether sink_send_buffer wraps stop_sink for the Close case or keeps them separate

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PLN-01 | TokioSink C++ operator implementing ExecutablePipelineStage (execute/stop) | TokioSource lifecycle pattern (PIMPL, SpawnFn), Sink base class with BackpressureController, ExecutablePipelineStage interface (start/execute/stop) |
| PLN-02 | Non-blocking try_send with repeatTask retry when channel is full | NetworkSink execute() pattern: try_send -> SendResult switch -> repeatTask on Full, BackpressureHandler::onFull/onSuccess |
| PLN-03 | BackpressureController with hysteresis thresholds (apply/release pressure) | NetworkSink's BackpressureHandler class (folly::Synchronized<State>, deque, upper/lower thresholds), BackpressureController::applyPressure/releasePressure |
| PLN-04 | Guaranteed flush on pipeline stop via repeatTask polling until Rust confirms drain | NetworkSink stop() pattern: drain backlog, send Close, poll is_sink_done via repeatTask; TokioSource::tryStop() polling eosProcessed |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| CXX (cxx crate) | (existing) | Rust-C++ FFI bridge for sink functions | Already used for source FFI; shared enum support for SendResult |
| async_channel | (existing) | Bounded channel between C++ pipeline and Rust sink task | Already used for source bridge; try_send for non-blocking C++ side |
| tokio_util | (existing) | CancellationToken for cooperative shutdown | Already used in sink framework (Phase 4) |
| folly::Synchronized | (existing) | Thread-safe state for BackpressureHandler | Already used by NetworkSink's BackpressureHandler |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| Corrosion | (existing) | CMake integration for Rust crates | Building renamed nes-io-bindings crate |
| gtest | (existing) | C++ unit/integration tests | TokioSink operator tests |

## Architecture Patterns

### Recommended Project Structure

Changes are additions/renames to existing structure:

```
nes-sources/
  rust/
    nes-io-bindings/               # RENAMED from nes-source-bindings
      CMakeLists.txt               # Updated target names
      Cargo.toml                   # Updated crate name
      lib.rs                       # Existing Phase 1 bridge (init_source_runtime)
      include/
        IoBindings.hpp             # RENAMED from SourceBindings.hpp
      src/
        IoBindings.cpp             # RENAMED from SourceBindings.cpp + new sink FFI functions
    nes-source-lib/
      src/
        lib.rs                     # Extended: SinkHandle wrapper, CXX bridge for sink FFI
    nes-source-runtime/
      src/
        sink.rs                    # Phase 4 (exists) -- spawn_sink
        sink_handle.rs             # Phase 4 (exists) -- SinkTaskHandle, stop_sink, is_sink_done
        sink_context.rs            # Phase 4 (exists) -- SinkContext, SinkMessage
  include/Sources/
    TokioSink.hpp                  # NEW: C++ TokioSink operator
  src/
    TokioSink.cpp                  # NEW: Implementation
```

### Pattern 1: PIMPL for Rust Handle (from TokioSource)
**What:** TokioSink::RustSinkHandleImpl wraps `rust::Box<SinkHandle>`, defined in .cpp only
**When to use:** Always -- avoids exposing CXX types in public headers
**Example:**
```cpp
// In TokioSink.hpp
class TokioSink final : public Sink {
public:
    struct RustSinkHandleImpl; // Forward declaration only
    // ...
private:
    std::unique_ptr<RustSinkHandleImpl> rustHandle;
};

// In TokioSink.cpp
struct TokioSink::RustSinkHandleImpl {
    rust::Box<::SinkHandle> handle;
    explicit RustSinkHandleImpl(rust::Box<::SinkHandle> h) : handle(std::move(h)) {}
};
```

### Pattern 2: SpawnFn Indirection (from TokioSource)
**What:** TokioSink takes a `SpawnFn` capturing sink config that calls the Rust FFI spawn entry point
**When to use:** To decouple TokioSink from specific sink types
**Example:**
```cpp
using SpawnFn = std::function<std::unique_ptr<RustSinkHandleImpl>(
    uint64_t sink_id, uint32_t channel_capacity,
    uintptr_t error_fn_ptr, uintptr_t error_ctx_ptr)>;
```

### Pattern 3: NetworkSink execute() with BackpressureHandler (from NetworkSink)
**What:** try_send result drives a loop: Ok -> onSuccess (may return next buffered item to send), Full -> onFull (buffer internally, repeatTask the pending retry), Closed -> error
**When to use:** In TokioSink::execute()
**Example:**
```cpp
// Source: NetworkSink.cpp execute() pattern
void TokioSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) {
    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer) {
        // Retain buffer for Rust ownership, send raw pointer through CXX
        currentBuffer->retain();
        auto result = ::sink_send_buffer(*rustHandle->handle,
                          reinterpret_cast<uintptr_t>(&*currentBuffer));
        switch (result) {
            case SendResult::Success:
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                break;
            case SendResult::Full:
                currentBuffer->release(); // Undo retain since send failed
                if (auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController)) {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            case SendResult::Closed:
                currentBuffer->release();
                throw CannotOpenSink("TokioSink channel closed unexpectedly");
        }
    }
}
```

### Pattern 4: NetworkSink stop() with repeatTask Polling (from NetworkSink)
**What:** stop() drains backlog, sends Close, then polls for completion via repeatTask
**When to use:** In TokioSink::stop()
**Example:**
```cpp
void TokioSink::stop(PipelineExecutionContext& pec) {
    if (!stopInitiated) {
        // Drain BackpressureHandler's buffered items into channel
        // (may need repeatTask if channel full during drain)
        drainBacklog(pec);
        stopInitiated = true;
    }
    if (!closeSent) {
        // Try to send Close via stop_sink FFI
        bool sent = ::stop_sink(*rustHandle->handle);
        if (!sent) {
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
        closeSent = true;
    }
    // Poll for Rust sink completion
    if (!::is_sink_done(*rustHandle->handle)) {
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }
    // Sink done -- cleanup complete
}
```

### Pattern 5: CXX Shared Enum for SendResult
**What:** CXX bridge `shared enum` for type-safe result passing between Rust and C++
**When to use:** For sink_send_buffer return value
**Example:**
```rust
#[cxx::bridge]
pub mod ffi_sink {
    #[derive(Debug, PartialEq)]
    enum SendResult {
        Success,
        Full,
        Closed,
    }

    extern "Rust" {
        type SinkHandle;
        fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> SendResult;
        fn spawn_sink_ffi(
            sink_id: u64, channel_capacity: u32,
            error_fn_ptr: usize, error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;
        fn stop_sink_ffi(handle: &SinkHandle) -> bool;
        fn is_sink_done_ffi(handle: &SinkHandle) -> bool;
    }
}
```

### Anti-Patterns to Avoid
- **Blocking C++ worker threads:** Never use `send_blocking()` from execute(). Use `try_send()` + repeatTask.
- **Double-retain on FFI boundary:** C++ retains ONCE before passing raw ptr to Rust. Rust wraps raw ptr without additional retain. On Full, C++ must release the undo-retain.
- **Polling is_sink_done in a spin loop:** Use repeatTask for non-blocking polling, never a tight while loop.
- **Sending data after Close:** TokioSink state machine must prevent execute() from sending after stop() initiated.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hysteresis backpressure | Custom threshold logic | BackpressureHandler class (from NetworkSink pattern) | NetworkSink's BackpressureHandler handles thread-safe buffering, upper/lower thresholds, pending retry tracking. ~80 lines of subtle concurrent state |
| Non-blocking retry | sleep/spin loops | PipelineExecutionContext::repeatTask() | Pipeline-aware retry that yields the worker thread |
| Bounded MPSC channel | Manual ring buffer | async_channel::bounded + try_send | Already proven in source bridge; handles all edge cases |
| Buffer refcount management | Manual retain/release tracking | TupleBufferHandle Drop impl | Existing RAII pattern handles release on drop |
| CXX type-safe enum | uint8_t return codes | CXX shared enum | Type safety across FFI boundary; match exhaustiveness |

**Key insight:** Every major component in Phase 5 has a direct analogue in the existing codebase. TokioSink = TokioSource + NetworkSink patterns combined. The FFI bridge = source bridge extended. The risk is in the integration seams, not in novel components.

## Common Pitfalls

### Pitfall 1: Buffer Double-Retain Across FFI
**What goes wrong:** C++ calls retain() on the buffer AND the Rust side also calls retain (via clone or copy constructor), leaving refcount too high. Buffer is never recycled.
**Why it happens:** The source direction (bridge_emit) uses `TupleBuffer buffer(*rawBuffer)` which calls the copy constructor (implicit retain). If sink code also explicitly calls retain() before passing the pointer, the refcount is incremented twice.
**How to avoid:** C++ calls retain() exactly once before passing the raw pointer. Rust constructs TupleBufferHandle from the raw pointer WITHOUT cloning. On TupleBufferHandle drop, release() is called once. On Full result, C++ calls release() to undo the retain since the buffer was not transferred.
**Warning signs:** Buffer pool exhaustion under sustained load; getReferenceCounter() returns values higher than expected.

### Pitfall 2: repeatTask Re-entrance During Stop
**What goes wrong:** A buffer send was repeatTask'd due to Full channel. Between the retry and execution, stop() is called. The retried execute() now finds the sink in a stopping state and either sends data after Close or panics.
**Why it happens:** repeatTask is asynchronous -- the task is re-enqueued and may execute after state transitions.
**How to avoid:** TokioSink must check a `stopInitiated` flag at the top of execute(). If stopping, drop the buffer (the pipeline is shutting down; data loss is acceptable at this point). The flag must be set atomically before any Close is sent.
**Warning signs:** Data arriving at the Rust sink after Close message; non-deterministic test failures.

### Pitfall 3: Backlog Drain Ordering During Stop
**What goes wrong:** stop() needs to drain BackpressureHandler's internal deque into the channel before sending Close. But the channel may be full during drain, requiring repeatTask. If Close is sent before all backlog items are drained, those items are lost.
**How to avoid:** stop() must be a multi-phase state machine: (1) drain backlog (repeatTask if channel full during drain), (2) send Close (repeatTask if channel full), (3) poll is_sink_done (repeatTask until true). Each phase is gated by a flag so repeatTask re-entrance advances through phases correctly.
**Warning signs:** Data loss on shutdown; backpressureHandler.empty() assertion failure.

### Pitfall 4: Crate Rename Breaking Includes
**What goes wrong:** Renaming nes-source-bindings to nes-io-bindings breaks CXX-generated header paths (e.g., `#include <nes-source-bindings/lib.h>` becomes `#include <nes-io-bindings/lib.h>`), CMake target names, Cargo.toml dependencies, and all Rust `use` statements.
**Why it happens:** The crate name appears in CXX-generated include paths, CMake target names, and Rust module paths. Missing one breaks the build.
**How to avoid:** Enumerate ALL references before renaming: CMakeLists.txt (corrosion_add_cxxbridge target, target_link_libraries), Cargo.toml (crate name, dependencies), Rust use/extern crate, C++ #include paths, and any build scripts. Do the rename as a single atomic change.
**Warning signs:** Build failures mentioning missing headers or unresolved symbols.

### Pitfall 5: TupleBufferHandle Construction from Raw Pointer
**What goes wrong:** The current TupleBufferHandle::new() takes a `cxx::UniquePtr<ffi::TupleBuffer>`. For sinks, we need to construct a TupleBufferHandle from a raw `*mut ffi::TupleBuffer` that C++ already retained. Creating a UniquePtr from a raw pointer would transfer ownership, but the refcount was already incremented by retain().
**Why it happens:** TupleBufferHandle was designed for source direction where Rust allocates buffers via getBufferBlocking(). Sinks receive pre-retained raw pointers from C++.
**How to avoid:** Add a `TupleBufferHandle::from_retained_raw(ptr: *mut ffi::TupleBuffer) -> Self` constructor that wraps the raw pointer in a UniquePtr without additional retain. The existing Drop impl calls release(), matching the single retain from C++. Alternatively, use the existing `cloneTupleBuffer` approach from bridge_emit (C++ copies the buffer, giving Rust a UniquePtr; the copy constructor handles retain).
**Warning signs:** Compile errors when trying to construct TupleBufferHandle from raw pointer; double-free or leak if ownership is wrong.

## Code Examples

### CXX Bridge Extension for Sink FFI

```rust
// In nes-source-lib/src/lib.rs (extended ffi_source module or new ffi_sink module)
// Source: pattern from existing ffi_source bridge

#[cxx::bridge]
pub mod ffi_sink {
    #[derive(Debug, PartialEq)]
    enum SendResult {
        Success,
        Full,
        Closed,
    }

    unsafe extern "C++" {
        include!("IoBindings.hpp");
    }

    extern "Rust" {
        type SinkHandle;

        fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> SendResult;
        fn stop_sink_bridge(handle: &SinkHandle) -> bool;
        fn is_sink_done_bridge(handle: &SinkHandle) -> bool;

        fn spawn_sink_bridge(
            sink_id: u64,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;
    }
}
```

### sink_send_buffer Implementation (Rust)

```rust
// In nes-source-lib/src/lib.rs
// Source: mirrors SinkTaskHandle::sender().try_send() from Phase 4

fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> ffi_sink::SendResult {
    // Reconstruct the pre-retained TupleBuffer pointer
    let raw_ptr = buffer_ptr as *mut nes_source_bindings::ffi::TupleBuffer;

    // Wrap in UniquePtr -- C++ already called retain(), so Drop will call release()
    // SAFETY: C++ guarantees the pointer is valid and retained
    let unique = unsafe { cxx::UniquePtr::from_raw(raw_ptr) };
    let buf_handle = nes_source_runtime::TupleBufferHandle::new(unique);

    match handle.inner.sender().try_send(SinkMessage::Data(buf_handle)) {
        Ok(()) => ffi_sink::SendResult::Success,
        Err(async_channel::TrySendError::Full(msg)) => {
            // IMPORTANT: Must NOT drop the buffer here -- it would call release()
            // but C++ expects to undo the retain itself on Full.
            // Leak the TupleBufferHandle to prevent drop/release.
            std::mem::forget(msg);
            ffi_sink::SendResult::Full
        }
        Err(async_channel::TrySendError::Closed(_msg)) => {
            std::mem::forget(_msg);
            ffi_sink::SendResult::Closed
        }
    }
}
```

### TokioSink C++ Header

```cpp
// Source: mirrors TokioSource.hpp + Sink base class pattern

class TokioSink final : public Sink {
public:
    struct RustSinkHandleImpl;

    using SpawnFn = std::function<std::unique_ptr<RustSinkHandleImpl>(
        uint64_t sink_id, uint32_t channel_capacity,
        uintptr_t error_fn_ptr, uintptr_t error_ctx_ptr)>;

    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    TokioSink(BackpressureController backpressureController,
              SpawnFn spawnFn,
              uint32_t channelCapacity,
              size_t upperThreshold,
              size_t lowerThreshold);
    ~TokioSink() override;

    void start(PipelineExecutionContext& pec) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    SpawnFn spawnFn;
    uint32_t channelCapacity;
    std::unique_ptr<RustSinkHandleImpl> rustHandle;
    BackpressureHandler backpressureHandler;
    std::unique_ptr<ErrorContext> errorContext;
    std::atomic<bool> stopInitiated{false};
    bool closeSent{false};
    bool started{false};
};
```

### Buffer Refcount Sequence (Critical Documentation)

```
execute(buffer) called on C++ pipeline worker thread:
  1. buffer arrives with refcount = N (pipeline's copy)
  2. C++ calls buffer.retain()                    -> refcount = N+1
  3. C++ calls sink_send_buffer(handle, raw_ptr)  -> passes raw ptr
  4. [If Success]: Rust wraps raw ptr in TupleBufferHandle (no additional retain)
     - Pipeline eventually releases its copy       -> refcount = N
     - Rust sink processes buffer, drops handle    -> release() -> refcount = N-1
  5. [If Full]: Rust does NOT take ownership (std::mem::forget)
     - C++ calls buffer.release() to undo step 2  -> refcount = N
     - Buffer goes into BackpressureHandler backlog for retry
  6. [If Closed]: Same as Full -- undo retain, then throw
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| uint8_t return codes for FFI | CXX shared enums (SendResult) | CXX 1.0+ | Type-safe matching on both sides; exhaustiveness checking |
| Separate bridge thread for each direction | Per-sink channels with direct try_send | v1.1 design | No bridge thread needed for sinks; simpler topology |
| Source-only bindings crate | Shared I/O bindings crate (nes-io-bindings) | Phase 5 rename | Single crate for source+sink FFI; shared runtime |

## Open Questions

1. **UniquePtr::from_raw for pre-retained buffers**
   - What we know: CXX provides `UniquePtr::from_raw()` but it assumes ownership of the underlying object. For a pre-retained TupleBuffer, the UniquePtr destructor would call the C++ destructor, not release(). TupleBufferHandle::Drop explicitly calls ffi::release().
   - What's unclear: Whether wrapping a raw retained TupleBuffer* in UniquePtr followed by TupleBufferHandle::Drop(release) is the correct ownership chain, or if a separate `from_raw_retained` constructor is needed.
   - Recommendation: Use the `cloneTupleBuffer` approach from bridge_emit instead. C++ creates a copy of the buffer (copy constructor calls retain), wraps it in UniquePtr, passes that to Rust. This is proven to work. The retain/release is handled by copy constructor + TupleBufferHandle::Drop. Avoid `UniquePtr::from_raw` entirely.

2. **BackpressureHandler extraction vs duplication**
   - What we know: BackpressureHandler is ~80 lines in NetworkSink.hpp/cpp. TokioSink needs identical logic.
   - What's unclear: Whether extracting to a shared header is worth the cross-module dependency.
   - Recommendation: Duplicate the BackpressureHandler in TokioSink.cpp for now. It is self-contained and avoids modifying NetworkSink's build. Can be extracted later if a third sink type appears.

3. **Stop draining backlog when channel is full**
   - What we know: stop() must drain BackpressureHandler's buffered items into the channel before sending Close. The channel may be full.
   - What's unclear: The exact interaction between drain repeatTask and is_sink_done polling in a single stop() method that is called repeatedly.
   - Recommendation: Use a 3-phase state machine in stop(): `DRAINING -> CLOSING -> WAITING_DONE`. Each repeatTask re-entrance checks which phase and advances. DRAINING pops from backlog and try_sends; CLOSING sends Close; WAITING_DONE polls is_sink_done.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | gtest (C++), cargo test (Rust) |
| Config file | CMakeLists.txt (add_tests_if_enabled), Cargo.toml |
| Quick run command | `cargo test -p nes-source-runtime --lib` (Rust) |
| Full suite command | Build target `nes-sources-tests` (C++) + `cargo test` (Rust) |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PLN-01 | TokioSink implements start/execute/stop | integration | Build + run TokioSinkTest (C++) | No -- Wave 0 |
| PLN-02 | Non-blocking try_send with repeatTask retry | integration | TokioSinkTest::BackpressureRetry | No -- Wave 0 |
| PLN-03 | BackpressureController hysteresis thresholds | unit | TokioSinkTest::HysteresisThresholds | No -- Wave 0 |
| PLN-04 | Flush-confirm on stop via repeatTask polling | integration | TokioSinkTest::FlushOnStop | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test -p nes-source-runtime --lib` (Rust changes)
- **Per wave merge:** Full C++ build + test suite
- **Phase gate:** All TokioSinkTest cases pass, cargo test passes

### Wave 0 Gaps
- [ ] `nes-sources/tests/TokioSinkTest.cpp` -- covers PLN-01 through PLN-04
- [ ] Rust unit test for `sink_send_buffer` FFI wrapper (in nes-source-lib or nes-source-runtime)
- [ ] Test fixture: mock PipelineExecutionContext with repeatTask recording (may already exist in TokioSourceTest)

## Sources

### Primary (HIGH confidence)
- Direct codebase analysis: `nes-plugins/Sinks/NetworkSink/NetworkSink.hpp` -- BackpressureHandler class, hysteresis thresholds, BACKPRESSURE_RETRY_INTERVAL
- Direct codebase analysis: `nes-plugins/Sinks/NetworkSink/NetworkSink.cpp` -- execute() try_send + repeatTask pattern, stop() flush polling
- Direct codebase analysis: `nes-sources/include/Sources/TokioSource.hpp` -- PIMPL pattern, SpawnFn type, lifecycle interface
- Direct codebase analysis: `nes-sources/src/TokioSource.cpp` -- RustHandleImpl, start/stop/tryStop implementation
- Direct codebase analysis: `nes-sources/rust/nes-source-lib/src/lib.rs` -- CXX bridge for source FFI, SourceHandle wrapper pattern
- Direct codebase analysis: `nes-sources/rust/nes-source-bindings/lib.rs` -- CXX bridge for init_source_runtime, TupleBuffer FFI
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/sink.rs` -- spawn_sink, AsyncSink trait (Phase 4)
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/sink_handle.rs` -- SinkTaskHandle, stop_sink, is_sink_done (Phase 4)
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/sink_context.rs` -- SinkContext, SinkMessage enum
- Direct codebase analysis: `nes-sources/rust/nes-source-runtime/src/buffer.rs` -- TupleBufferHandle retain/release, as_raw_ptr
- Direct codebase analysis: `nes-executable/include/ExecutablePipelineStage.hpp` -- start/execute/stop interface
- Direct codebase analysis: `nes-executable/include/PipelineExecutionContext.hpp` -- repeatTask signature
- Direct codebase analysis: `nes-executable/include/BackpressureChannel.hpp` -- BackpressureController::applyPressure/releasePressure
- Direct codebase analysis: `nes-sinks/include/Sinks/Sink.hpp` -- Sink base class with BackpressureController member

### Secondary (MEDIUM confidence)
- `.planning/research/ARCHITECTURE.md` -- sink architecture analysis (pre-Phase 4, some details evolved)
- `.planning/research/PITFALLS.md` -- comprehensive pitfall catalog (still accurate for Phase 5)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in use, no new dependencies
- Architecture: HIGH -- every pattern has a direct analogue in existing codebase (TokioSource, NetworkSink)
- Pitfalls: HIGH -- v1.0 post-mortem lessons + detailed codebase analysis of buffer lifecycle, backed by PITFALLS.md research
- Crate rename: MEDIUM -- mechanical but touches many files; exact CXX include path behavior after rename needs build verification

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable -- no external dependency changes expected)
