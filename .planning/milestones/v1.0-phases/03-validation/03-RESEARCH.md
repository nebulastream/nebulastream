# Phase 3: Validation - Research

**Researched:** 2026-03-15
**Domain:** Rust async source validation, integration testing, backpressure/shutdown invariants
**Confidence:** HIGH

## Summary

Phase 3 validates the complete Rust source framework built in Phases 1-2. The work is entirely within the existing codebase: implementing a concrete `GeneratorSource` that exercises the `AsyncSource` trait, then writing integration tests that prove correctness of the end-to-end pipeline, backpressure stability, and shutdown safety.

The key technical challenge is the **two-tier test architecture** established in Phases 1-2. Pure Rust unit tests (`cargo test`) use `cfg(test)` stubs that replace `BridgeMessage` with `Sender<()>` to avoid CXX linker dependencies. Integration tests that exercise the full Rust-to-C++ pipeline must run under CMake/CTest where the C++ library is linked. Phase 3 must decide which tests belong in each tier.

**Primary recommendation:** Implement `GeneratorSource` in `nes-source-runtime` with `cfg(test)` Rust-side tests for the source logic, then add a C++ GTest integration test (under `nes-sources/tests/`) that creates a real `TokioSource` with `BufferManager`, exercises the full emit pipeline, and verifies buffer ordering, backpressure stability, and shutdown safety.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| VAL-01 | Generator test source using async sleep loop that emits incrementing values | GeneratorSource implements AsyncSource with `tokio::time::sleep` loop, writes u64 counter into TupleBuffer bytes, uses `ctx.allocate_buffer()` + `ctx.emit(buf).await` + `ctx.cancellation_token()` |
| VAL-02 | End-to-end integration test: Rust source -> bridge -> task queue -> pipeline execution | C++ GTest creates TokioSource with BufferManager, starts it with a RecordingEmitFunction, verifies buffer contents arrive in order. Backpressure and shutdown tests verify invariants from success criteria 3 and 4 |
</phase_requirements>

## Standard Stack

### Core (already in workspace)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.40.0+ | Async runtime, time::sleep, sync::Semaphore | Already configured in workspace |
| tokio-util | 0.7.13 | CancellationToken | Already configured |
| async-channel | 2.3.1 | Bridge channel | Already configured |
| cxx | 1.0 | C++/Rust FFI | Already configured |
| dashmap | 6.1 | EMIT_REGISTRY, BACKPRESSURE_REGISTRY | Already configured |

### Testing
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tokio (test-util) | 1.40.0+ | `#[tokio::test]`, time control | Already in dev-dependencies |
| gtest/gmock | (CMake) | C++ integration tests | Full pipeline tests under CMake |

### No New Dependencies
Phase 3 requires zero new crate or library additions. Everything needed is already in the workspace.

## Architecture Patterns

### GeneratorSource Location and Structure

GeneratorSource belongs in `nes-source-runtime/src/source.rs` (alongside existing test sources) as a `#[cfg(test)]` item for pure Rust tests, but ALSO needs a non-test version for the C++ integration test.

**Recommended approach:** Define GeneratorSource in a new file `nes-source-runtime/src/generator.rs` without `cfg(test)` gating so it is available to both Rust tests and production code (the C++ integration test calls `spawn_source` with it). Gate the module inclusion as `pub mod generator;` in lib.rs.

```
nes-sources/rust/
  nes-source-runtime/src/
    generator.rs        # NEW: GeneratorSource implementation
    lib.rs              # ADD: pub mod generator;
  nes-source-lib/src/
    lib.rs              # MODIFY: wire GeneratorSource into spawn_source dispatch
```

### Pattern: GeneratorSource Implementation

```rust
// generator.rs
use crate::context::SourceContext;
use crate::error::SourceResult;
use crate::source::AsyncSource;

/// Test/validation source that emits incrementing u64 values.
///
/// Each iteration: allocate buffer, write counter as little-endian u64 bytes,
/// set number_of_tuples to 1, emit, increment counter, sleep.
pub struct GeneratorSource {
    /// Number of values to emit before returning EndOfStream.
    count: u64,
    /// Sleep duration between emissions.
    interval: std::time::Duration,
}

impl GeneratorSource {
    pub fn new(count: u64, interval: std::time::Duration) -> Self {
        Self { count, interval }
    }
}

impl AsyncSource for GeneratorSource {
    async fn run(&mut self, ctx: SourceContext) -> SourceResult {
        let cancel = ctx.cancellation_token();
        for i in 0..self.count {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return SourceResult::EndOfStream;
                }
                _ = async {
                    let mut buf = ctx.allocate_buffer().await;
                    // Write counter value as little-endian u64
                    let bytes = i.to_le_bytes();
                    buf.as_mut_slice()[..8].copy_from_slice(&bytes);
                    buf.set_number_of_tuples(1);
                    if let Err(_) = ctx.emit(buf).await {
                        return; // channel closed during shutdown
                    }
                    tokio::time::sleep(self.interval).await;
                } => {}
            }
        }
        SourceResult::EndOfStream
    }
}
```

### Pattern: Source Type Dispatch in nes-source-lib

Currently `spawn_source` in `nes-source-lib/src/lib.rs` uses `PlaceholderSource`. For Phase 3, this needs to support `GeneratorSource`. The simplest approach is to add a source_type parameter or use a separate FFI entry point for the generator.

**Recommended:** Add a `spawn_generator_source(source_id, count, interval_ms, ...)` FFI function alongside the existing `spawn_source`. This avoids complicating the generic spawn_source with type dispatch logic that will be redesigned when real source types are added.

### Pattern: C++ Integration Test Structure

Follow the existing `SourceThreadTest.cpp` pattern:

```cpp
class TokioSourceTest : public Testing::BaseUnitTest {
    // BufferManager, RecordingEmitFunction, etc.
};

TEST_F(TokioSourceTest, GeneratorEmitsIncrementingValues) {
    // 1. init_source_runtime(2)
    // 2. Create BufferManager
    // 3. Create TokioSource with originId, inflightLimit, bufferProvider
    // 4. Start with RecordingEmitFunction
    // 5. Wait for N emits
    // 6. Verify buffer contents (u64 counter values in order)
    // 7. Stop source
}
```

### Two-Tier Test Architecture

| Test Tier | Where | What It Tests | Linker |
|-----------|-------|---------------|--------|
| Rust unit (`cargo test`) | `nes-source-runtime/src/*.rs` | GeneratorSource logic with cfg(test) emit stub, backpressure/semaphore gating | No C++ linker |
| C++ integration (CTest) | `nes-sources/tests/TokioSourceTest.cpp` | Full pipeline: Rust -> bridge -> C++ emit callback -> verify buffers | Full C++ + Rust linking |

**Critical constraint:** The `cfg(test)` emit path in SourceContext sends `()` not `BridgeMessage`. Rust-side tests can verify that GeneratorSource calls emit the correct number of times and respects cancellation, but cannot verify buffer contents arrive in C++. That is the C++ integration test's job.

### Anti-Patterns to Avoid

- **Testing buffer contents in `cargo test`:** The `cfg(test)` SourceContext drops the buffer on emit (sends `()` instead). Buffer content verification MUST happen in the C++ integration test.
- **Using `std::thread::sleep` in GeneratorSource:** Must use `tokio::time::sleep` to avoid blocking Tokio worker threads.
- **Unbounded emission without sleep/yield:** Even in tests, a tight emit loop without yielding can starve the Tokio runtime. Always include at least a `tokio::task::yield_now()`.
- **Forgetting `tokio::select!` with cancellation:** The source loop must check cancellation on every iteration, not just at the top.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Buffer content encoding | Custom serialization | Direct `u64::to_le_bytes()` into `as_mut_slice()` | Simple, deterministic, no alignment issues |
| Waiting for N emits in C++ test | Custom polling loop | `RecordingEmitFunction` + `wait_for_emits()` pattern from SourceThreadTest | Already proven, handles timeouts |
| Buffer pool for tests | Mock buffer provider | `BufferManager::create()` | Real buffer manager needed for integration test |
| Backpressure channel for tests | Mock | `createBackpressureChannel()` | Factory already exists in test utils |

## Common Pitfalls

### Pitfall 1: CXX Linker Symbols in cargo test
**What goes wrong:** Adding GeneratorSource code that references `BridgeMessage` or `TupleBufferHandle::drop()` in test builds causes linker errors.
**Why it happens:** `cfg(test)` stubs replace `BridgeMessage` channel with `Sender<()>`, but if GeneratorSource is compiled with test cfg and calls emit() which constructs a TupleBufferHandle, Drop needs C++ symbols.
**How to avoid:** In Rust-side tests, use the existing `spawn_test_source` pattern with a null BufferProviderHandle. The test SourceContext's emit() accepts TupleBufferHandle but drops it without calling C++ release (because the handle was created from null).
**Warning signs:** `undefined reference to` errors mentioning `retain`, `release`, `cloneTupleBuffer`.

### Pitfall 2: Test Source ID Collisions
**What goes wrong:** Tests that use the same source_id corrupt global registries (EMIT_REGISTRY, BACKPRESSURE_REGISTRY).
**Why it happens:** DashMap registries are process-global. Parallel tests with the same ID overwrite each other's entries.
**How to avoid:** Use unique source_id values per test (existing tests use 20001, 20002, etc.). For Phase 3, use 30001+.

### Pitfall 3: Shutdown Race in Backpressure Test
**What goes wrong:** Backpressure test applies pressure, source blocks, test releases pressure and immediately asserts memory -- but the source hasn't had time to drain.
**Why it happens:** Async task scheduling is non-deterministic.
**How to avoid:** Use the `wait_for_emits` pattern with a timeout. After releasing backpressure, wait for expected emits before asserting.

### Pitfall 4: Semaphore Permit Leak Detection
**What goes wrong:** Test asserts `semaphore.available_permits() == capacity` but the monitoring task hasn't completed cleanup yet.
**Why it happens:** After source task exits, the monitoring task runs cleanup asynchronously. The semaphore permits are released when BridgeMessage is dropped by the bridge, but in test builds the bridge may not be running.
**How to avoid:** In Rust tests, the `cfg(test)` emit drops permits immediately (the `_permit` in the unit sender path). For the C++ integration test, wait for the source to fully stop (monitoring task cleanup) before checking permits.

### Pitfall 5: init_source_runtime Must Be Called Before Tests
**What goes wrong:** `source_runtime()` panics with "not initialized".
**Why it happens:** C++ integration tests must call `init_source_runtime` via the CXX bridge before creating any TokioSource.
**How to avoid:** Call `init_source_runtime(2)` in test SetUp or SetUpTestSuite. Rust-side tests use `#[tokio::test]` which creates its own runtime (they don't go through `source_runtime()`).

## Code Examples

### Verified: Existing AsyncSource Implementation Pattern
```rust
// Source: nes-sources/rust/nes-source-runtime/src/source.rs (lines 205-209)
struct TestEndOfStreamSource;
impl AsyncSource for TestEndOfStreamSource {
    async fn run(&mut self, _ctx: SourceContext) -> SourceResult {
        SourceResult::EndOfStream
    }
}
```

### Verified: Cancellation Pattern
```rust
// Source: nes-sources/rust/nes-source-runtime/src/source.rs (lines 214-219)
struct TestCancellableSource;
impl AsyncSource for TestCancellableSource {
    async fn run(&mut self, ctx: SourceContext) -> SourceResult {
        ctx.cancellation_token().cancelled().await;
        SourceResult::EndOfStream
    }
}
```

### Verified: spawn_test_source Pattern
```rust
// Source: nes-sources/rust/nes-source-runtime/src/source.rs (lines 242-281)
// Uses null BufferProviderHandle, closed dummy channel, no C++ linker needed
fn spawn_test_source(source_id: u64, source: impl AsyncSource, ...) -> Box<SourceTaskHandle> {
    let buffer_provider = unsafe { BufferProviderHandle::from_raw(std::ptr::null_mut()) };
    let (sender, _receiver) = async_channel::bounded::<()>(1);
    let ctx = SourceContext::new(buffer_provider, sender, ...);
    // ...
}
```

### Verified: C++ RecordingEmitFunction Pattern
```cpp
// Source: nes-sources/tests/SourceThreadTest.cpp (lines 74-99)
struct RecordingEmitFunction {
    void operator()(const OriginId, SourceReturnType::SourceReturnType emittedData) {
        // Copies buffer into local buffer manager to avoid dangling refs
        auto value = std::visit([&]<typename T>(T emitted) -> ... { ... }, emittedData);
        recordedEmits.lock()->emplace_back(std::move(value));
        block.notify_one();
    }
    AbstractBufferProvider& bm;
    folly::Synchronized<std::vector<...>> recordedEmits;
    std::condition_variable block;
};
```

### Verified: TokioSource Start/Stop
```cpp
// Source: nes-sources/src/TokioSource.cpp
// start() creates EmitContext + ErrorContext, calls spawn_source FFI
// stop() calls stop_source FFI (non-blocking, cancels CancellationToken)
```

### Verified: Buffer Data Access
```rust
// Source: nes-sources/rust/nes-source-runtime/src/buffer.rs (lines 35-47)
pub fn as_mut_slice(&mut self) -> &mut [u8] { ... }
pub fn as_slice(&self) -> &[u8] { ... }
pub fn set_number_of_tuples(&mut self, count: u64) { ... }
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| PlaceholderSource (EndOfStream immediately) | GeneratorSource (emits real data) | Phase 3 | Validates full pipeline with real buffer contents |
| Test-only stub channels | Same (keep cfg(test) stubs) | Phase 1-2 | Rust unit tests remain free of C++ linker |
| No C++ integration test for Tokio sources | TokioSourceTest.cpp | Phase 3 | First end-to-end validation of Rust->C++ pipeline |

## Key Design Decisions for Planner

### 1. GeneratorSource: Finite vs Infinite
GeneratorSource should accept a `count` parameter for the number of values to emit. This makes tests deterministic -- the source emits exactly N values then returns EndOfStream. Tests that want to exercise shutdown mid-emission can set count to a very large number and call stop() during execution.

### 2. spawn_generator_source FFI Entry Point
Rather than adding a type dispatch enum to the existing `spawn_source`, add a dedicated `spawn_generator_source(source_id, count, interval_ms, buffer_provider_ptr, inflight_limit, emit_fn_ptr, emit_ctx_ptr, error_fn_ptr, error_ctx_ptr)` function in `nes-source-lib`. This is cleaner for Phase 3 and the type dispatch pattern will be redesigned later when real source types (Kafka, etc.) are added.

### 3. Where to Put the C++ Integration Test
Add `TokioSourceTest.cpp` to `nes-sources/tests/` and register it via `add_nes_source_test(tokio-source-test TokioSourceTest.cpp)` in the tests CMakeLists.txt. This follows the exact pattern of `SourceThreadTest.cpp`.

### 4. Backpressure Regression Test Strategy
The success criteria calls for verifying "memory usage stays stable (no unbounded buffer accumulation)." In the C++ integration test:
- Create a TokioSource with a small inflight limit (e.g., 2)
- Use a slow emit function (blocks for a few ms)
- Let the generator run for many iterations
- Verify semaphore never goes negative and available permits recover
This tests the invariant without needing actual memory measurement -- the semaphore IS the memory bound.

### 5. Shutdown Race Test Strategy
The success criteria calls for "stop() while mid-emit, zero leaked semaphore permits."
- Start GeneratorSource with large count and short interval
- Wait until some buffers are emitted (source is actively emitting)
- Call stop()
- Wait for source to fully exit
- Verify semaphore available_permits equals initial capacity

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework (Rust) | tokio::test (already configured) |
| Framework (C++) | GTest/GMock (already configured via CMake) |
| Config file | `nes-sources/rust/nes-source-runtime/Cargo.toml` (dev-deps), `nes-sources/tests/CMakeLists.txt` |
| Quick run command | `cd nes-sources/rust && cargo test -p nes_source_runtime` |
| Full suite command | CMake build + `ctest --test-dir <build> -R tokio-source-test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VAL-01 | GeneratorSource compiles, emits N u64 values via ctx.allocate_buffer + ctx.emit | unit (Rust) | `cd nes-sources/rust && cargo test -p nes_source_runtime generator` | Wave 0 |
| VAL-01 | GeneratorSource respects cancellation mid-loop | unit (Rust) | `cd nes-sources/rust && cargo test -p nes_source_runtime generator` | Wave 0 |
| VAL-02 | End-to-end: buffers arrive at C++ sink in correct order | integration (C++) | `ctest -R tokio-source-test` | Wave 0 |
| VAL-02 | Backpressure regression: memory stays stable under sustained emission | integration (C++) | `ctest -R tokio-source-test` | Wave 0 |
| VAL-02 | Shutdown race: stop mid-emit, zero leaked permits | integration (C++) | `ctest -R tokio-source-test` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cd nes-sources/rust && cargo test -p nes_source_runtime`
- **Per wave merge:** Full CMake build + `ctest -R tokio-source-test`
- **Phase gate:** All Rust tests pass + all C++ integration tests pass

### Wave 0 Gaps
- [ ] `nes-sources/rust/nes-source-runtime/src/generator.rs` -- GeneratorSource implementation
- [ ] `nes-sources/tests/TokioSourceTest.cpp` -- C++ integration test file
- [ ] `nes-sources/tests/CMakeLists.txt` -- add `add_nes_source_test(tokio-source-test TokioSourceTest.cpp)`
- [ ] `nes-source-lib/src/lib.rs` -- `spawn_generator_source` FFI entry point + CXX bridge declaration

## Open Questions

1. **EmitFunction signature adaptation**
   - What we know: The `RecordingEmitFunction` in SourceThreadTest takes `(OriginId, SourceReturnType, stop_token)` and returns `EmitResult`. The C++ `bridge_emit` already implements this correctly.
   - What's unclear: Whether the `RecordingEmitFunction` pattern from SourceThreadTest works unchanged with TokioSource (it should, since bridge_emit adapts the Rust bridge to the same EmitFunction interface).
   - Recommendation: Reuse RecordingEmitFunction as-is. The bridge_emit C function already wraps the EmitFunction correctly.

2. **init_source_runtime idempotency in test suite**
   - What we know: `init_source_runtime` uses OnceLock, so multiple calls are no-ops.
   - What's unclear: Whether other tests in the suite might already call it with a different thread count.
   - Recommendation: Call in `SetUpTestSuite` with a reasonable count (2). If already initialized, it's a no-op.

## Sources

### Primary (HIGH confidence)
- Project source code: direct reading of all Rust and C++ files in nes-sources/
- Phase 1-2 summaries and research documents
- Existing test patterns in SourceThreadTest.cpp and source.rs

### Secondary (MEDIUM confidence)
- None needed -- this is implementation-only work within existing codebase patterns

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - no new dependencies, all already in workspace
- Architecture: HIGH - follows established patterns from Phase 1-2
- Pitfalls: HIGH - derived from direct analysis of cfg(test) stubs and CXX linker constraints
- Test strategy: HIGH - follows existing SourceThreadTest.cpp patterns exactly

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable -- this is project-internal validation work)
