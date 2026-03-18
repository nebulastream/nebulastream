# Phase 1: FFI Foundation - Research

**Researched:** 2026-03-15
**Domain:** Rust/C++ FFI via cxx, Tokio runtime initialization, safe wrapper types
**Confidence:** HIGH

## Summary

Phase 1 establishes the safe Rust/C++ interop layer that all higher-level source code builds upon. The project already has a proven pattern in `nes-network/` -- a Cargo workspace with a bindings crate using `cxx`, a logic crate, and a `staticlib` crate linked via Corrosion in CMake. This phase replicates that pattern for sources with three sub-crates (`nes-source-bindings`, `nes-source-runtime`, `nes-sources`), plus a `nes-source-lib` staticlib.

The core technical challenges are: (1) wrapping C++ `TupleBuffer` retain/release reference counting in Rust `Clone`/`Drop`, (2) wrapping `AbstractBufferProvider` (concretely `BufferManager`) for async buffer allocation, (3) initializing a dedicated Tokio multi-threaded runtime via `OnceLock`, and (4) exposing a clean `SourceContext` API that hides all FFI details. The C++ `BufferManager` is thread-safe (uses `folly::MPMCQueue` and atomics), which validates the `unsafe impl Send` for `BufferProviderHandle`.

**Primary recommendation:** Mirror the `nes-network` crate structure exactly. Use cxx free functions for TupleBuffer operations, `OnceLock` for runtime init, and `tokio::task::spawn_blocking` to bridge the blocking `getBufferBlocking()` C++ call into async Rust.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- New Cargo workspace under `nes-sources/rust/` mirroring the nes-network pattern
- Three sub-crates: `nes-source-bindings` (cxx bridge), `nes-source-runtime` (Tokio runtime, SourceContext, AsyncSource trait), `nes-sources` (concrete implementations, Phase 3+)
- Single `nes-source-lib` staticlib crate that re-exports bindings + runtime -- CMake links this one .a file
- Source bindings are fully independent from network bindings -- no cross-dependency, separate cxx bridge declarations
- CMake integration follows the nes-network corrosion-based approach; if duplicate symbol issues arise during linking, fall back to per-executable purpose-built staticlibs
- Separate dedicated Tokio multi-threaded runtime for sources, independent from network layer runtimes
- Thread count configured via WorkerConfiguration field, passed through FFI during init
- Runtime initialized via OnceLock -- first caller wins, subsequent calls with different thread count log a `tracing::warn`
- No explicit shutdown FFI -- sources are shut down via query engine; runtime cleaned up on process exit
- Thread name prefix: `nes-source-rt`
- `allocate_buffer().await` -- async, awaits until a buffer is available
- `emit(buf).await` -- async, handles backpressure internally
- `run()` returns `SourceResult` enum: `EndOfStream` or `Error(SourceError)`
- Cancellation token exposed directly via `ctx.cancellation_token()`
- Buffer access is opaque byte-slice: `buf.as_mut_slice()` returns `&mut [u8]`, plus `set_number_of_tuples(n)`
- TupleBuffer: opaque cxx type with free functions (retain, release, getDataPtr, getCapacity, getNumberOfTuples, setNumberOfTuples)
- TupleBufferHandle: safe Rust wrapper with Clone (calls retain) and Drop (calls release), wraps UniquePtr
- TupleBufferHandle is `Send` only (not `Sync`)
- BufferProviderHandle: `unsafe impl Send`
- Panic handling: trust cxx's built-in panic-to-exception conversion. Manual catch_unwind only if raw `extern "C"` functions are added outside cxx

### Claude's Discretion
- Exact cxx bridge function signatures (may need adjustment based on what cxx supports for these C++ types)
- Internal SourceContext struct layout
- Error type design for SourceError
- tracing instrumentation details
- Exact Cargo.toml dependency versions (match nes-network where applicable)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FFI-01 | Safe Rust wrapper around TupleBuffer with retain/release in Clone/Drop | TupleBuffer C++ API confirmed: `retain()` returns `TupleBuffer&`, `release()` is void, `getReferenceCounter()` available for testing. Wrapper uses cxx `UniquePtr` with free function bridge. |
| FFI-02 | Safe Rust wrapper around AbstractBufferProvider for buffer allocation | `BufferManager` (concrete provider) uses `folly::MPMCQueue` + atomics -- thread-safe. `getBufferBlocking()` blocks the thread, so Rust side must use `spawn_blocking` for async wrapper. |
| FFI-03 | catch_unwind on all FFI-exposed Rust functions to prevent panic-abort across boundary | cxx automatically catches panics in `extern "Rust"` functions returning `Result` (converts to C++ exception). For non-Result functions, cxx aborts on panic. Existing panic hook in spdlog/lib.rs calls `process::exit(1)`. Decision: trust cxx, add `catch_unwind` only for raw `extern "C"` if any. |
| FFI-04 | cxx bridge declarations for source-related C++ types (TupleBuffer, BufferProvider, BackpressureListener) | C++ types confirmed in headers. TupleBuffer has retain/release/getBufferSize/getNumberOfTuples/setNumberOfTuples. AbstractBufferProvider has getBufferBlocking/getBufferNoBlocking. BackpressureListener has wait(stop_token). |
| FWK-01 | Shared Tokio runtime configured via WorkerConfiguration (thread count) | Network layer pattern confirmed: `tokio::runtime::Builder::new_multi_thread()` with `worker_threads()`, `thread_name()`. Use `OnceLock` (not `LazyLock` which network uses) for init-once semantics with thread count parameter. |
| FWK-03 | SourceContext type exposing only allocate_buffer() and emit(buffer).await | SourceContext holds BufferProviderHandle + async_channel::Sender + CancellationToken. `allocate_buffer()` uses `spawn_blocking` to call C++ `getBufferBlocking()`. `emit()` sends through bounded async_channel. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| cxx | 1.0 | Safe Rust/C++ FFI bridge | Already used in nes-network; generates type-safe bindings |
| tokio | 1.40.0 | Async runtime for sources | Matches nes-network version; multi-thread with configurable workers |
| tracing | 0.1 | Structured logging | Already integrated with spdlog via existing bridge in nes-network |
| async-channel | 2.3.1 | Bounded MPMC channel for buffer emission | Already used in nes-network; supports bounded backpressure |
| tokio-util | 0.7.13 | CancellationToken | Standard companion to tokio for cooperative cancellation |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| cxx-build | 1.0 | Build-time cxx code generation | Required in build.rs for cxx bridge compilation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| cxx | autocxx | autocxx auto-generates bindings but less control over opaque types; project already standardized on cxx |
| OnceLock | LazyLock | LazyLock cannot accept runtime parameters (thread count); OnceLock::get_or_init is the right primitive |
| spawn_blocking for getBufferBlocking | tokio::sync::Semaphore polling | spawn_blocking is simpler, uses dedicated thread pool, avoids busy-wait |

**Installation:**
```toml
# nes-source-bindings/Cargo.toml
[dependencies]
cxx = "1.0"
tokio = { version = "1.40.0", features = ["rt-multi-thread", "sync", "time"] }
tracing = "0.1"

# nes-source-runtime/Cargo.toml
[dependencies]
tokio = { version = "1.40.0", features = ["rt-multi-thread", "sync", "time", "macros"] }
tokio-util = { version = "0.7.13", features = ["rt"] }
async-channel = "2.3.1"
tracing = "0.1"
```

## Architecture Patterns

### Recommended Project Structure
```
nes-sources/rust/
├── Cargo.toml                    # workspace root
├── Cargo.lock
├── nes-source-bindings/
│   ├── Cargo.toml                # depends on cxx
│   ├── lib.rs                    # #[cxx::bridge] declarations
│   └── src/
│       └── SourceBindings.cpp    # C++ side of bridge (include headers, implement free functions)
├── nes-source-runtime/
│   ├── Cargo.toml                # depends on tokio, async-channel, tokio-util, nes-source-bindings
│   └── src/
│       ├── lib.rs
│       ├── runtime.rs            # OnceLock<Runtime>, init_source_runtime()
│       ├── buffer.rs             # TupleBufferHandle, BufferProviderHandle
│       ├── context.rs            # SourceContext
│       └── error.rs              # SourceResult, SourceError
├── nes-source-lib/
│   ├── Cargo.toml                # staticlib, re-exports bindings + runtime
│   └── src/lib.rs                # pub use nes_source_bindings::*; pub use nes_source_runtime::*;
└── nes-sources/                  # Phase 3+ concrete source implementations
    └── Cargo.toml
```

### Pattern 1: cxx Bridge with Opaque C++ Types and Free Functions
**What:** Declare C++ types as opaque in `unsafe extern "C++"` block, expose operations as free functions rather than methods (cxx limitation for opaque types).
**When to use:** Always for wrapping C++ classes that Rust cannot see the layout of.
**Example:**
```rust
// Source: nes-network/nes-rust-bindings/network/lib.rs (established pattern)
#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("SourceBindings.hpp");

        type TupleBuffer;
        fn retain(buf: Pin<&mut TupleBuffer>);
        fn release(buf: Pin<&mut TupleBuffer>);
        fn getDataPtr(buf: &TupleBuffer) -> *const u8;
        fn getDataPtrMut(buf: Pin<&mut TupleBuffer>) -> *mut u8;
        fn getCapacity(buf: &TupleBuffer) -> u64;
        fn getNumberOfTuples(buf: &TupleBuffer) -> u64;
        fn setNumberOfTuples(buf: Pin<&mut TupleBuffer>, count: u64);
        fn getReferenceCounter(buf: &TupleBuffer) -> u32;

        type AbstractBufferProvider;
        fn getBufferBlocking(provider: Pin<&mut AbstractBufferProvider>) -> UniquePtr<TupleBuffer>;

        // Runtime init
        fn getSourceRuntimeThreadCount(/* WorkerConfiguration ref */) -> u32;
    }

    extern "Rust" {
        fn init_source_runtime(thread_count: u32);
        // ... other FFI-exposed functions
    }
}
```

### Pattern 2: Safe Wrapper with Clone/Drop Mirroring C++ Refcounting
**What:** Rust struct wrapping a `UniquePtr<CppType>` where `Clone` calls the C++ `retain()` and `Drop` calls `release()`.
**When to use:** For any C++ type with reference-counted ownership (TupleBuffer).
**Example:**
```rust
pub struct TupleBufferHandle {
    inner: cxx::UniquePtr<ffi::TupleBuffer>,
}

// SAFETY: TupleBuffer's reference counting (retain/release) uses atomics.
// The buffer can be sent between threads safely. Not Sync because
// concurrent mutable access to the data region is not safe.
unsafe impl Send for TupleBufferHandle {}

impl Clone for TupleBufferHandle {
    fn clone(&self) -> Self {
        // Need to get a mutable pin to call retain
        // This requires careful design -- see Pitfall 2 below
        let mut new_ptr = /* clone the UniquePtr somehow -- see design note */;
        ffi::retain(new_ptr.pin_mut());
        TupleBufferHandle { inner: new_ptr }
    }
}

impl Drop for TupleBufferHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            ffi::release(self.inner.pin_mut());
        }
    }
}

impl TupleBufferHandle {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = ffi::getDataPtrMut(self.inner.pin_mut());
        let cap = ffi::getCapacity(&*self.inner) as usize;
        unsafe { std::slice::from_raw_parts_mut(ptr, cap) }
    }

    pub fn set_number_of_tuples(&mut self, n: u64) {
        ffi::setNumberOfTuples(self.inner.pin_mut(), n);
    }
}
```

### Pattern 3: OnceLock Runtime Initialization
**What:** Use `std::sync::OnceLock` to initialize the Tokio runtime exactly once with a runtime-provided thread count.
**When to use:** For the shared source runtime.
**Example:**
```rust
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static SOURCE_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn init_source_runtime(thread_count: u32) {
    let created = SOURCE_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .thread_name("nes-source-rt")
            .enable_all()
            .build()
            .expect("Failed to create source Tokio runtime")
    });
    // If runtime was already initialized, warn (can't change thread count)
    // OnceLock doesn't tell us if we were first, so use a separate AtomicBool
}

pub fn source_runtime() -> &'static Runtime {
    SOURCE_RUNTIME.get().expect("Source runtime not initialized")
}
```

### Pattern 4: Blocking C++ Call in Async Context
**What:** Use `spawn_blocking` to call blocking C++ functions from async Rust.
**When to use:** For `allocate_buffer()` which calls `getBufferBlocking()`.
**Example:**
```rust
impl SourceContext {
    pub async fn allocate_buffer(&self) -> TupleBufferHandle {
        let provider = self.buffer_provider.clone();
        tokio::task::spawn_blocking(move || {
            provider.get_buffer_blocking()
        })
        .await
        .expect("spawn_blocking task panicked")
    }
}
```

### Anti-Patterns to Avoid
- **Calling blocking C++ from async context without spawn_blocking:** Will starve the Tokio worker threads. `getBufferBlocking()` can block indefinitely waiting for buffer recycling.
- **Making TupleBufferHandle Sync:** The data region is a raw byte buffer. Concurrent reads/writes without synchronization are UB. Send-only is correct.
- **Using LazyLock for runtime init:** LazyLock's closure captures no parameters. OnceLock::get_or_init allows passing the thread count at call time.
- **Sharing one Tokio runtime with nes-network:** The network layer creates separate runtimes per sender/receiver. Source runtime must be independent to avoid contention and lifecycle coupling.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| C++ interop bindings | Manual `extern "C"` with raw pointers | cxx bridge | Type safety, automatic string/slice conversion, compile-time verification |
| Runtime-once initialization | Custom Mutex + bool flag | `std::sync::OnceLock` | Standard library, zero overhead after init, no deadlock risk |
| Async cancellation | Manual AtomicBool polling | `tokio_util::sync::CancellationToken` | Composable with `tokio::select!`, tree-structured cancellation |
| Bounded async channel | Custom ring buffer | `async-channel` crate | Battle-tested MPMC, correct backpressure, already used in project |
| Reference count wrapper | Manual pointer arithmetic | Wrapper struct with Clone/Drop calling C++ retain/release | Rust's ownership model enforces correctness at compile time |

**Key insight:** The nes-network crate has already solved most of these FFI patterns. The source bindings should follow the same structure, not reinvent.

## Common Pitfalls

### Pitfall 1: UniquePtr Cannot Be Cloned
**What goes wrong:** `cxx::UniquePtr<T>` has no `Clone` impl. You cannot simply clone a UniquePtr to implement TupleBufferHandle::clone().
**Why it happens:** UniquePtr represents exclusive ownership, like C++ `std::unique_ptr`.
**How to avoid:** The C++ bridge must provide a `cloneTupleBuffer` free function that takes a `&TupleBuffer`, calls `retain()`, and returns a new `UniquePtr<TupleBuffer>` constructed from the same underlying pointer. Alternatively, use `SharedPtr` instead of `UniquePtr` -- but this changes the C++ API. The cleanest approach is a C++ free function that copies the TupleBuffer value (which internally calls the copy constructor, which calls retain).
**Warning signs:** Compile error "the trait Clone is not implemented for UniquePtr<TupleBuffer>".

### Pitfall 2: Pin<&mut T> Requirements for Mutable Operations
**What goes wrong:** cxx opaque C++ types behind `UniquePtr` require `Pin<&mut T>` for mutable access via `.pin_mut()`.
**Why it happens:** cxx uses pinning to prevent moving C++ objects in memory (C++ objects have stable addresses).
**How to avoid:** Design bridge functions to take `Pin<&mut TupleBuffer>` for mutating operations (retain, release, setNumberOfTuples, write data) and `&TupleBuffer` for read-only operations (getCapacity, getNumberOfTuples).
**Warning signs:** Compile errors about Pin requirements.

### Pitfall 3: Existing Panic Hook Conflicts
**What goes wrong:** The spdlog bridge (`nes-network/nes-rust-bindings/spdlog/lib.rs`) installs a custom panic hook that calls `std::process::exit(1)`. This is called before cxx's own panic handling.
**Why it happens:** `std::panic::set_hook` is global -- only one hook can be active.
**How to avoid:** For Phase 1, this is acceptable behavior (panics terminate the process, which is the desired policy per CONTEXT.md). If `catch_unwind` is needed for raw `extern "C"` functions, it must be wrapped BEFORE the panic hook fires, using `std::panic::catch_unwind` directly.
**Warning signs:** Process exits with code 1 instead of propagating error to C++.

### Pitfall 4: spawn_blocking Pool Exhaustion
**What goes wrong:** If many sources call `allocate_buffer()` simultaneously and all block waiting for C++ buffers, the spawn_blocking thread pool grows unboundedly.
**Why it happens:** Tokio's blocking thread pool has no default cap (or a very high one, 512).
**How to avoid:** The `defaultMaxInflightBuffers` (64 per source) already limits concurrency. The total blocking threads is bounded by number_of_sources * 1 (each source allocates one buffer at a time in its run loop). For Phase 1, this is not a concern -- just document it.
**Warning signs:** Thread count growing unexpectedly in production.

### Pitfall 5: C++ Header Include Paths in cxx Bridge
**What goes wrong:** The cxx bridge `include!()` directive needs C++ headers accessible at build time. The CMake target must have correct include directories.
**Why it happens:** Corrosion builds Rust via cargo but links against CMake-managed C++ libraries.
**How to avoid:** Follow the nes-network pattern: create a `SourceBindings.hpp` that includes NES headers, and a `SourceBindings.cpp` that implements the free function wrappers. The CMakeLists.txt must `target_link_libraries` with `nes-memory` (for TupleBuffer) and `nes-executable` (for BackpressureListener).
**Warning signs:** Build errors about missing headers or unresolved symbols.

### Pitfall 6: TupleBuffer Drop in Wrong Order
**What goes wrong:** If a TupleBufferHandle is dropped after the BufferManager is destroyed, the recycle callback segfaults.
**Why it happens:** BufferManager checks `isDestroyed` atomic in recycler, and the destructor asserts all refcounts are zero.
**How to avoid:** Source runtime must ensure all TupleBufferHandle instances are dropped before the C++ shutdown path destroys the BufferManager. Since there is no explicit shutdown FFI (cleanup on process exit), and sources are stopped via query engine before shutdown, this ordering is naturally maintained.
**Warning signs:** Assertion failure in BufferManager destructor about non-zero reference counts.

## Code Examples

### CMakeLists.txt for Source Bindings
```cmake
# Source: nes-network/nes-rust-bindings/network/CMakeLists.txt (established pattern)
corrosion_add_cxxbridge(
    nes-source-bindings
    CRATE nes_source_lib
    FILES ../nes-source-bindings/lib.rs
)
target_sources(nes-source-bindings PRIVATE src/SourceBindings.cpp)
target_include_directories(nes-source-bindings
    PRIVATE .
    PUBLIC include
)
target_link_libraries(nes-source-bindings PUBLIC nes-memory nes-sources nes-executable)
```

### C++ Bridge Implementation (SourceBindings.hpp)
```cpp
#pragma once
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <memory>

// Free functions wrapping TupleBuffer methods for cxx bridge
namespace NES::SourceBindings {
    void retain(NES::TupleBuffer& buf);
    void release(NES::TupleBuffer& buf);
    const uint8_t* getDataPtr(const NES::TupleBuffer& buf);
    uint8_t* getDataPtrMut(NES::TupleBuffer& buf);
    uint64_t getCapacity(const NES::TupleBuffer& buf);
    uint64_t getNumberOfTuples(const NES::TupleBuffer& buf);
    void setNumberOfTuples(NES::TupleBuffer& buf, uint64_t count);
    uint32_t getReferenceCounter(const NES::TupleBuffer& buf);

    // Creates a new TupleBuffer by copy (calls retain via copy constructor)
    std::unique_ptr<NES::TupleBuffer> cloneTupleBuffer(const NES::TupleBuffer& buf);

    // Buffer allocation
    std::unique_ptr<NES::TupleBuffer> getBufferBlocking(NES::AbstractBufferProvider& provider);
}
```

### Workspace Cargo.toml
```toml
# Source: nes-network/Cargo.toml (established pattern)
[workspace]
resolver = "2"
members = ["nes-source-bindings", "nes-source-runtime", "nes-source-lib"]
```

### staticlib Re-export Crate
```rust
// Source: nes-network/nes-rust-bindings/src/lib.rs (established pattern)
pub use nes_source_bindings::*;
pub use nes_source_runtime::*;
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Manual `extern "C"` FFI | cxx bridge | Adopted in nes-network (project convention) | Type-safe, compile-verified FFI |
| `lazy_static!` / `once_cell` | `std::sync::OnceLock` (std since 1.70) | Rust 1.70, June 2023 | No external dependency needed |
| `Mutex<Option<Runtime>>` init | `OnceLock::get_or_init` | std stabilization | Simpler, no unwrap chains |
| Network creates runtime per service | Sources share one runtime via OnceLock | This phase | Simpler lifecycle, sufficient for source workload |

**Deprecated/outdated:**
- `lazy_static!` crate: Replaced by `std::sync::LazyLock` (std since 1.80). But LazyLock doesn't accept runtime parameters, so `OnceLock` is correct here.
- `once_cell` crate: Functionality merged into std as `OnceLock`/`LazyLock`.

## Open Questions

1. **UniquePtr Clone Strategy**
   - What we know: cxx `UniquePtr<T>` cannot be cloned. TupleBuffer's C++ copy constructor calls retain internally.
   - What's unclear: Whether cxx can bridge a function returning `UniquePtr<TupleBuffer>` from a `const TupleBuffer&` input. This should work since TupleBuffer has a public copy constructor.
   - Recommendation: Implement `cloneTupleBuffer()` as a C++ free function that constructs a new `unique_ptr<TupleBuffer>` via copy. Test this early in implementation.

2. **AbstractBufferProvider Bridging**
   - What we know: Sources receive `shared_ptr<AbstractBufferProvider>` in `Source::open()`. cxx supports `SharedPtr` for C++ shared_ptr.
   - What's unclear: Whether cxx can bridge virtual method calls on `AbstractBufferProvider` (it's abstract). May need a C++ wrapper function `getBufferBlockingFromProvider(SharedPtr<AbstractBufferProvider>&)`.
   - Recommendation: Use free functions that take `SharedPtr<AbstractBufferProvider>` and delegate to the virtual methods in C++.

3. **Tokio Version Compatibility**
   - What we know: nes-network uses tokio 1.40.0. STATE.md notes tokio 1.43.x LTS ends March 2026.
   - What's unclear: Whether we should pin to 1.40.0 for workspace compatibility or upgrade.
   - Recommendation: Use 1.40.0 to match nes-network. Both workspaces are independent so version mismatch is acceptable but unnecessary.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust `#[cfg(test)]` + `#[tokio::test]` for async, Google Test for C++ integration |
| Config file | Cargo.toml `[dev-dependencies]` section in each crate |
| Quick run command | `cargo test -p nes_source_runtime --lib` |
| Full suite command | `cargo test --workspace` (from nes-sources/rust/) |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FFI-01 | TupleBufferHandle Clone calls retain, Drop calls release, refcount correct | unit | `cargo test -p nes_source_runtime buffer::tests::test_clone_drop_refcount` | No -- Wave 0 |
| FFI-02 | BufferProviderHandle allocates buffer via getBufferBlocking | integration | Requires C++ linkage -- test via CMake/ctest | No -- Wave 0 |
| FFI-03 | Panic in FFI-exposed function caught (or abort is acceptable per decision) | unit | `cargo test -p nes_source_bindings tests::test_panic_handling` | No -- Wave 0 |
| FFI-04 | cxx bridge compiles with TupleBuffer, BufferProvider, BackpressureListener types | build | `cargo build -p nes_source_bindings` | No -- Wave 0 |
| FWK-01 | Runtime initializes once via OnceLock, second call is no-op with warn | unit | `cargo test -p nes_source_runtime runtime::tests::test_runtime_init_once` | No -- Wave 0 |
| FWK-03 | SourceContext compiles with allocate_buffer() and emit().await signatures | unit | `cargo test -p nes_source_runtime context::tests::test_source_context_api` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --workspace` (from nes-sources/rust/)
- **Per wave merge:** `cargo test --workspace` + CMake build verification
- **Phase gate:** Full Rust test suite green + CMake integration builds

### Wave 0 Gaps
- [ ] `nes-sources/rust/` workspace -- entire directory structure needs creation
- [ ] `nes-source-runtime/src/buffer.rs` -- TupleBufferHandle tests (FFI-01)
- [ ] `nes-source-runtime/src/runtime.rs` -- runtime init tests (FWK-01)
- [ ] `nes-source-runtime/src/context.rs` -- SourceContext API tests (FWK-03)
- [ ] `nes-source-bindings/lib.rs` -- cxx bridge declarations (FFI-04)
- [ ] Note: FFI-01 refcount assertion test requires C++ linkage. Pure Rust unit test can verify Clone/Drop call sites but actual refcount verification needs a mock or integration test via CMake.

## Sources

### Primary (HIGH confidence)
- Codebase: `nes-network/nes-rust-bindings/network/lib.rs` -- established cxx bridge pattern with opaque C++ types, free functions, LazyLock singleton, Tokio runtime creation
- Codebase: `nes-network/nes-rust-bindings/network/CMakeLists.txt` -- Corrosion + cxx bridge CMake integration
- Codebase: `nes-memory/include/Runtime/TupleBuffer.hpp` -- TupleBuffer API (retain, release, getBufferSize, getNumberOfTuples, setNumberOfTuples, copy constructor)
- Codebase: `nes-memory/include/Runtime/AbstractBufferProvider.hpp` -- AbstractBufferProvider virtual interface (getBufferBlocking, getBufferNoBlocking)
- Codebase: `nes-memory/include/Runtime/BufferManager.hpp` -- BufferManager uses folly::MPMCQueue + atomic (thread-safe)
- Codebase: `cmake/EnableRust.cmake` -- Corrosion v0.5.2, nightly toolchain for sanitizers, corrosion_import_crate pattern
- [cxx UniquePtr docs](https://cxx.rs/binding/uniqueptr.html) -- UniquePtr behavior and limitations
- [cxx result/panic docs](https://cxx.rs/binding/result.html) -- panics in extern Rust functions abort the process

### Secondary (MEDIUM confidence)
- [Rust std::panic::catch_unwind](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html) -- catch_unwind semantics
- [cxx.rs main docs](https://docs.rs/cxx) -- cxx crate API reference

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already used in nes-network, versions confirmed from Cargo.toml files
- Architecture: HIGH -- crate structure directly mirrors established nes-network pattern, confirmed from codebase
- Pitfalls: HIGH -- derived from actual codebase analysis (panic hook, UniquePtr limitations, BufferManager thread safety confirmed via source code)

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable domain, no fast-moving dependencies)
