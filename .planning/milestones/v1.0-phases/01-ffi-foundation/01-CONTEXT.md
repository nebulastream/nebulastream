# Phase 1: FFI Foundation - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Safe Rust type wrappers (TupleBuffer, BufferProvider), cxx bridge declarations for source-related C++ types, shared Tokio runtime initialization, and SourceContext type with allocate/emit API. Everything higher-level code builds on without revisiting FFI safety.

</domain>

<decisions>
## Implementation Decisions

### Crate organization
- New Cargo workspace under `nes-sources/rust/` mirroring the nes-network pattern
- Three sub-crates: `nes-source-bindings` (cxx bridge), `nes-source-runtime` (Tokio runtime, SourceContext, AsyncSource trait), `nes-sources` (concrete implementations, Phase 3+)
- Single `nes-source-lib` staticlib crate that re-exports bindings + runtime — CMake links this one .a file
- Source bindings are fully independent from network bindings — no cross-dependency, separate cxx bridge declarations
- CMake integration follows the nes-network corrosion-based approach; if duplicate symbol issues arise during linking, fall back to per-executable purpose-built staticlibs

### Tokio runtime strategy
- Separate dedicated Tokio multi-threaded runtime for sources, independent from network layer runtimes
- Thread count configured via WorkerConfiguration field, passed through FFI during init
- Runtime initialized via OnceLock — first caller wins, subsequent calls with different thread count log a `tracing::warn`
- No explicit shutdown FFI — sources are shut down via query engine; runtime cleaned up on process exit
- Thread name prefix: `nes-source-rt`

### SourceContext API shape
- `allocate_buffer().await` — async, awaits until a buffer is available, never returns allocation failure (or task is cancelled)
- `emit(buf).await` — async, handles backpressure internally
- `run()` returns `SourceResult` enum: `EndOfStream` or `Error(SourceError)` — framework dispatches based on return value
- Cancellation token exposed directly via `ctx.cancellation_token()` — source authors use it in `tokio::select!` for cooperative cancellation with custom shutdown logic
- Buffer access is opaque byte-slice: `buf.as_mut_slice()` returns `&mut [u8]`, plus `set_number_of_tuples(n)` — source writes raw bytes

### FFI safety boundaries
- TupleBuffer: opaque cxx type with free functions (retain, release, getDataPtr, getCapacity, getNumberOfTuples, setNumberOfTuples)
- TupleBufferHandle: safe Rust wrapper with Clone (calls retain) and Drop (calls release), wraps UniquePtr
- TupleBufferHandle is `Send` only (not `Sync`) — sufficient for async_channel transfer, one owner at a time
- BufferProviderHandle: `unsafe impl Send` — required for SourceContext to live in async tasks across Tokio worker threads. Safety depends on concrete C++ BufferProvider being thread-safe (audit required during research)
- Panic handling: trust cxx's built-in panic-to-exception conversion. Manual catch_unwind only if raw `extern "C"` functions are added outside cxx

### Claude's Discretion
- Exact cxx bridge function signatures (may need adjustment based on what cxx supports for these C++ types)
- Internal SourceContext struct layout
- Error type design for SourceError
- tracing instrumentation details
- Exact Cargo.toml dependency versions (match nes-network where applicable)

</decisions>

<specifics>
## Specific Ideas

- "We also need to handle errors so this is the way" — SourceResult enum chosen specifically because run() needs to communicate both EoS and error conditions
- "The source may have to do some shutdown logic so we require cooperative multitasking here" — cancellation token is exposed, not hidden, because sources need to run cleanup before exiting
- "I think we need only Send for async_channel" — TupleBufferHandle is Send-only, matching the channel transfer pattern
- CMake duplicate symbol concern: "I remember there being some issue with linking duplicate symbols during linking, if that happens we may have to whip out some cmake utils to track the rust crates used per executable and create a purpose-built rust staticlib for every binary we create"

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `nes-network/nes-rust-bindings/`: Working cxx bridge pattern to follow (network/lib.rs has the template)
- `nes-network/network/Cargo.toml`: Tokio 1.40, async-channel 2.3.1, tracing — same dependencies needed here
- `nes-network/` CMakeLists.txt: Corrosion-based Rust build integration to mirror

### Established Patterns
- cxx bridge with opaque C++ types and free functions (see network bindings)
- Separate Cargo workspace per NES module (nes-network has its own workspace)
- staticlib crate as single link target re-exporting sub-crates

### Integration Points
- `SourceHandle` (nes-sources/include/Sources/SourceHandle.hpp): Takes BackpressureListener, AbstractBufferProvider, and Source — Phase 2 will add an async variant
- `Source::fillTupleBuffer()` (nes-sources/include/Sources/Source.hpp): Existing sync interface — async sources replace this with `run()` returning `SourceResult`
- `WorkerConfiguration`: Where the runtime thread count field will be added
- `TupleBuffer` retain/release: C++ reference counting that Clone/Drop must mirror

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-ffi-foundation*
*Context gathered: 2026-03-15*
