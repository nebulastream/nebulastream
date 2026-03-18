# Phase 6: Registration and Validation - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

TokioSinkRegistry for factory-based sink registration by string name, AsyncFileSink reference implementation writing raw binary via tokio::fs, and end-to-end system test running TokioSource through a pipeline into AsyncFileSink at scale. Also adds SinkMessage::Flush variant and generic spawn FFI for file sinks.

</domain>

<decisions>
## Implementation Decisions

### Registry design & lookup path
- TokioSinkRegistry lives in `nes-sinks/registry/` (alongside existing SinkRegistry)
- Uses same BaseRegistry + generated `.inc.in` template pattern as TokioSourceRegistry
- TokioSinkRegistryArguments contains `{SinkDescriptor, uint32_t channelCapacity}`
- channelCapacity resolved by SinkProvider from SinkDescriptor config (per-sink override) or WorkerConfiguration global default — mirrors how inflightLimit works for sources
- SinkProvider gets a `TokioSinkRegistry::instance().contains(sinkType)` pre-check before falling back to standard SinkRegistry — same pattern as SourceProvider

### AsyncFileSink behavior
- Writes raw binary (TupleBuffer data bytes as-is) — not CSV or human-readable
- Flushes on `SinkMessage::Flush` and on `SinkMessage::Close` — NOT after every `Data` message
- Constructor takes `PathBuf` for output file path
- Factory reads FILE_PATH from SinkDescriptor config, passes to AsyncFileSink::new(path)

### SinkMessage::Flush variant (Phase 4 extension)
- Add `SinkMessage::Flush` to the enum: `Data(TupleBufferHandle) | Flush | Close`
- Flush is advisory — sinks should flush internal state but it's not blocking/required
- No C++ code sends Flush automatically yet — available for future use (periodic flush, etc.)
- AsyncFileSink handles Flush by calling `file.flush().await`, then continues recv loop
- Other sinks can treat Flush as a no-op without breaking correctness

### System test structure
- Scale matches TokioSource systest: 10000 sources x 1000 tuples = 10M tuples
- Sink type name: `TokioFileSink` (registered in TokioSinkRegistry)
- Uses `CREATE SINK ... TYPE TokioFileSink` syntax
- No special verification logic — let systest infra handle pass/fail, inspect manually if needed

### Generic spawn_sink FFI
- Per-type spawn functions: each sink type gets its own CXX function (spawn_file_sink, spawn_devnull_sink)
- Each has a matching C++ factory helper: makeFileSinkSpawnFn(path), makeDevNullSinkSpawnFn()
- Mirrors source pattern exactly (spawn_generator_source + makeGeneratorSpawnFn)
- Type-safe, no string dispatch on Rust side
- AsyncFileSink config: FILE_PATH only (single config parameter from SinkDescriptor)

### Claude's Discretion
- TokioSinkGeneratedRegistrar.inc.in template exact structure (mirror TokioSourceGeneratedRegistrar.inc.in)
- SinkProvider integration details (where to add the TokioSinkRegistry check)
- WorkerConfiguration field name for default channel capacity
- AsyncFileSink internal buffering strategy (tokio::fs handles this)
- Systest file output path (temp file)
- Systest tool integration details for TokioSink type (VAL-03)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source registry (pattern to mirror for TokioSinkRegistry)
- `nes-sources/registry/include/TokioSourceRegistry.hpp` — BaseRegistry template, TokioSourceRegistryArguments struct, generated registrar include pattern
- `nes-sources/registry/templates/TokioSourceGeneratedRegistrar.inc.in` — Code generation template with @REGISTER_FUNCTION_DECLARATIONS@ and @REGISTER_ALL_FUNCTION_CALLS@
- `nes-sources/src/TokioGeneratorSource.cpp` — Factory function pattern (RegisterTokioGeneratorTokioSource), config extraction from SourceDescriptor, makeGeneratorSpawnFn

### Source provider (lookup pattern to mirror in SinkProvider)
- `nes-sources/src/SourceProvider.cpp` — TokioSourceRegistry::instance().contains() pre-check, inflightLimit resolution from descriptor + default, TokioSourceRegistryArguments construction

### Sink infrastructure
- `nes-sinks/registry/` — Existing SinkRegistry location (where TokioSinkRegistry will live)
- `nes-sinks/src/SinkProvider.cpp` — Where to add TokioSinkRegistry lookup (mirror SourceProvider pattern)

### TokioSink C++ operator (Phase 5 output)
- `nes-sources/include/Sources/TokioSink.hpp` — SpawnFn signature, RustSinkHandleImpl, constructor params
- `nes-sources/src/TokioSink.cpp` — start() using SpawnFn, makeDevNullSinkSpawnFn() factory pattern

### Sink framework (Phase 4 output — Rust side)
- `nes-sources/rust/nes-source-runtime/src/sink.rs` — AsyncSink trait, spawn_sink
- `nes-sources/rust/nes-source-runtime/src/sink_context.rs` — SinkMessage enum (to extend with Flush)
- `nes-sources/rust/nes-source-runtime/src/sink_handle.rs` — SinkTaskHandle, stop_sink

### CXX bridge (Phase 5 output)
- `nes-sources/rust/nes-source-lib/src/lib.rs` — ffi_sink module, spawn_devnull_sink, SinkHandle, SendResult

### Existing systest (pattern to mirror)
- `nes-systests/sources/TokioSource.test` — TokioGenerator systest structure, CREATE SOURCE/SINK syntax, scale (10000 x 1000)

### WorkerConfiguration
- `nes-runtime/interface/Configuration/WorkerConfiguration.hpp` — defaultMaxInflightBuffers pattern to mirror for default channel capacity

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `BaseRegistry` template: Direct reuse for TokioSinkRegistry
- `TokioSourceGeneratedRegistrar.inc.in`: Template to mirror for TokioSinkGeneratedRegistrar.inc.in
- `makeDevNullSinkSpawnFn()`: Already exists, serves as pattern for makeFileSinkSpawnFn()
- `spawn_devnull_sink` CXX function: Pattern for spawn_file_sink
- `DevNullSink` in lib.rs: Pattern for AsyncFileSink registration

### Established Patterns
- Per-type spawn function + makeXxxSpawnFn() factory helper
- SourceProvider/SinkProvider as the routing layer (registry pre-check + fallback)
- Config extraction from SourceDescriptor/SinkDescriptor
- Global default in WorkerConfiguration with per-instance override

### Integration Points
- SinkProvider: Add TokioSinkRegistry contains() check before standard SinkRegistry
- WorkerConfiguration: Add defaultChannelCapacity (or similar) field
- SinkDescriptor: Define CHANNEL_CAPACITY and FILE_PATH config parameters
- CMake: TokioSinkRegistry in nes-sinks/registry/, generated registrar template
- nes-source-runtime: Extend SinkMessage enum with Flush variant
- nes-source-lib: Add spawn_file_sink CXX function

</code_context>

<specifics>
## Specific Ideas

- "We should add a flush message, although it's only a suggestion nothing that blocks" — Flush is advisory, sinks can ignore it
- "Just let the systest fail and inspect the data yourself" — no special verification, rely on systest infra
- Channel capacity follows same pattern as inflightLimit — SinkProvider resolves from descriptor + global default
- Per-type spawn functions, not string dispatch — type safety across FFI

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 06-registration-and-validation*
*Context gathered: 2026-03-16*
