# Phase 6: Registration and Validation - Research

**Researched:** 2026-03-16
**Domain:** C++ registry infrastructure, Rust async file I/O, CMake plugin system, system test infrastructure
**Confidence:** HIGH

## Summary

Phase 6 is primarily a mechanical mirroring exercise: the TokioSourceRegistry + TokioGeneratorSource pattern established in prior phases provides an exact template for TokioSinkRegistry + TokioFileSink registration. The code paths are well-understood from direct inspection of the existing codebase. The Rust side adds AsyncFileSink (tokio::fs raw binary writes) and a spawn_file_sink CXX function. The C++ side adds TokioSinkRegistry, SinkProvider integration, and a registration factory. The systest extends the existing TokioSource.test pattern.

The SinkProvider integration is the most architecturally significant change: the current `NES::lower()` function in SinkProvider.cpp only checks the standard SinkRegistry. It must be extended to check TokioSinkRegistry first (mirroring SourceProvider's pattern), constructing a TokioSink with the appropriate SpawnFn. This also requires extending SinkMessage with a Flush variant and adding a WorkerConfiguration field for default channel capacity.

**Primary recommendation:** Mirror the TokioSourceRegistry/TokioGeneratorSource pattern exactly for sinks. The existing code provides a complete template -- deviations should be avoided.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- TokioSinkRegistry lives in `nes-sinks/registry/` (alongside existing SinkRegistry)
- Uses same BaseRegistry + generated `.inc.in` template pattern as TokioSourceRegistry
- TokioSinkRegistryArguments contains `{SinkDescriptor, uint32_t channelCapacity}`
- channelCapacity resolved by SinkProvider from SinkDescriptor config (per-sink override) or WorkerConfiguration global default -- mirrors how inflightLimit works for sources
- SinkProvider gets a `TokioSinkRegistry::instance().contains(sinkType)` pre-check before falling back to standard SinkRegistry -- same pattern as SourceProvider
- AsyncFileSink writes raw binary (TupleBuffer data bytes as-is) -- not CSV or human-readable
- Flushes on `SinkMessage::Flush` and on `SinkMessage::Close` -- NOT after every Data message
- Constructor takes `PathBuf` for output file path
- Factory reads FILE_PATH from SinkDescriptor config, passes to AsyncFileSink::new(path)
- Add `SinkMessage::Flush` to the enum: `Data(TupleBufferHandle) | Flush | Close`
- Flush is advisory -- sinks should flush internal state but it's not blocking/required
- No C++ code sends Flush automatically yet -- available for future use
- AsyncFileSink handles Flush by calling `file.flush().await`, then continues recv loop
- Other sinks can treat Flush as a no-op without breaking correctness
- Scale matches TokioSource systest: 10000 sources x 1000 tuples = 10M tuples
- Sink type name: `TokioFileSink` (registered in TokioSinkRegistry)
- Uses `CREATE SINK ... TYPE TokioFileSink` syntax
- No special verification logic -- let systest infra handle pass/fail, inspect manually if needed
- Per-type spawn functions: each sink type gets its own CXX function (spawn_file_sink, spawn_devnull_sink)
- Each has a matching C++ factory helper: makeFileSinkSpawnFn(path), makeDevNullSinkSpawnFn()
- Type-safe, no string dispatch on Rust side
- AsyncFileSink config: FILE_PATH only (single config parameter from SinkDescriptor)

### Claude's Discretion
- TokioSinkGeneratedRegistrar.inc.in template exact structure (mirror TokioSourceGeneratedRegistrar.inc.in)
- SinkProvider integration details (where to add the TokioSinkRegistry check)
- WorkerConfiguration field name for default channel capacity
- AsyncFileSink internal buffering strategy (tokio::fs handles this)
- Systest file output path (temp file)
- Systest tool integration details for TokioSink type (VAL-03)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| REG-01 | TokioSinkRegistry for string-name factory registration (mirrors TokioSourceRegistry) | Registry pattern fully documented: BaseRegistry template, generated registrar .inc.in, add_plugin CMake macro, SinkProvider integration path |
| VAL-01 | AsyncFileSink reference implementation using tokio::fs async file I/O | AsyncSink trait, SinkContext/SinkMessage API documented; tokio::fs::File write pattern straightforward; Flush variant addition to SinkMessage |
| VAL-02 | System test using TokioSource -> pipeline -> AsyncFileSink with inline sink syntax | TokioSource.test pattern documented; CREATE SINK syntax with TYPE; SystestBinder sink handling analyzed |
| VAL-03 | Systest tool integration for TokioSink (enable sink type if needed) | SystestBinder uses SinkDescriptor + SinkValidationRegistry; TokioFileSink needs SinkValidation registration and SinkProvider routing to TokioSinkRegistry |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.40.0 | Async runtime (already in Cargo.toml) | Project standard |
| tokio::fs | (part of tokio) | Async file I/O for AsyncFileSink | Needs `fs` feature flag added to Cargo.toml |
| cxx | 1.0 | CXX bridge for spawn_file_sink FFI | Project standard |
| async-channel | 2.3.1 | Bounded channel for SinkMessage | Already in use |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| BaseRegistry (C++) | N/A | Template for TokioSinkRegistry | Direct reuse from Util/Registry.hpp |
| DescriptorConfig (C++) | N/A | Config parameter extraction from SinkDescriptor | Same pattern as source config params |

**Installation (Rust side):**
```toml
# In nes-sources/rust/nes-source-runtime/Cargo.toml, add "fs" to tokio features:
tokio = { version = "1.40.0", features = ["rt-multi-thread", "sync", "time", "macros", "fs"] }
```

## Architecture Patterns

### Recommended Project Structure

```
nes-sinks/
  registry/
    include/
      SinkRegistry.hpp              # existing
      SinkValidationRegistry.hpp    # existing
      TokioSinkRegistry.hpp         # NEW: BaseRegistry<TokioSinkRegistry, string, ...>
    templates/
      SinkGeneratedRegistrar.inc.in              # existing
      SinkValidationGeneratedRegistrar.inc.in    # existing
      TokioSinkGeneratedRegistrar.inc.in         # NEW: mirrors TokioSourceGeneratedRegistrar.inc.in
  src/
    SinkProvider.cpp                # MODIFY: add TokioSinkRegistry pre-check
    TokioFileSink.cpp               # NEW: registration factory + config params
    CMakeLists.txt                  # MODIFY: add_plugin for TokioFileSink

nes-sources/rust/
  nes-source-runtime/src/
    sink_context.rs                 # MODIFY: add SinkMessage::Flush variant
    file_sink.rs                    # NEW: AsyncFileSink implementation
    lib.rs                          # MODIFY: add pub mod file_sink
  nes-source-lib/src/
    lib.rs                          # MODIFY: add spawn_file_sink to ffi_sink bridge

nes-runtime/interface/Configuration/
  WorkerConfiguration.hpp           # MODIFY: add defaultChannelCapacity field

nes-systests/sources/
  TokioFileSink.test                # NEW: end-to-end systest
```

### Pattern 1: TokioSinkRegistry (mirrors TokioSourceRegistry exactly)

**What:** A BaseRegistry specialization for Rust async sinks, using generated registrar code.
**When to use:** For all TokioSink-type registrations.

**TokioSinkRegistry.hpp structure (from TokioSourceRegistry.hpp):**
```cpp
// In nes-sinks/registry/include/TokioSinkRegistry.hpp
#pragma once
#include <memory>
#include <string>
#include <Sources/TokioSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Registry.hpp>

namespace NES {

using TokioSinkRegistryReturnType = std::unique_ptr<TokioSink>;

struct TokioSinkRegistryArguments {
    SinkDescriptor sinkDescriptor;
    uint32_t channelCapacity;
};

class TokioSinkRegistry final
    : public BaseRegistry<TokioSinkRegistry, std::string, TokioSinkRegistryReturnType, TokioSinkRegistryArguments>
{};
}

#define INCLUDED_FROM_TOKIO_SINK_REGISTRY
#include <TokioSinkGeneratedRegistrar.inc>
#undef INCLUDED_FROM_TOKIO_SINK_REGISTRY
```

**Key difference from SinkRegistryArguments:** TokioSinkRegistryArguments does NOT contain BackpressureController (TokioSink manages its own backpressure). It contains channelCapacity instead.

### Pattern 2: Registration Factory Function (mirrors TokioGeneratorSource.cpp)

**What:** A .cpp file that registers a factory function and a validation function for a Tokio sink type.
**When to use:** For each new AsyncSink type.

**TokioFileSink.cpp structure (from TokioGeneratorSource.cpp):**
```cpp
// Factory function registered as "TokioFileSink" in TokioSinkRegistry
TokioSinkRegistryReturnType
TokioSinkGeneratedRegistrar::RegisterTokioFileSinkTokioSink(TokioSinkRegistryArguments args) {
    const auto filePath = args.sinkDescriptor.getFromConfig(ConfigParametersTokioFileSink::FILE_PATH);
    return std::make_unique<TokioSink>(
        BackpressureController{},  // TokioSink creates its own
        makeFileSinkSpawnFn(filePath),
        args.channelCapacity);
}
```

**CRITICAL: The CMake plugin name must be `TokioFileSink` to match the registered type name.** The `add_plugin` macro generates `RegisterTokioFileSinkTokioSink` from `add_plugin(TokioFileSink TokioSink nes-sinks TokioFileSink.cpp)`.

### Pattern 3: SinkProvider Integration (mirrors SourceProvider.cpp)

**What:** Pre-check TokioSinkRegistry before falling back to SinkRegistry.
**When to use:** In the `NES::lower()` function in SinkProvider.cpp.

```cpp
std::unique_ptr<Sink> lower(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor) {
    const auto& sinkType = sinkDescriptor.getSinkType();

    // Check TokioSinkRegistry first -- Rust async sinks
    if (TokioSinkRegistry::instance().contains(sinkType)) {
        auto tokioArgs = TokioSinkRegistryArguments{
            sinkDescriptor,
            resolveChannelCapacity(sinkDescriptor)  // descriptor override or global default
        };
        if (auto sink = TokioSinkRegistry::instance().create(sinkType, std::move(tokioArgs))) {
            return std::move(sink.value());
        }
        throw UnknownSinkType("TokioSinkRegistry contains '{}' but factory returned nullopt", sinkType);
    }

    // Fall through to standard SinkRegistry
    auto sinkArguments = SinkRegistryArguments(std::move(backpressureController), sinkDescriptor);
    if (auto sink = SinkRegistry::instance().create(sinkType, std::move(sinkArguments))) {
        return std::move(sink.value());
    }
    throw UnknownSinkType("Unknown Sink Descriptor Type: {}", sinkType);
}
```

**NOTE:** The current `lower()` is a free function, not a class method. The channel capacity resolution needs access to a global default from WorkerConfiguration. One approach: make `lower()` take an additional parameter (defaultChannelCapacity), or access it through a global/passed config. The SourceProvider approach uses a class instance with the default stored as a member. SinkProvider may need the same treatment.

### Pattern 4: Per-Type Spawn FFI (mirrors spawn_generator_source)

**What:** Each sink type gets its own CXX function and C++ factory.
**When to use:** For every AsyncSink implementation.

```rust
// In ffi_sink bridge
fn spawn_file_sink(
    sink_id: u64,
    file_path: &str,
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>>;
```

```cpp
// In TokioSink.cpp (or TokioFileSink.cpp)
TokioSink::SpawnFn makeFileSinkSpawnFn(std::string filePath) {
    return [filePath = std::move(filePath)](
        uint64_t sinkId, uint32_t channelCapacity,
        uintptr_t errorFnPtr, uintptr_t errorCtxPtr) {
        auto boxedHandle = ::spawn_file_sink(sinkId, filePath, channelCapacity, errorFnPtr, errorCtxPtr);
        return std::make_unique<TokioSink::RustSinkHandleImpl>(std::move(boxedHandle));
    };
}
```

### Pattern 5: AsyncFileSink Implementation

**What:** Rust struct implementing AsyncSink that writes raw binary to a file.
**When to use:** VAL-01 reference implementation.

```rust
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct AsyncFileSink {
    path: std::path::PathBuf,
}

impl AsyncFileSink {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}

impl AsyncSink for AsyncFileSink {
    async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
        let mut file = File::create(&self.path).await
            .map_err(|e| SinkError::new(format!("failed to open file: {}", e)))?;

        loop {
            match ctx.recv().await {
                SinkMessage::Data(buf) => {
                    file.write_all(buf.as_slice()).await
                        .map_err(|e| SinkError::new(format!("write failed: {}", e)))?;
                }
                SinkMessage::Flush => {
                    file.flush().await
                        .map_err(|e| SinkError::new(format!("flush failed: {}", e)))?;
                }
                SinkMessage::Close => {
                    file.flush().await
                        .map_err(|e| SinkError::new(format!("final flush failed: {}", e)))?;
                    return Ok(());
                }
            }
        }
    }
}
```

### Anti-Patterns to Avoid
- **String dispatch on Rust side:** Do NOT create a generic `spawn_sink(type_name, ...)` that matches on a string. Each sink type has its own spawn function.
- **Flushing after every Data message:** Decision explicitly says flush only on Flush/Close, not per-buffer.
- **Custom BaseRegistry implementation:** Reuse the existing BaseRegistry template, do not roll a custom registry.
- **Putting TokioSinkRegistry in nes-sources:** It belongs in nes-sinks/registry/ alongside SinkRegistry.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Registry infrastructure | Custom hashmap-based registry | BaseRegistry template from Util/Registry.hpp | Handles singleton pattern, thread safety, generated registrars |
| Generated registrar code | Manual registration calls | CMake add_plugin + generate_plugin_registrars | Automatic code generation from .inc.in templates |
| Config parameter parsing | String parsing from SinkDescriptor | DescriptorConfig::ConfigParameter + validateAndFormat | Type-safe extraction with validation |
| Async file I/O | std::fs in tokio::spawn_blocking | tokio::fs::File | Native async I/O on tokio runtime |

## Common Pitfalls

### Pitfall 1: CMake Registry Name Mismatch
**What goes wrong:** The CMake `add_plugin` macro generates function names from the plugin name. If you register `TokioFileSink` in CMake but the code declares a different function name, the linker fails.
**Why it happens:** The `generate_plugin_registrar` function creates `Register${plugin_name}${plugin_registry}` -- e.g., `RegisterTokioFileSinkTokioSink`. The .cpp file must define this exact function signature.
**How to avoid:** Use `add_plugin(TokioFileSink TokioSink nes-sinks TokioFileSink.cpp)` and implement `TokioSinkGeneratedRegistrar::RegisterTokioFileSinkTokioSink(TokioSinkRegistryArguments args)`.
**Warning signs:** Undefined symbol errors at link time mentioning `Register*`.

### Pitfall 2: Missing create_registries_for_component
**What goes wrong:** The TokioSinkGeneratedRegistrar.inc is never generated, causing include failures.
**Why it happens:** The `nes-sinks/CMakeLists.txt` has `create_registries_for_component(Sink SinkValidation)` -- must add `TokioSink` to this list.
**How to avoid:** Update to `create_registries_for_component(Sink SinkValidation TokioSink)`.
**Warning signs:** "file not found" errors for TokioSinkGeneratedRegistrar.inc.

### Pitfall 3: SinkProvider Circular Dependency
**What goes wrong:** SinkProvider.cpp includes TokioSinkRegistry.hpp which includes TokioSink.hpp from nes-sources, creating a cross-module dependency.
**Why it happens:** TokioSink lives in nes-sources, but SinkProvider lives in nes-sinks. nes-sinks does not currently depend on nes-sources.
**How to avoid:** Either (a) move TokioSink.hpp to a shared interface location, (b) add nes-sources as a PRIVATE dependency of nes-sinks, or (c) have TokioSinkRegistry return `std::unique_ptr<Sink>` (base class) instead of `std::unique_ptr<TokioSink>`. Option (c) is cleanest -- the registry factory creates the TokioSink internally and returns the base pointer, so the registry header only needs `#include <Sinks/Sink.hpp>`.
**Warning signs:** CMake dependency cycle errors or include-not-found at compile time.

### Pitfall 4: Tokio "fs" Feature Not Enabled
**What goes wrong:** `tokio::fs::File` is not available, causing compilation errors in Rust.
**Why it happens:** The current Cargo.toml has `features = ["rt-multi-thread", "sync", "time", "macros"]` but not `"fs"`.
**How to avoid:** Add `"fs"` to tokio features in Cargo.toml.
**Warning signs:** Rust compile error: "no module named `fs` in crate `tokio`".

### Pitfall 5: SinkMessage::Flush Breaking Existing Match Arms
**What goes wrong:** Adding Flush variant to SinkMessage breaks all existing `match` expressions (DevNullSink in lib.rs, all tests in sink.rs and sink_context.rs).
**Why it happens:** Rust's exhaustive pattern matching requires all variants be handled.
**How to avoid:** Update all match arms simultaneously. DevNullSink should treat Flush as no-op. Test code should handle Flush where needed.
**Warning signs:** Rust compile errors about non-exhaustive pattern match.

### Pitfall 6: BackpressureController in SinkProvider
**What goes wrong:** The current `NES::lower()` takes a `BackpressureController` parameter that TokioSink creates internally. If the factory passes one from the caller, there's a mismatch.
**Why it happens:** Standard sinks need the caller's BackpressureController. TokioSink creates its own internally.
**How to avoid:** When creating via TokioSinkRegistry, the factory constructs TokioSink with a default/empty BackpressureController. TokioSink's constructor already handles this.
**Warning signs:** Backpressure not working, or double backpressure application.

### Pitfall 7: Systest SinkValidation Registry
**What goes wrong:** The systest uses `SinkDescriptor::validateAndFormatConfig()` which checks `SinkValidationRegistry`. If `TokioFileSink` is not registered there, sink creation fails.
**Why it happens:** The systest binder calls `sinkCatalog->addSinkDescriptor()` which validates config through SinkValidationRegistry before creating the descriptor.
**How to avoid:** Register TokioFileSink in both TokioSinkRegistry and SinkValidation. Use `add_plugin(TokioFileSink SinkValidation nes-sinks TokioFileSink.cpp)`.
**Warning signs:** "Unknown sink type" error during systest at descriptor creation time.

## Code Examples

### SinkMessage::Flush Extension
```rust
// In sink_context.rs
pub enum SinkMessage {
    Data(TupleBufferHandle),
    Flush,
    Close,
}
```

### DevNullSink Updated Match
```rust
// In lib.rs -- updated DevNullSink
impl nes_source_runtime::AsyncSink for DevNullSink {
    async fn run(
        &mut self,
        ctx: &nes_source_runtime::SinkContext,
    ) -> Result<(), nes_source_runtime::SinkError> {
        loop {
            match ctx.recv().await {
                nes_source_runtime::SinkMessage::Data(_buf) => {}
                nes_source_runtime::SinkMessage::Flush => {} // no-op
                nes_source_runtime::SinkMessage::Close => return Ok(()),
            }
        }
    }
}
```

### WorkerConfiguration Extension
```cpp
// In WorkerConfiguration.hpp
UIntOption defaultChannelCapacity
    = {"default_channel_capacity",
       "64",
       "Default bounded channel capacity for Tokio async sinks.",
       {std::make_shared<NumberValidation>()}};
```

### spawn_file_sink CXX Bridge
```rust
// In ffi_sink bridge block
fn spawn_file_sink(
    sink_id: u64,
    file_path: &str,
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>>;
```

Note: CXX supports `&str` for Rust string references. The C++ side passes `rust::Str`.

### Systest File Structure
```
# name: sources/TokioFileSink.test
# description: Tests TokioFileSink -- 10000 sources x 1000 tuples -> file sink
# groups: [Sources, TokioSink]

CREATE LOGICAL SOURCE tokioGen(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR tokioGen TYPE TokioGenerator SET(
    1000 AS `SOURCE`.GENERATOR_COUNT,
    0 AS `SOURCE`.GENERATOR_INTERVAL_MS
);
... (repeat 10000 times to match TokioSource.test scale)

CREATE SINK fileSink(value UINT64 NOT NULL) TYPE TokioFileSink;

SELECT * FROM tokioGen INTO fileSink;
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| SinkProvider as free function | Needs to become class (or take config param) for channel capacity default | This phase | SinkProvider.cpp + SinkProvider.hpp need update |
| SinkMessage: Data + Close only | Data + Flush + Close | This phase | All match arms in Rust code must update |
| nes-sinks independent of nes-sources | nes-sinks may need nes-sources dependency (or use base class return type) | This phase | CMakeLists.txt dependency management |

## Open Questions

1. **SinkProvider function signature change**
   - What we know: Current `lower()` is a free function with no access to WorkerConfiguration defaults. SourceProvider is a class with defaults in its constructor.
   - What's unclear: Whether to make SinkProvider a class (like SourceProvider) or pass defaultChannelCapacity as a parameter to `lower()`.
   - Recommendation: Make SinkProvider a class mirroring SourceProvider, with defaultChannelCapacity in constructor. This is the cleanest parallel to the source side.

2. **TokioSinkRegistry return type and dependency direction**
   - What we know: TokioSink.hpp lives in nes-sources. SinkProvider.cpp lives in nes-sinks.
   - What's unclear: How to avoid nes-sinks depending on nes-sources for the TokioSink type.
   - Recommendation: Use `std::unique_ptr<Sink>` as TokioSinkRegistryReturnType (base class). The factory creates TokioSink internally. The registry header only needs `Sinks/Sink.hpp`. This avoids cross-module dependencies. The factory .cpp file (in nes-sinks) will need a PRIVATE include of TokioSink.hpp -- add nes-sources as PRIVATE dependency of the factory compilation unit only.

3. **BackpressureController sourcing for TokioSink via registry**
   - What we know: `NES::lower()` receives a BackpressureController from the caller (ExecutableQueryPlan.cpp). TokioSink creates its own internal backpressure handling.
   - What's unclear: Whether to pass the caller's BackpressureController to TokioSink or use a default one.
   - Recommendation: Pass the caller's BackpressureController to TokioSink. TokioSink already takes it in its constructor (inherited from Sink base class). The registry factory should accept it. This means TokioSinkRegistryArguments should also include BackpressureController.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust: cargo test (tokio::test); C++: GTest + CTest |
| Config file | nes-sources/rust/nes-source-runtime/Cargo.toml; CMakeLists.txt |
| Quick run command | `cargo test -p nes_source_runtime --lib` (Rust), `ctest --test-dir cmake-build-debug -R TokioSink -j` (C++) |
| Full suite command | `cmake --build cmake-build-debug -j && ctest --test-dir cmake-build-debug -j --output-on-failure` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| REG-01 | TokioSinkRegistry lookup + factory | unit (C++) | `ctest --test-dir cmake-build-debug -R TokioSinkTest -j` | Exists (TokioSinkTest.cpp) -- extend |
| VAL-01 | AsyncFileSink write/flush/close | unit (Rust) | `cargo test -p nes_source_runtime file_sink` | Wave 0 |
| VAL-02 | End-to-end source->sink systest | systest | Manual: run systest binary with TokioFileSink.test | Wave 0 |
| VAL-03 | Systest tool integration | integration | Covered by VAL-02 systest execution | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test -p nes_source_runtime --lib` (Rust changes) / `cmake --build cmake-build-debug --target nes-sinks -j` (C++ changes)
- **Per wave merge:** Full build + `ctest --test-dir cmake-build-debug -R "TokioSink|TokioSource" -j`
- **Phase gate:** Full suite green, systest passes

### Wave 0 Gaps
- [ ] `nes-sources/rust/nes-source-runtime/src/file_sink.rs` -- AsyncFileSink + unit tests for VAL-01
- [ ] `nes-systests/sources/TokioFileSink.test` -- systest file for VAL-02
- [ ] Extend existing `nes-sources/tests/TokioSinkTest.cpp` -- registry tests for REG-01
- [ ] Tokio `"fs"` feature: add to Cargo.toml

## Sources

### Primary (HIGH confidence)
- Direct code inspection of TokioSourceRegistry.hpp, TokioSourceGeneratedRegistrar.inc.in, TokioGeneratorSource.cpp -- exact pattern to mirror
- Direct code inspection of SourceProvider.cpp -- integration pattern to mirror
- Direct code inspection of SinkProvider.cpp, SinkRegistry.hpp -- current state to extend
- Direct code inspection of sink.rs, sink_context.rs, sink_handle.rs, lib.rs -- Rust framework to extend
- Direct code inspection of TokioSink.hpp, TokioSink.cpp -- C++ operator and SpawnFn pattern
- Direct code inspection of PluginRegistrationUtil.cmake -- CMake plugin system mechanics
- Direct code inspection of SystestBinder.cpp -- systest sink handling
- Direct code inspection of ExecutableQueryPlan.cpp -- where `lower()` is called

### Secondary (MEDIUM confidence)
- tokio::fs API -- well-known stable Tokio API, File::create/write_all/flush are standard

### Tertiary (LOW confidence)
- None -- all findings from direct code inspection

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries already in project, only adding "fs" feature
- Architecture: HIGH - exact mirror of existing source-side patterns with direct code evidence
- Pitfalls: HIGH - identified from direct analysis of code dependencies and CMake system
- Systest: MEDIUM - systest binder is complex; SinkValidation registration may have subtleties

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable -- mirrors existing patterns)
