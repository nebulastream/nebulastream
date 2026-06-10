# Rust CMake Integration: Multi-Crate Build System

## The Problem

Each Rust `staticlib` crate compiled by Cargo embeds its own copy of all dependencies — including the CXX runtime (`cxxbridge1$exception`, `cxxbridge1$slice$new`, etc.). If a C++ executable links two or more of these staticlibs directly, the linker sees duplicate definitions and fails.

The project has multiple Rust crates that produce CXX bindings (e.g. `nes_network_bindings`, `nes_buffer_bindings`, `nes_source_bindings`, `nes_sink_bindings`, `nes_rust_answer`). Many executables transitively need all of them.

## The Solution: Deduplicated Umbrella Crates

Instead of linking individual Rust staticlibs into C++ executables, the build system generates **umbrella crates** that bundle all needed Rust crates into a single staticlib. Executables that need the exact same set of crates share a single umbrella — the crate set is hashed to produce a stable key, so 37 executables needing the same crates produce only 1 Cargo build.

## Unified Workspace

All Rust crates live in a single Cargo workspace rooted at `/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "nes-sources/rust/nes-buffer-bindings",
    "nes-sources/rust/nes-source-bindings",
    "nes-sources/rust/nes-sink-bindings",
    "nes-sources/rust/nes-source-runtime",
    "nes-network/nes-network-bindings",
    "nes-network/network",
    "nes-network/nes-network-bindings/network",
    "nes-common/rust/spdlog-bindings",
    "nes-plugins/Functions/RustAnswer/rust",
]
```

This gives:
- **One `Cargo.lock`** — guaranteed version alignment across all crates (especially `cxx`)
- **One `target/` directory** — shared compilation, faster builds
- **Crate source stays colocated** with its C++ counterpart

The `Cargo.toml` is checked in (not auto-generated) to allow `cargo check`, `rust-analyzer`, and other Rust tooling to work without running CMake first.

A single `rust-toolchain.toml` at the project root controls the Rust toolchain for all crates.

## Auto-Discovery

`EnableRust.cmake` uses `cargo metadata` to automatically discover all workspace crates and their types. Any crate with `staticlib` in its `crate-type` is imported by Corrosion and registered for umbrella generation. No manual crate lists in CMake are needed — just add your crate to the workspace `Cargo.toml`.

## The Three Layers

### Layer 1: Corrosion Import + Link Severing

`EnableRust.cmake` auto-discovers staticlib crates and imports them:

```cmake
corrosion_import_crate(
    MANIFEST_PATH ${PROJECT_SOURCE_DIR}/Cargo.toml
    CRATES ${_staticlib_crates}    # auto-discovered via cargo metadata
    CRATE_TYPES staticlib
)
```

Corrosion creates CMake targets and normally wires the Rust staticlib as a link dependency of the CXX bridge C++ target. But that's what we DON'T want — it would cause duplicate symbols. So each bridge target immediately **severs** that automatic link:

```cmake
corrosion_add_cxxbridge(nes-buffer-bindings CRATE nes_buffer_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-buffer-bindings nes_buffer_bindings)   # <-- sever
```

After this, `nes-buffer-bindings` is a C++ library that provides the generated CXX header files and the C++ glue code — but it carries **no Rust symbols**. Those come from the umbrella.

`cxxbridge_remove_rust_link` handles both `nes_buffer_bindings` and the `-static` suffixed target name that Corrosion may use internally.

### Layer 2: Declarative Rust Requirements (INTERFACE properties)

C++ library targets declare which Rust crates they need using a custom INTERFACE property:

```cmake
target_link_rust_lib(nes-sources nes_buffer_bindings)
target_link_rust_lib(nes-sources nes_source_bindings)
target_link_rust_lib(nes-sources nes_sink_bindings)
target_link_rust_lib(nes-common  nes_network_bindings)
```

This sets `INTERFACE_NES_RUST_LIBS` on the target. It's purely metadata — no actual linking happens. The property propagates transitively through `target_link_libraries` chains, so if `nes-distributed` links `nes-sources` and `nes-common`, it inherits all Rust crate requirements.

### Layer 3: Deferred Deduplicated Umbrella Generation

Each executable that needs Rust calls:

```cmake
target_link_rust(nes-cli)
```

This registers the executable for deferred processing. The actual work happens via a **double-deferred** CMake call (so it runs after all plugin registration):

```cmake
cmake_language(DEFER ... CALL _defer_finalize_rust_links)
  -> cmake_language(DEFER ... CALL _finalize_rust_links)
```

When `_finalize_rust_links` runs, for each registered executable it:

1. **Walks the transitive dependency graph** (`collect_transitive_rust_libs`) — follows `LINK_LIBRARIES` and `INTERFACE_LINK_LIBRARIES` recursively, collecting every `INTERFACE_NES_RUST_LIBS` value.

2. **If only 1 crate**: links the Corrosion staticlib directly (no duplication possible).

3. **If 2+ crates**: sorts the crate list and hashes it (MD5). If an umbrella for that hash already exists, reuses it. Otherwise generates one at `cmake-build-debug/rust-umbrellas/<hash>/`:

   ```toml
   # Auto-generated Cargo.toml
   [package]
   name = "nes_umbrella_9813de0fc3e5"
   [lib]
   crate-type = ["staticlib"]
   [workspace]                        # opts out of root workspace
   [dependencies]
   cxx = "=1.0.194"                   # pinned to match cxxbridge CLI
   nes_network_bindings = { path = "..." }
   nes_buffer_bindings = { path = "..." }
   nes_source_bindings = { path = "..." }
   nes_sink_bindings = { path = "..." }
   ```

   ```rust
   // Auto-generated src/lib.rs
   extern crate nes_network_bindings;
   extern crate nes_buffer_bindings;
   extern crate nes_source_bindings;
   extern crate nes_sink_bindings;
   ```

4. Calls `corrosion_import_crate()` on the umbrella, then links it to the executable.

Cargo resolves all shared dependencies (CXX, tokio, serde, etc.) once, producing a single `.a` with no duplicates.

## Visual Flow

```
nes-cli (executable)
  |-- links: nes-sources (C++ lib)
  |    |-- links: nes-buffer-bindings  (CXX bridge, C++ only, no Rust symbols)
  |    |-- links: nes-source-bindings  (CXX bridge, C++ only)
  |    |-- links: nes-sink-bindings    (CXX bridge, C++ only)
  |    '-- INTERFACE_NES_RUST_LIBS: [nes_buffer_bindings, nes_source_bindings, nes_sink_bindings]
  |-- links: nes-common (C++ lib)
  |    '-- INTERFACE_NES_RUST_LIBS: [nes_network_bindings]
  '-- target_link_rust(nes-cli) --> hashes crate set --> reuses or generates:
       nes_umbrella_9813de0fc3e5 (single staticlib, shared by all exes with same crate set)
         depends on: nes_network_bindings + nes_buffer_bindings + nes_source_bindings + ...
         --> Cargo deduplicates CXX, tokio, etc.
         --> one .a linked into nes-cli (and every other exe needing the same crates)
```

## Adding a New Rust Crate

To add a new Rust crate that backs a C++ plugin (e.g. a function, source, or sink):

### 1. Create the Rust crate

Place it anywhere in the source tree:

```
nes-plugins/Functions/MyFunc/rust/
  Cargo.toml    # crate-type = ["rlib", "staticlib"]
  src/lib.rs    # #[cxx::bridge] mod ffi { ... }
```

### 2. Add it to the root workspace

In `/Cargo.toml`:
```toml
members = [
    ...existing...,
    "nes-plugins/Functions/MyFunc/rust",
]
```

CMake auto-discovers it via `cargo metadata` — no changes to `EnableRust.cmake` needed.

### 3. Create the CXX bridge CMakeLists.txt

```cmake
corrosion_add_cxxbridge(my-func-bindings CRATE my_func FILES lib.rs)
cxxbridge_remove_rust_link(my-func-bindings my_func)
```

### 4. Wire it into the C++ library

```cmake
target_link_libraries(nes-physical-operators PRIVATE my-func-bindings)
target_link_rust_lib(nes-physical-operators my_func)
```

### 5. Executables

Any executable that transitively depends on the C++ library must have `target_link_rust()`. Most already do — only add it for new test binaries.

That's it. The umbrella system picks up the new crate automatically.

## Key Constraints

- **Crate types**: Source crates must have `crate-type = ["staticlib", "rlib"]` — `staticlib` so Corrosion can import them for CXX header generation, `rlib` so the umbrella can use them as dependencies.
- **CXX version pinning**: The umbrella pins `cxx = "=<version>"` (auto-detected from the workspace) to match the `cxxbridge` CLI. A version mismatch would produce ABI incompatibilities.
- **Every executable needs `target_link_rust()`**: If you add a new test or binary that transitively depends on Rust, you must call this or you'll get either missing symbols or duplicate symbols.
- **Umbrella crates opt out of the workspace**: Each generated umbrella includes `[workspace]` in its `Cargo.toml` to avoid being claimed by the root workspace.

## Key Files

| File | Purpose |
|------|---------|
| `/Cargo.toml` | Root workspace — lists all Rust crate members (checked in, manually maintained) |
| `/Cargo.lock` | Lockfile — ensures reproducible dependency versions across all crates |
| `/rust-toolchain.toml` | Rust toolchain version (single copy for the whole project) |
| `cmake/EnableRust.cmake` | Fetches Corrosion, auto-discovers crates via `cargo metadata`, configures sanitizers/env vars |
| `cmake/RustLibCollection.cmake` | Core machinery: `target_link_rust_lib`, `target_link_rust`, `cxxbridge_remove_rust_link`, deduplicated umbrella generation |
| `*/rust/CMakeLists.txt` | Per-crate bridge setup: `corrosion_add_cxxbridge` + `cxxbridge_remove_rust_link` |
| `cmake-build-*/rust-umbrellas/` | Generated umbrella crates (one per unique crate-set hash) |
