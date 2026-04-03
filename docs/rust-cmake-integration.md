# Rust CMake Integration: Multi-Crate Build System

## The Problem

Each Rust `staticlib` crate compiled by Cargo embeds its own copy of all dependencies — including the CXX runtime (`cxxbridge1$exception`, `cxxbridge1$slice$new`, etc.). If a C++ executable links two or more of these staticlibs directly, the linker sees duplicate definitions and fails.

The project has 4 Rust crates that produce bindings: `nes_rust_bindings` (networking), `nes_buffer_bindings`, `nes_source_bindings`, and `nes_sink_bindings`. Many executables (like `nes-cli`) transitively need all of them.

## The Solution: Per-Executable Umbrella Crates

Instead of linking individual Rust staticlibs into C++ executables, the build system generates a **single umbrella crate per executable** that depends on all needed Rust crates as **rlib** (Rust library) dependencies. Cargo compiles the umbrella into one staticlib, deduplicating shared dependencies internally.

## The Three Layers

### Layer 1: Corrosion Import + Link Severing

In `EnableRust.cmake`:

```cmake
corrosion_import_crate(MANIFEST_PATH nes-sources/rust/Cargo.toml
    CRATES nes_buffer_bindings nes_source_bindings nes_sink_bindings
    CRATE_TYPES staticlib)
```

Corrosion imports the Rust crates and creates CMake targets. Normally, it also wires the Rust staticlib as a link dependency of the CXX bridge C++ target (so `nes-buffer-bindings` would link `nes_buffer_bindings-static`).

But that's exactly what we DON'T want. So each bridge target immediately **severs** that automatic link:

```cmake
corrosion_add_cxxbridge(nes-buffer-bindings CRATE nes_buffer_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-buffer-bindings nes_buffer_bindings)   # <-- sever
```

After this, `nes-buffer-bindings` is a C++ library that provides the generated CXX header files and the C++ glue code (`BufferBindings.cpp`) — but it carries **no Rust symbols**. Those will come later from the umbrella.

### Layer 2: Declarative Rust Requirements (INTERFACE properties)

C++ library targets declare which Rust crates they need using a custom INTERFACE property:

```cmake
target_link_rust_lib(nes-sources nes_buffer_bindings)
target_link_rust_lib(nes-sources nes_source_bindings)
target_link_rust_lib(nes-sources nes_sink_bindings)
target_link_rust_lib(nes-common  nes_rust_bindings)
```

This sets `INTERFACE_NES_RUST_LIBS` on the target. It's purely metadata — no actual linking happens. The property propagates transitively through `target_link_libraries` chains, so if `nes-distributed` links `nes-sources` and `nes-common`, it inherits all four Rust crate requirements.

### Layer 3: Deferred Umbrella Generation (at executable level)

Each executable that needs Rust calls:

```cmake
target_link_rust(nes-cli)
```

This doesn't link anything immediately. It just registers the executable in a global list. The actual work happens via `cmake_language(DEFER)` — specifically a **double-defer** so it runs after all other deferred calls (like plugin registration):

```cmake
cmake_language(DEFER ... CALL _defer_finalize_rust_links)
  -> cmake_language(DEFER ... CALL _finalize_rust_links)
```

When `_finalize_rust_links` finally runs, for each registered executable it:

1. **Walks the transitive dependency graph** (`collect_transitive_rust_libs`) — follows `LINK_LIBRARIES` and `INTERFACE_LINK_LIBRARIES` recursively, collecting every `INTERFACE_NES_RUST_LIBS` value it finds.

2. **If only 1 crate**: links the Corrosion staticlib directly (no duplication possible).

3. **If 2+ crates**: generates a per-exe umbrella crate at `cmake-build-debug/rust-umbrellas/<exe>/`:

   ```toml
   # Auto-generated Cargo.toml
   [package]
   name = "nes_umbrella_nes_cli"
   [lib]
   crate-type = ["staticlib"]
   [dependencies]
   cxx = "=1.0.194"                    # pinned to match cxxbridge CLI
   nes_rust_bindings = { path = "..." }
   nes_buffer_bindings = { path = "..." }
   nes_source_bindings = { path = "..." }
   nes_sink_bindings = { path = "..." }
   ```

   ```rust
   // Auto-generated src/lib.rs
   extern crate nes_rust_bindings;
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
  |    '-- INTERFACE_NES_RUST_LIBS: [nes_rust_bindings]
  '-- target_link_rust(nes-cli) --> generates:
       nes_umbrella_nes_cli (single staticlib)
         depends on: nes_rust_bindings + nes_buffer_bindings + nes_source_bindings + nes_sink_bindings
         --> Cargo deduplicates CXX, tokio, etc.
         --> one .a linked into nes-cli
```

## Key Constraints

- **Crate types**: Source crates must have `crate-type = ["staticlib", "rlib"]` — `staticlib` for when they're the only crate, `rlib` for when they're dependencies of an umbrella.
- **CXX version pinning**: The umbrella pins `cxx = "=1.0.194"` to match the `cxxbridge` CLI that generated the C++ header files. A version mismatch would produce ABI incompatibilities.
- **Every executable needs `target_link_rust()`**: If you add a new test or binary that transitively depends on Rust, you must call this or you'll get either missing symbols (no Rust linked) or duplicate symbols (raw staticlibs leaking through).

## Key Files

| File | Purpose |
|------|---------|
| `cmake/EnableRust.cmake` | Fetches Corrosion, imports Rust crates, registers them, configures sanitizers/env vars |
| `cmake/RustLibCollection.cmake` | Core machinery: `target_link_rust_lib`, `target_link_rust`, `cxxbridge_remove_rust_link`, umbrella generation |
| `nes-sources/rust/*/CMakeLists.txt` | Per-crate bridge setup: `corrosion_add_cxxbridge` + `cxxbridge_remove_rust_link` |
| `cmake-build-debug/rust-umbrellas/` | Generated umbrella crates (one per executable) |
