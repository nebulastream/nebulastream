# Rust CMake Integration: Multi-Crate Build System

## The Problem

Each Rust `staticlib` crate compiled by Cargo embeds its own copy of all dependencies — including the CXX runtime (`cxxbridge1$exception`, `cxxbridge1$slice$new`, etc.). If a C++ executable links two or more of these staticlibs directly, the linker sees duplicate definitions and fails.

The project has multiple Rust crates that produce CXX bindings, including
`nes_network_bindings`, `spdlog_bindings`, and `nes_rust_timestamp`. Many
executables transitively need more than one of them.

## The Solution: Deduplicated Umbrella Crates

Instead of linking individual Rust staticlibs into C++ executables, the build system generates **umbrella crates** that bundle all needed Rust crates into a single staticlib. Executables that need the exact same set of crates share a single umbrella — the crate set is hashed to produce a stable key, so 37 executables needing the same crates produce only 1 Cargo build.

## Unified Workspace

All Rust crates live in a single Cargo workspace rooted at `/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "nes-network/network",
    "nes-network/network-bindings",
    "nes-common/rust/spdlog-bindings",
    "nes-physical-operators/rust/cast-to-unix-timestamp",
]
```

This gives:
- **One `Cargo.lock`** — guaranteed version alignment across all crates (especially `cxx`)
- **One `target/` directory** — shared compilation, faster builds
- **One package baseline** — member versions and Rust editions inherit from `[workspace.package]`
- **One dependency catalog** — versions, features, and internal crate paths live in
  `[workspace.dependencies]`; member crates only use `{ workspace = true }`
- **Crate source stays colocated** with its C++ counterpart

The `Cargo.toml` is checked in (not auto-generated) to allow `cargo check`, `rust-analyzer`, and other Rust tooling to work without running CMake first.

Add or update dependencies only in the root `Cargo.toml`:

```toml
[workspace.dependencies]
tokio = { version = "1.52.0", features = ["macros", "rt-multi-thread"] }
```

Package defaults are centralized there too:

```toml
[workspace.package]
version = "0.1.0"
edition = "2024"
```

Member manifests opt in without repeating versions, features, or paths:

```toml
[dependencies]
tokio = { workspace = true }
nes_network = { workspace = true }
```

Every workspace dependency must be used by at least one member. This ensures it is
present in `Cargo.lock` and vendored into the development image as soon as it is added.
`scripts/check_rust_dependency_policy.py` enforces both rules before the
dependency-image hash is calculated. Consequently, member `Cargo.toml` files do not
need to participate in that hash: changing which already-vendored workspace dependency
a member uses cannot introduce an unvendored crate.

Running the dependency-image hash requires Python 3.11 or newer so the checker can use
the standard-library `tomllib` parser.

Each member inherits the package defaults:

```toml
[package]
name = "my_func"
version.workspace = true
edition.workspace = true
```

A single `rust-toolchain.toml` at the project root controls the Rust toolchain for all crates.

## Auto-Discovery

`EnableRust.cmake` uses `cargo metadata` to automatically discover all workspace crates and their types. Any crate with `staticlib` in its `crate-type` is imported by Corrosion and registered for umbrella generation. No manual crate lists in CMake are needed — just add your crate to the workspace `Cargo.toml`.

## CMake Integration

### Create a CXX Bridge

Use the NES wrapper for every Rust/CXX bridge:

```cmake
add_cxx_bridge(
    nes-rust-timestamp-bindings
    CRATE nes_rust_timestamp
    FILES cast_to_unix_timestamp.rs
)
```

The wrapper creates the bridge, makes its generated headers available through
the `nes-codegen` target, and routes its Rust symbols through the umbrella
library. These build-system details should not be repeated in individual crate
`CMakeLists.txt` files. `FILES` names the crate-root file relative to `src/`;
in this example the generated C++ header is
`<nes-rust-timestamp-bindings/cast_to_unix_timestamp.h>`.

### Declare Rust Requirements

C++ library targets declare which Rust crates they need using a custom INTERFACE property:

```cmake
target_link_rust_lib(nes-sources nes_network_bindings)
target_link_rust_lib(nes-common spdlog_bindings)
target_link_rust_lib(nes-physical-operators nes_rust_timestamp)
```

### Automatic Umbrella Generation

At the end of CMake configuration, the build system discovers executables and
collects their transitive Rust requirements. Each unique sorted crate set gets
one generated umbrella static library, shared by every executable with the same
requirements. Cargo therefore includes shared dependencies such as CXX only
once. No per-executable Rust configuration is required.

## Adding a New Rust Crate

Rust crates integrate like ordinary C++ library dependencies and can be used by
any C++ target. Functions, sources, and sinks are common integration points,
but the build system does not restrict Rust crates to those locations.

To add a new Rust crate:

### 1. Create the Rust crate

Place it anywhere in the source tree:

```
nes-my-component/rust/
  Cargo.toml
  src/my_func.rs    # #[cxx::bridge] mod ffi { ... }
```

Use a descriptive crate-root filename and declare it explicitly in the member
manifest:

```toml
[lib]
path = "src/my_func.rs"
crate-type = ["rlib", "staticlib"]
```

### 2. Add it to the root workspace

In `/Cargo.toml`:
```toml
members = [
    ...existing...,
    "nes-my-component/rust",
]
```

CMake auto-discovers it via `cargo metadata` — no changes to `EnableRust.cmake` needed. Add the crate to `[workspace.dependencies]` only when another workspace member consumes it.

### 3. Create the CXX bridge CMakeLists.txt

```cmake
add_cxx_bridge(
    my-func-bindings # name of the C++ side of the bridge library target
    CRATE my_func
    FILES my_func.rs
)
```

### 4. Wire it into the C++ library

```cmake
target_link_libraries(nes-my-component PRIVATE my-func-bindings)
target_link_rust_lib(nes-my-component my_func)
```

That's it. The umbrella system finds affected executables automatically.

## Rust Unit Tests in CTest

When `NES_ENABLES_TESTS` is enabled, CMake registers every workspace package as
`rust-<crate-name>`. Each entry runs `cargo test -p <crate-name>` from the root
workspace with the Cargo executable, Rust compiler, toolchain, and sanitizer
environment selected by Corrosion. The tests carry the `rust` label and are
serialized because they share one Cargo target directory.

## Key Constraints

- **Crate types**: Source crates must have `crate-type = ["staticlib", "rlib"]` — `staticlib` so Corrosion can import them for CXX header generation, `rlib` so the umbrella can use them as dependencies.
- **CXX version pinning**: The umbrella pins `cxx = "=<version>"` (auto-detected from the workspace) to match the `cxxbridge` CLI. A version mismatch would produce ABI incompatibilities.
- **Workspace-owned dependencies**: member manifests use `{ workspace = true }`; dependency versions, features, and paths belong in the root manifest.
- **No unused workspace dependencies**: every `[workspace.dependencies]` entry is consumed by at least one member so the development image vendors it immediately.
- **Umbrella crates opt out of the workspace**: Each generated umbrella includes `[workspace]` in its `Cargo.toml` to avoid being claimed by the root workspace.

## Key Files

| File | Purpose |
|------|---------|
| `/Cargo.toml` | Root workspace — lists all Rust crate members (checked in, manually maintained) |
| `/Cargo.lock` | Lockfile — ensures reproducible dependency versions across all crates |
| `/rust-toolchain.toml` | Rust toolchain version (single copy for the whole project) |
| `cmake/EnableRust.cmake` | Fetches Corrosion, auto-discovers crates via `cargo metadata`, configures sanitizers/env vars |
| `cmake/RustLibCollection.cmake` | Public helpers `add_cxx_bridge` and `target_link_rust_lib`, plus umbrella generation |
| `*/rust/CMakeLists.txt` | Per-crate bridge setup using `add_cxx_bridge` |
| `cmake-build-*/rust-umbrellas/` | Generated umbrella crates (one per unique crate-set hash) |
