# nes-validator WASM Module

Compiles the NES topology validator to WebAssembly using Emscripten + Embind, allowing the topology editor frontend to validate topologies in the browser without a backend.

## Prerequisites

- The emsdk is bundled at `../emsdk/` (checked into the repo)
- vcpkg at `/vcpkg_input/vcpkg_repository/` (available in the devcontainer)
- Ninja build system
- A native NES build (for up-to-date generated registrar files)

## Build Instructions

All commands run from `nes-topology-editor/wasm/`:

```bash
# 1. Set up emscripten environment
export EMSDK=$PWD/emsdk
export PATH="$EMSDK:$EMSDK/upstream/emscripten:$EMSDK/node/22.16.0_64bit/bin:$PATH"

# 2. Configure with vcpkg + emscripten chainloading
cmake -B build -G Ninja \
  -DCMAKE_TOOLCHAIN_FILE=/vcpkg_input/vcpkg_repository/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$EMSDK/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake \
  -DVCPKG_TARGET_TRIPLET=wasm32-emscripten \
  -DVCPKG_OVERLAY_TRIPLETS=$PWD/triplets \
  -DVCPKG_OVERLAY_PORTS=../../vcpkg/vcpkg-registry/ports \
  -DCMAKE_BUILD_TYPE=Release \
  "-DVCPKG_INSTALL_OPTIONS=--allow-unsupported"

# 3. Build
ninja -C build -j$(nproc)
```

The output is `build/nes-validator/nes-validator-wasm.mjs` (~5MB ES6 module with embedded WASM).

## How It Works

- **vcpkg** is the CMake toolchain file; it chainloads the **Emscripten** toolchain via the `wasm32-emscripten` triplet in `triplets/`
- Dependencies (antlr4, boost-url, yaml-cpp, spdlog, fmt, nlohmann-json, highs, etc.) are built for wasm32 by vcpkg
- NES source files are compiled directly (not linked against native NES libraries)
- Stubs in `stubs/` replace headers that can't compile for WASM (folly, protobuf, NES runtime types)
- Generated registrar files in `generated/` must be copied from a native build (e.g., `cmake-build-debug/nes-*/registry/templates/*.inc`)

## Updating Generated Registrar Files

When NES source/sink/operator registrations change, update the generated files:

```bash
# Build NES natively first, then copy:
cp cmake-build-debug/nes-query-optimizer/registry/templates/TraitGeneratedRegistrar.inc \
   cmake-build-debug/nes-logical-operators/registry/templates/*.inc \
   cmake-build-debug/nes-data-types/registry/templates/*.inc \
   cmake-build-debug/nes-sources/registry/templates/*.inc \
   cmake-build-debug/nes-sinks/registry/templates/*.inc \
   cmake-build-debug/nes-input-formatters/registry/templates/*.inc \
   nes-topology-editor/wasm/nes-validator/generated/
```

## Exported Functions (via Embind)

- `validateTopology(yaml: string): string` - returns empty string on success, error message on failure
- `explainTopology(yaml: string): string` - returns human-readable topology explanation
- `getRegisteredSourceTypes(): string[]`
- `getRegisteredSinkTypes(): string[]`
- `getSourceConfigFields(name: string): ConfigField[]`
- `getSinkConfigFields(name: string): ConfigField[]`
