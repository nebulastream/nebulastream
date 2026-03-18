# Technology Stack

**Analysis Date:** 2026-03-13

## Languages

**Primary:**
- C++ 23 - Core query engine, runtime, and all backend systems
- C - System interfaces and low-level utilities

**Secondary:**
- Shell/Bash - Build scripts and infrastructure automation
- Python - Pre-build scripts and tooling (preamble checks, linting summaries)

## Runtime

**Environment:**
- LLVM/Clang - Compiler toolchain, version 19 (configurable via CMAKE)
- libc++ - C++ Standard Library (primary, libstdc++ available)
- CMake 3.21.0+ (modern: 3.31.11 in docker)
- Mold Linker 2.37.1 - High-performance linker
- Ninja - Default build generator

**Package Manager:**
- vcpkg - C++ package manager
- Lockfile: vcpkg.json (present at `/IdeaProjects/nebulastream2/vcpkg/vcpkg.json`)
- vcpkg manifest mode: Manages dependencies via overlay triplets and ports

## Frameworks

**Core:**
- gRPC 1.x+ - RPC communication for distributed query execution
  - Location: Core communication in `nes-frontend/src/QueryManager/GRPCQuerySubmissionBackend.cpp`
  - Proto definitions: `/IdeaProjects/nebulastream2/grpc/*.proto`
- Protobuf 3 - Message serialization and RPC contracts
  - Proto files: `SingleNodeWorkerRPCService.proto`, `SerializableQueryPlan.proto`, `SerializableDataType.proto`

**Query Processing:**
- ANTLR 4 - SQL parsing and lexical analysis
  - Grammar: `AntlrSQL.g4` (location: `nes-sql-parser/`)
  - Jar version: Dynamically fetched from ANTLR.org or pre-baked in docker
- LLVM/MLIR - Intermediate representation and compilation backend (optional feature)
  - Feature flag: `NES_ENABLE_PRECOMPILED_HEADERS`, `NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR`
  - Location: Configured via USE_LOCAL_MLIR or vcpkg MLIR feature

**Testing:**
- Google Test (GTest) - Unit and integration testing framework
  - Config: Enabled by default via `NES_ENABLES_TESTS` option
  - Runner: CMake test integration with `ctest`
- Code Coverage - Optional coverage analysis
  - Flag: `CODE_COVERAGE` - Generates coverage reports

**Build/Dev:**
- CMake 3.21.0+ - Primary build system
  - Main config: `/IdeaProjects/nebulastream2/CMakeLists.txt`
  - Per-module configs: Each `nes-*/CMakeLists.txt`
- Docker - Containerized development and runtime environments
  - Base: Ubuntu 24.04
  - Dev images: Dependency image with pre-built vcpkg SDK

## Key Dependencies

**Critical:**
- `protobuf` - Message serialization (via vcpkg)
- `grpc` - RPC framework and networking (via vcpkg)
- `antlr4` / `antlr4-runtime` - SQL parsing (via vcpkg)
- `gtest` - Testing framework (via vcpkg)
- `nautilus` - MLIR backend (via vcpkg, optional with mlir feature)

**Infrastructure:**
- `boost-asio` - Asynchronous I/O, networking primitives
- `boost-url` - URL parsing and manipulation
- `folly` - Facebook's C++ library (utilities, containers)
- `fmt` - Modern formatting library
- `spdlog` - Fast logging library
- `yaml-cpp` - YAML configuration parsing
- `nlohmann-json` - JSON parsing
- `libuuid` - UUID generation
- `libcuckoo` - Lock-free hash table
- `cpptrace` - Stack trace capture for debugging
- `scope-guard` - RAII scope guard utilities
- `replxx` - REPL library for interactive shells
- `argparse` - Command-line argument parsing
- `magic-enum` - Compile-time enum utilities
- `reflectcpp` - Reflection for C++
- `simdjson` - SIMD JSON parsing

## Configuration

**Environment:**
- CMake options drive build configuration:
  - `CMAKE_BUILD_TYPE`: Debug, Release, RelWithDebInfo, Benchmark
  - `NES_LOG_LEVEL`: TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, NONE (compile-time)
  - `NES_BUILD_NATIVE`: Enable native CPU instructions (mtune/march=native, AVX2 detection)
  - Feature flags: `NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR`, `ENABLE_DOCKER_TESTS`, etc.

**Build:**
- `CMakeLists.txt` - Main configuration
- `vcpkg.json` - Dependency manifest
- `vcpkg/` - Overlay triplets in `custom-triplets/`, overlay ports in `vcpkg-registry/ports/`
- `.nix/nix-cmake.sh`, `.nix/nix-run.sh` - Nix flake support (optional)

**Environment Variables:**
- `CMAKE_CXX_COMPILER` - Explicitly set to Clang (required)
- `LLVM_TOOLCHAIN_VERSION` - LLVM version override (default: 19)
- `NES_PREBUILT_VCPKG_ROOT` - Pre-built vcpkg SDK path (docker mode)
- `NES_USE_SYSTEM_DEPS` - Use external dependencies instead of vcpkg
- `USE_LOCAL_MLIR` - Use locally installed MLIR instead of vcpkg
- `CMAKE_GENERATOR` - Build tool (default in docker: Ninja)

## Platform Requirements

**Development:**
- OS: Ubuntu 24.04 (standard dev container)
- Architecture: x64 (x86-64) or arm64 (aarch64)
- Compiler: Clang 19+ (mandatory)
- Memory: Significant for compilation (vcpkg builds LLVM/MLIR)
- Build time: 30-60+ minutes depending on configuration

**Production:**
- Deployment target: Linux systems (ubuntu-based or custom)
- Runtime: CPU with optional AVX2 support detection
- Docker images available:
  - `nes-development-base` - Build environment
  - `nes-development-dependency` - Pre-built SDK
  - `nes-frontend-cli`, `nes-frontend-repl` - Frontend applications
  - `nes-single-node-worker` - Embedded worker runtime

## Compiler & Performance Tuning

**Default Flags:**
- C++ Standard: 23
- Warnings: Strict compilation with `-Werror` for most categories
- Frame pointers: `-fno-omit-frame-pointer` (always enabled for debugging)
- Position independent code: ON
- Optimization: None (-O0) by default in Debug, controllable per build type

**Special Build Types:**
- Benchmark: `-DNO_ASSERT`, native CPU tuning, `NES_LOG_LEVEL=NONE`
- Release: `-DNO_ASSERT`, `NES_LOG_LEVEL=ERROR` (if not overridden)
- RelWithDebInfo: `NES_LOG_LEVEL=WARN` (if not overridden)

---

*Stack analysis: 2026-03-13*
