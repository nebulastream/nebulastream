# Technology Stack

**Analysis Date:** 2026-03-14

## Languages

**Primary:**
- C++ 23 - Core streaming engine and runtime implementation
- Rust 2024 - Network layer bindings and asynchronous networking
- Java 21 - Build tooling and runtime dependencies (OpenJDK)

**Secondary:**
- Python 3 - Development utilities and benchmarking scripts
- Shell Script - Build automation and CI/CD workflows

## Runtime

**Environment:**
- Linux x86_64 and aarch64 (ARM64) - Primary deployment targets
- Ubuntu 24.04 - Base container environment for runtime

**Compiler Toolchain:**
- LLVM 19 - Primary C++ compiler via Clang
- Clang 19 - C and C++ compilation
- libc++ - Standard library (instead of libstdc++)
- Mold 2.37.1 - Fast linker

## Package Manager & Build Tools

**Build System:**
- CMake 3.31.11+ - C++ project configuration and building
- Ninja - Parallel build backend
- pkg-config - Dependency metadata lookup

**Dependency Management:**
- vcpkg - C++ dependency package manager (via Nix integration)
- Nix Flakes - Development environment and reproducible builds

**Caching/Acceleration:**
- ccache - C++ compiler caching
- sccache - Distributed compiler cache for CI

## Frameworks & Core Libraries

**Streaming/Query Processing:**
- MLIR (Multi-Level Intermediate Representation) - Query compilation and optimization backend
- Located at: `nes-query-optimizer/`, `nes-physical-operators/`

**RPC & Communication:**
- gRPC - Remote procedure calls for worker coordination
- Protobuf (proto3) - Message serialization format
- Version from CMakeLists.txt: Latest compatible with LLVM 19

**Data Serialization:**
- nlohmann/json - JSON parsing and generation
- simdjson 4.0.7 - High-performance JSON parsing
- CBOR, JSON - Via tokio-serde for Rust network layer

**Async Runtime (Rust):**
- Tokio 1.40.0 - Async runtime with multi-threaded executor

**Logging & Tracing:**
- spdlog - Structured logging framework
- cpptrace - Exception stack trace handling
- tracing (Rust) - Structured logging in Rust components
- Google Event Trace - Performance profiling for single-node worker

**Configuration:**
- yaml-cpp - YAML configuration parsing
- Located at: `nes-configurations/`

**SQL & Query Parsing:**
- ANTLR 4.13.2 - SQL parser generator
- Custom parser for NES SQL dialect
- Located at: `nes-sql-parser/`

**Optimization & Algorithms:**
- Folly - Meta's C++ library for algorithms and utilities
- Abseil C++ - Google's base library for C++ (concurrent data structures, utilities)
- Intel TBB - Threading Building Blocks for parallelism
- HiGHS - High-performance optimization solver

**Testing:**
- Google Test (GTest) - C++ unit testing framework
- BATS - Bash Automated Testing System for shell scripts
- Configured in CMakeLists.txt with coverage support

**Utilities:**
- Magic Enum - Compile-time enum reflection
- nameof - Getting variable names at compile-time
- argparse - Command-line argument parsing
- scope_guard - RAII resource management
- replxx - Interactive REPL library for CLI
- Howard Hinnant Date - Date/time library

**System Libraries:**
- OpenSSL - Cryptography and TLS support
- libffi - Foreign function interface
- libxml2 - XML parsing
- zstd - Compression
- zlib - Additional compression
- libuuid - UUID generation
- libdwarf - Debug information handling
- Boost - Additional C++ utilities

## Configuration & Build

**Environment Configuration:**
- CMakeLists.txt - Main build configuration
- `.clang-format` - Code formatting style definitions
- `.clang-tidy` - Static analysis configuration
- flake.nix - Nix development environment declarative specification
- Locations: `/tmp/tokio-sources/CMakeLists.txt`, `/tmp/tokio-sources/.clang-format`, `/tmp/tokio-sources/flake.nix`

**Build Options:**
- NES_BUILD_NATIVE - Native CPU architecture optimizations (AVX2 detection for x86_64)
- NES_ENABLES_TESTS - Enable/disable test compilation (default: ON)
- CODE_COVERAGE - Enable code coverage reporting
- NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR - Enable MLIR backend (default: ON)
- NES_LOG_WITH_STACKTRACE - Include stack traces in logs (default: ON)
- CMAKE_BUILD_TYPE - Release, Debug, RelWithDebInfo, Benchmark

**Log Levels:**
- Compilation-time configuration: TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, NONE
- Default levels by build type:
  - Debug/Default: TRACE
  - RelWithDebInfo: WARN
  - Release: ERROR
  - Benchmark: LEVEL_NONE

**C++ Standard:**
- C++23 with strict compiler flags
- Treats warnings as errors (multiple -Werror flags)
- Frame pointers enabled for debugging and profiling

## Platform Requirements

**Development:**
- LLVM 19 toolchain (Clang, lld)
- CMake 3.21.0+
- Git
- Linux kernel headers
- ccache for build caching
- Optional: CLion IDE support via `.nix/nix-run.sh` wrapper

**Production Deployment:**
- libc++ 19 runtime library
- gRPC health probe binary v0.4.40 - Container health checks
- Located at: `/tmp/tokio-sources/docker/runtime/Runtime.dockerfile`

**Container Base Images:**
- Development image: `nebulastream/nes-development` (includes full LLVM/CMake toolchain)
- Runtime image: `nebulastream/nes-runtime-base` (minimal, only libc++ + utilities)
- Dependency image: `nebulastream/nes-development-dependency` (pre-built vcpkg dependencies)

## Architecture-Specific Details

**x86_64 Compilation:**
- Default march: x86-64
- mtune: generic
- AVX2 detection and optimization if NES_BUILD_NATIVE=ON

**ARM64 (aarch64) Compilation:**
- Default march: armv8-a
- Supported on same LLVM 19 toolchain

## Database & State

**No persistent database:**
- NebulaStream is an in-memory streaming engine
- Configuration stored in YAML files (not a database)
- State management via protocol buffer serialization
- Query plans serialized via Protobuf for distribution

---

*Stack analysis: 2026-03-14*
