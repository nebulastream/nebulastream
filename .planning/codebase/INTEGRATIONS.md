# External Integrations

**Analysis Date:** 2026-03-14

## APIs & External Services

**Inter-Service Communication:**
- gRPC WorkerRPCService - Worker-coordinator synchronous RPC
  - SDK/Client: gRPC C++ (grpcpp)
  - Proto definitions: `grpc/SingleNodeWorkerRPCService.proto`
  - Services: RegisterQuery, UnregisterQuery, StartQuery, StopQuery, RequestQueryStatus, RequestQueryLog, RequestStatus
  - Auth: None (internal network protocol)

**Performance Monitoring:**
- Google Event Trace - Distributed tracing and profiling
  - Implementation: `nes-single-node-worker/src/GoogleEventTracePrinter.cpp`
  - Traces query execution timeline and performance metrics

## Data Storage

**Databases:**
- None - NebulaStream is a pure in-memory streaming engine
- State: Transient, not persisted to external storage

**Configuration Storage:**
- YAML files - Human-readable configuration
  - Parsed by: yaml-cpp library
  - Location: Configuration loading in `nes-configurations/include/Configurations/BaseConfiguration.hpp`
  - Usage: Worker configuration, coordinator configuration, query parameters

**File Storage:**
- Local filesystem only
  - File sink output: `nes-sinks/include/Sinks/FileSink.hpp`
  - Formats: CSV, JSON
  - No cloud storage integration detected

**Message Serialization:**
- Protocol Buffers (proto3) - Structured message format
  - Generated from: `grpc/*.proto` files
  - Types: SerializableQueryId, SerializableQueryPlan, SerializableDataType, SerializableVariantDescriptor
  - CBOR, JSON, serde_json - Tokio serialization formats for network layer

**Caching:**
- None - All processing is streaming/real-time
- Compiler cache: ccache/sccache for build artifacts only

## Authentication & Identity

**Auth Provider:**
- None - No external authentication system
- Internal network security: Relies on network isolation
- gRPC communication: No explicit auth tokens or TLS configuration detected in service definitions

**Configuration Access:**
- No user management system
- Environment variables: Not detected in forbidden file list (`.env` files not present)
- Command-line configuration: Arguments parsed via argparse

## Data Input Sources

**Input Data Sources:**
- Streaming sources (pluggable)
  - Source catalog: `nes-sources/include/Sources/SourceCatalog.hpp`
  - Source descriptor: `nes-sources/include/Sources/SourceDescriptor.hpp`
  - Custom source plugins via plugin system

**Data Output Sinks:**
- File-based sinks
  - CSV format: `nes-sinks/src/SinksParsing/CSVFormat.cpp`
  - JSON format: `nes-sinks/src/SinksParsing/JSONFormat.cpp`
- Sink catalog: `nes-sinks/include/Sinks/SinkCatalog.hpp`
- No cloud storage sinks (S3, GCS) detected

## Monitoring & Observability

**Error Tracking:**
- None (no external service like Sentry)
- In-process exception handling with cpptrace
  - Stack trace generation: `grpc/SingleNodeWorkerRPCService.proto` error messages include stackTrace field
  - Location: `nes-single-node-worker/src/GrpcService.cpp` error handlers

**Logs:**
- spdlog-based structured logging
  - Log levels: TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR
  - Compilation-time level configuration
  - No log aggregation service detected
  - Output: stderr/stdout, potentially rotatable files

**Query Status Tracking:**
- In-process query status listener pattern
  - Base class: `nes-runtime/include/Listeners/AbstractQueryStatusListener.hpp`
  - Query log: `nes-runtime/include/Listeners/QueryLog.hpp`
  - Metrics tracked: Start time, running time, stop time, error information

**Profiling:**
- Google Event Trace format output
  - Generator: `nes-single-node-worker/src/GoogleEventTracePrinter.cpp`
  - Can be imported into Chrome DevTools or other trace viewers

## CI/CD & Deployment

**Hosting:**
- Docker container images (self-hosted or registry push)
- Container images:
  - Single-node worker: `docker/single-node-worker/SingleNodeWorker.dockerfile`
  - Frontend CLI: `docker/frontend/cli.dockerfile`
  - Frontend REPL: `docker/frontend/repl.dockerfile`, `docker/frontend/repl-embedded.dockerfile`
  - Development image: `docker/dependency/Development.dockerfile`
  - Benchmarking image: `docker/dependency/Benchmark.dockerfile`
  - CI runner image: `docker/dependency/DevelopmentCI.dockerfile`
  - Runtime base: `docker/runtime/Runtime.dockerfile`

**CI Pipeline:**
- GitHub Actions (inferred from `.github/` directory presence)
- Build artifacts: CMake binaries (nes-single-node-worker, nes-cli, nes-repl)
- Docker build caching with CCACHE_DIR and mold linker optimization
- Multi-stage builds for reduced image size
- Test execution: CTest via CMake
- Code coverage: Optional via CODE_COVERAGE CMake flag

**Build Infrastructure:**
- Nix Flakes - Reproducible development environments
- vcpkg - Package manager with S3 caching support detected in Dockerfiles
- sccache - Distributed build caching in CI images

## Environment Configuration

**Required Configuration (No env vars detected):**
- Build-time: CMake flags passed via command-line
- Runtime: YAML configuration files
- Worker startup: `SingleNodeWorkerConfiguration` loaded from YAML
- Coordinator startup: Likely similar YAML-based configuration

**Secrets Location:**
- No secrets management system detected
- Assumption: Operators manage TLS certificates and network security externally
- gRPC communication appears to be unencrypted (internal network assumption)

## Webhooks & Callbacks

**Incoming Webhooks:**
- None detected
- Query submission: Direct gRPC calls only

**Outgoing Webhooks:**
- None detected
- Event listeners: In-process pattern (AbstractQueryStatusListener)
- No external callback endpoints

## Network Communication

**Internal Protocol Stack:**
- gRPC over HTTP/2 - Worker-coordinator communication
  - Proto files: `grpc/SingleNodeWorkerRPCService.proto`
  - Status tracking: QueryState (Registered, Started, Running, Stopped, Failed)
  - Metadata: Error codes, stack traces, diagnostic information passed via gRPC trailing metadata

**Rust Network Layer:**
- Tokio async runtime with multi-threaded executor
- tokio-util codec framework
- async-channel for message passing
- tokio-serde for serialization (CBOR, JSON formats)
- tokio-retry2 with jitter and tracing support
- Network bindings: `nes-network/nes-rust-bindings/network/`

**Message Format:**
- gRPC messages (Protobuf wire format)
- Serialized query plans: Binary Protobuf format
- Status messages: QueryStatusReply, QueryLogReply structures

## External Tool Integrations

**IDE Support:**
- CLion - Via `.nix/nix-run.sh` wrapper for Nix-based toolchain

**Development Tools:**
- ClangBuildAnalyzer - Build time analysis (installed in Development image)
- Format validation: clang-format (LLVM 19)
- Static analysis: clang-tidy (LLVM 19)

**Documentation Generation:**
- None detected - markdown-only (manual docs in `/docs/`)

---

*Integration audit: 2026-03-14*
