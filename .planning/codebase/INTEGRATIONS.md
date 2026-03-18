# External Integrations

**Analysis Date:** 2026-03-13

## APIs & External Services

**Not Detected:**
- No external cloud APIs (AWS, GCP, Azure) are integrated
- No third-party SaaS integrations
- No public HTTP REST APIs consumed
- System is primarily self-contained

## Data Storage

**Databases:**
- Not detected - NebulaStream is a streaming data processing engine, not a primary data store
- Data sources are pluggable via SourceProvider interface (`nes-sources/include/Sources/SourceProvider.hpp`)

**File Storage:**
- Local filesystem only
  - File sources: `FileSource` class in `nes-sources/private/FileSource.hpp`
  - File sinks: `FileSink` class in `nes-sinks/include/Sinks/FileSink.hpp`
  - File I/O handled through standard C++ file APIs
- No cloud storage integration (S3, GCS, etc.)
- vcpkg Cache: S3-compatible optional caching for dependency builds (docker layer only)

**Caching:**
- In-memory buffer management:
  - Global buffer pool: Configurable via `number_of_buffers_in_global_buffer_manager` (default: 32768 buffers)
  - Per-source buffer backpressure: `default_max_inflight_buffers` (default: 64)
- Lock-free hash table: `libcuckoo` dependency for internal data structures
- No external cache service (Redis, Memcached, etc.)

## Authentication & Identity

**Auth Provider:**
- Custom/None - NebulaStream doesn't implement authentication
- gRPC communication uses **InsecureChannelCredentials** (no TLS/SSL)
  - Location: `nes-frontend/src/QueryManager/GRPCQuerySubmissionBackend.cpp`
  - Code: `grpc::CreateChannel(config.grpc.getRawValue(), grpc::InsecureChannelCredentials())`
- No user authentication, authorization, or session management implemented
- Suitable for internal/closed-network deployments

## Communication & RPC

**gRPC Services:**
- `WorkerRPCService` - Query submission and monitoring
  - Proto definition: `/IdeaProjects/nebulastream2/grpc/SingleNodeWorkerRPCService.proto`
  - Endpoints:
    - `RegisterQuery` - Submit query execution plan
    - `UnregisterQuery` - Remove query
    - `StartQuery` - Begin query execution
    - `StopQuery` - Halt query (graceful or hard stop)
    - `RequestQueryStatus` - Get query state and metrics
    - `RequestQueryLog` - Retrieve query execution history
    - `RequestStatus` - Get worker health and active queries
  - Channel: Default localhost:8080 or via `NES_WORKER_GRPC_ADDR` env var
  - Implementation: `GRPCQuerySubmissionBackend` in `nes-frontend/src/QueryManager/`

**Message Serialization:**
- Protobuf 3 protocol buffers
  - Query plans: `SerializableQueryPlan.proto`
  - Data types: `SerializableDataType.proto`
  - Variant descriptors: `SerializableVariantDescriptor.proto`

## Data Sources & Sinks

**Implemented Sources:**
- File-based data ingestion
  - Class: `FileSource` (`nes-sources/private/FileSource.hpp`)
  - Registry: `FileDataRegistry` for managing file-based data

**Implemented Sinks:**
- File output
  - Class: `FileSink` (`nes-sinks/include/Sinks/FileSink.hpp`)
- Console/print output
  - Class: `PrintSink` (`nes-sinks/include/Sinks/PrintSink.hpp`)

**Data Provider Interface:**
- Extensible architecture via `SourceProvider` and `SourceDataProvider`
  - Catalog: `SourceCatalog` manages registered data sources
  - Location: `nes-sources/include/Sources/`

## Monitoring & Observability

**Error Tracking:**
- Not detected - No external error tracking service integration
- In-process error logging and stack trace capture

**Logging:**
- **Framework:** spdlog (fast C++ logging library)
  - Dependency: `spdlog` in vcpkg.json
- **Stack Trace Capture:** cpptrace
  - Enabled via compile-time flag: `NES_LOG_WITH_STACKTRACE`
  - Location: Core settings in `CMakeLists.txt`
- **Log Levels:** Compile-time configurable
  - `NES_LOG_LEVEL` environment variable (TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, NONE)
  - Default: TRACE (dev), ERROR (release), WARN (relwithdebinfo)
  - Location: Logging setup in top-level `CMakeLists.txt`

**Observability:**
- Query execution metrics tracked via Protobuf:
  - `QueryMetrics` - Start time, running time, stop time, error info
  - Retrievable via `RequestQueryStatus` gRPC call
- No external metrics pipeline (Prometheus, Grafana, etc.)

## CI/CD & Deployment

**Hosting:**
- Self-hosted deployments
- Docker images for containerized deployment
  - Base runtime: Ubuntu 24.04
  - Development image: Pre-built vcpkg SDK in `nebulastream/nes-development-dependency`
  - Frontend apps: Separate docker images for CLI and REPL
  - Single-node worker: Embedded runtime image

**CI Pipeline:**
- Not detected in code - CI/CD managed externally
- Docker Compose available in `docker/` but not integrated

**Build & Deployment Artifacts:**
- CMake generates binaries and libraries
- Docker images available for distribution
  - `docker/frontend/cli.dockerfile` - CLI application
  - `docker/frontend/repl.dockerfile` - Interactive REPL
  - `docker/frontend/repl-embedded.dockerfile` - Embedded engine REPL
  - `docker/single-node-worker/SingleNodeWorker.dockerfile` - Worker runtime

## Environment Configuration

**Required env vars:**
- `NES_WORKER_GRPC_ADDR` - Worker gRPC endpoint (default: localhost:8080)
- Optional for docker development:
  - `NES_PREBUILT_VCPKG_ROOT` - Path to pre-built SDK
  - `VCPKG_DEPENDENCY_HASH` - Hash validation for dependency image compatibility
  - `VCPKG_STDLIB` - Standard library choice (libcxx or local)
  - `VCPKG_SANITIZER` - Sanitizer mode (asan, ubsan, none)

**Secrets location:**
- Not applicable - No external API keys or credentials required
- All configuration is environment-based or config files

## Webhooks & Callbacks

**Incoming:**
- Not detected - No webhook ingestion

**Outgoing:**
- Not detected - No callback/webhook dispatch to external services

## Build & Dependency Caching

**S3-Compatible Caching (Docker build layer):**
- Optional vcpkg binary cache for accelerating dependency builds
  - AWS CLI integration in Dependency.dockerfile
  - Environment: `VCPKG_CACHE_ACCESS_KEY`, `VCPKG_CACHE_SECRET_KEY`, `VCPKG_CACHE_ENDPOINT`, `VCPKG_CACHE_BUCKET`, `VCPKG_CACHE_REGION`
  - Read-only fallback: `VCPKG_CACHE_PUBLIC_URL`
- Used only during docker image construction, not runtime

## Third-Party Services Summary

| Service | Purpose | Status | Notes |
|---------|---------|--------|-------|
| Ubuntu 24.04 | Base OS | Used | Via Docker |
| LLVM/Clang 19 | Compiler | Used | Distributed in dev image |
| ANTLR 4 | SQL parsing | Used | Downloaded from antlr.org or pre-baked |
| gRPC/Protobuf | RPC & serialization | Used | Core communication layer |
| vcpkg | Dependency management | Used | All C++ dependencies managed here |
| Docker | Containerization | Used | Dev and runtime images |
| AWS S3 (optional) | Dep cache | Opt-in | Docker build optimization only |
| GitHub | Source hosting | External | Project repository |

---

*Integration audit: 2026-03-13*
