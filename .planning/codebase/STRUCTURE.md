# Codebase Structure

**Analysis Date:** 2026-03-14

## Directory Layout

```
nebulastream/
├── cmake/                          # CMake build infrastructure
│   ├── macros.cmake               # Custom CMake functions
│   ├── ImportDependencies.cmake   # External dependency management
│   ├── Sanitizers.cmake           # Address/memory sanitizer config
│   ├── EnableRust.cmake           # Rust interop setup
│   └── NebulaStreamTest.cmake     # Test infrastructure
├── docker/                         # Container images for deployment
│   ├── dependency/                # Build dependency images
│   ├── frontend/                  # Frontend container
│   ├── runtime/                   # Runtime base image
│   └── single-node-worker/        # Worker container
├── grpc/                           # Protocol buffer definitions for gRPC
├── scripts/                        # Utility scripts (Docker setup, etc.)
├── docs/                           # Developer documentation
│   ├── design/                    # Architecture decision records
│   ├── development/               # Dev environment setup
│   ├── guide/                     # Feature guides (extensibility, etc.)
│   ├── technical/                 # Deep technical dives
│   └── organizational/            # Team processes
├── CMakeLists.txt                 # Top-level build config
│── nes-common/                     # Shared utilities (logging, identifiers, error handling)
├── nes-configurations/            # Configuration schemas and validation
├── nes-data-types/                # Data type definitions and serialization
├── nes-distributed/               # Distributed query planning
├── nes-executable/                # Backpressure and execution framework
├── nes-frontend/                  # SQL frontend CLI/REPL
├── nes-input-formatters/          # Input parsing plugins (CSV, JSON, etc.)
├── nes-logical-operators/         # Logical query operators
├── nes-memory/                    # TupleBuffer and buffer management
├── nes-nautilus/                  # Code generation framework (IR, lowering)
├── nes-network/                   # Network communication (gRPC, Rust bindings)
├── nes-physical-operators/        # Runtime operator implementations
├── nes-plugins/                   # Plugin loading infrastructure
├── nes-query-compiler/            # Optimized plan → compiled pipeline compiler
├── nes-query-engine/              # Task scheduling and query lifecycle
├── nes-query-optimizer/           # Logical → optimized plan transformation
├── nes-runtime/                   # Runtime execution engine (NodeEngine)
├── nes-single-node-worker/        # Standalone worker process and gRPC service
├── nes-sinks/                     # Output connectors (File, Print, JDBC, etc.)
├── nes-sources/                   # Input connectors (File, CSV, MQTT, TCP, etc.)
├── nes-sql-parser/                # ANTLR SQL parsing
├── nes-systests/                  # System tests and test runner
└── vcpkg/                          # C++ package manager manifest
```

## Directory Purposes

**nes-common:**
- Purpose: Shared infrastructure used by all components
- Contains: Identifier types (QueryId, OperatorId, etc.), logging framework, error handling, string utilities, timer utilities, reflection system
- Key files: `include/Identifiers/`, `include/Util/Logger/`, `include/ErrorHandling.hpp`, `include/Thread.hpp`

**nes-data-types:**
- Purpose: Define query data types with serialization backends
- Contains: Primitive and complex data types, serialization implementations, type registry
- Key files: `include/DataTypes/`, `registry/` for type registration

**nes-sql-parser:**
- Purpose: Parse SQL strings into logical query plans
- Contains: ANTLR grammar, SQL binder, parser implementation
- Key files: `include/SQLQueryParser/AntlrSQLQueryParser.hpp`

**nes-logical-operators:**
- Purpose: Represent query operators at logical level (semantics independent of execution)
- Contains: LogicalOperator base class, Selection, Projection, Join, Aggregation, Source, Sink, Window operators
- Key files: `include/Operators/`, `registry/include/LogicalOperatorRegistry.hpp`

**nes-query-optimizer:**
- Purpose: Transform logical plans to optimized execution plans
- Contains: Traits (Placement, ImplementationType, MemoryLayout), optimization rules, plan transformation logic
- Key files: `include/QueryOptimizer.hpp`, `include/OptimizedPlan.hpp`, `include/Traits/`

**nes-query-compiler:**
- Purpose: Compile optimized plans to executable pipelines via Nautilus code generation
- Contains: QueryCompiler (pure function OptimizedPlan → CompiledQueryPlan), LoweringRule registry, lowering logic
- Key files: `include/QueryCompiler.hpp`, `include/PipelinedQueryPlan.hpp`

**nes-nautilus:**
- Purpose: Code generation framework converting query logic to native code
- Contains: Nautilus IR, code generation patterns, data structure interfaces (Record, PagedVector, HashMap), TupleBuffer proxy functions
- Key files: `include/Nautilus/` for IR and interfaces

**nes-physical-operators:**
- Purpose: Runtime operator implementations for execution
- Contains: Physical operator implementations matching logical operators, window/aggregation building and probing, operator state management
- Key files: `include/SourcePhysicalOperator.hpp`, `include/WindowBuildPhysicalOperator.hpp`, `include/Aggregation/`

**nes-memory:**
- Purpose: Memory management with zero-copy buffer pooling
- Contains: TupleBuffer (reference-counted batch), BufferManager (pool management), BufferRecycler, VariableSizedAccess
- Key files: `include/Runtime/TupleBuffer.hpp`, `include/Runtime/BufferManager.hpp`

**nes-runtime:**
- Purpose: Query execution engine with task scheduling
- Contains: NodeEngine (main entry point), ExecutableQueryPlan instantiation, ExecutablePipelineStage interface, listeners (QueryLog, StatisticListener)
- Key files: `include/Runtime/NodeEngine.hpp`, `include/ExecutableQueryPlan.hpp`

**nes-query-engine:**
- Purpose: Task scheduling and query lifecycle management
- Contains: QueryEngine (task queue management), RunningQueryPlan, Callback handling, Task definition
- Key files: `include/QueryEngine.hpp`

**nes-sources:**
- Purpose: Input connectors abstraction
- Contains: Source interface, SourceProvider, SourceCatalog, SourceDescriptor, SourceHandle, built-in source implementations
- Key files: `include/Sources/Source.hpp`, `include/Sources/SourceProvider.hpp`

**nes-sinks:**
- Purpose: Output connectors abstraction
- Contains: Sink interface, SinkProvider, SinkCatalog, SinkDescriptor, format handlers (CSV, JSON), built-in sink implementations
- Key files: `include/Sinks/Sink.hpp`, `include/Sinks/SinkProvider.hpp`

**nes-input-formatters:**
- Purpose: Data parsing from input sources
- Contains: RawValueParser, input format plugin registry, field delimiter/record delimiter handling
- Key files: `registry/include/` for formatter registry

**nes-single-node-worker:**
- Purpose: Standalone query processing worker
- Contains: SingleNodeWorker (public API), GrpcService (remote query submission), WorkerStatus tracking
- Key files: `include/SingleNodeWorker.hpp`, `include/GrpcService.hpp`

**nes-frontend:**
- Purpose: User-facing query interfaces
- Contains: CLI application, REPL application, query state backend
- Key files: `apps/cli/`, `apps/repl/`, test YAML topologies and SQL files

**nes-network:**
- Purpose: Network communication and Rust bindings
- Contains: gRPC service definitions, Rust interop bindings
- Key files: `grpc/` for proto definitions, `nes-rust-bindings/` for Rust wrapper

**nes-distributed:**
- Purpose: Distributed query planning across multiple workers
- Contains: DistributedLogicalPlan, WorkerCatalog, NetworkTopology, distributed query decomposition
- Key files: `include/DistributedLogicalPlan.hpp`, `include/WorkerCatalog.hpp`

**nes-configurations:**
- Purpose: Configuration schema definitions and validation
- Contains: Configuration classes for worker, query execution, source/sink properties
- Key files: `include/Configurations/`, `src/Configurations/Validation/`

**nes-plugins:**
- Purpose: Plugin loading and management
- Contains: Plugin loader, registry for custom sources/sinks/operators
- Key files: CMakeLists.txt defines plugin search paths

**nes-systests:**
- Purpose: System-level integration tests
- Contains: Test runner (systest binary), test case definitions (.test files), test utilities
- Key files: `systest/` for main executable, test cases in subdirectories

## Key File Locations

**Entry Points:**
- `nes-single-node-worker/src/SingleNodeWorkerStarter.cpp`: Worker process main()
- `nes-frontend/apps/cli/CLIStarter.cpp`: CLI application main()
- `nes-frontend/apps/repl/Repl.cpp`: REPL application main()

**Configuration:**
- `CMakeLists.txt`: Top-level build configuration (C++23, gRPC, Protobuf, dependencies)
- `.clang-format`: Code style (LLVM convention)
- `.clang-tidy`: Linting rules
- `flake.nix`: Nix development environment

**Core Logic - Query Pipeline:**
- `nes-sql-parser/include/SQLQueryParser/AntlrSQLQueryParser.hpp`: SQL to LogicalPlan
- `nes-query-optimizer/include/QueryOptimizer.hpp`: LogicalPlan to OptimizedPlan
- `nes-query-compiler/include/QueryCompiler.hpp`: OptimizedPlan to CompiledQueryPlan
- `nes-runtime/include/Runtime/NodeEngine.hpp`: Execution orchestration

**Core Logic - Execution:**
- `nes-query-engine/include/QueryEngine.hpp`: Task scheduling
- `nes-runtime/include/ExecutableQueryPlan.hpp`: Runtime query instance
- `nes-memory/include/Runtime/TupleBuffer.hpp`: Buffer management

**Testing:**
- `nes-common/tests/`: Unit tests for common utilities
- `nes-query-optimizer/tests/`: Optimizer tests
- `nes-query-compiler/tests/`: Compiler tests
- `nes-systests/`: System integration tests with test runner

## Naming Conventions

**Files:**
- Header files: `*.hpp` for C++ headers
- Source files: `*.cpp` for C++ implementations
- Test files: `*Test.cpp` or `*Tests.cpp` in `tests/` subdirectories
- Protocol buffers: `*.proto` in `grpc/`

**Directories:**
- Logical components prefixed with `nes-` (e.g., `nes-query-optimizer`)
- Within component: `include/` for public headers, `src/` for implementation, `tests/` for tests
- Within include: subdirectories mirror namespaces (e.g., `include/Operators/` for operators namespace)
- Private implementation in `private/` subdirectory when separating internal details

**Classes/Types:**
- CamelCase: `QueryEngine`, `LogicalOperator`, `TupleBuffer`
- Interfaces typically named with suffix (e.g., `ExecutablePipelineStage`)
- Identifiers use StrongType pattern: `QueryId`, `OperatorId`, `TupleBufferId`

**Namespaces:**
- Root namespace: `NES`
- Sub-namespaces match component: `NES::QueryCompilation`, `NES::AntlrSQLQueryParser`
- Detail/internal namespaces: `NES::detail`

## Where to Add New Code

**New Source/Sink Connector:**
- Implementation: `nes-sources/src/` or `nes-sinks/src/`
- Header: `nes-sources/include/Sources/` or `nes-sinks/include/Sinks/`
- Register in registry: Update CMakeLists.txt plugin registration
- Test: Create `*Test.cpp` in `nes-sources/tests/` or `nes-sinks/tests/`

**New Query Operator (Logical):**
- Implementation: `nes-logical-operators/src/Operators/`
- Header: `nes-logical-operators/include/Operators/`
- Register: Update LogicalOperatorRegistry CMakeLists.txt
- Tests: `nes-logical-operators/tests/`

**New Physical Operator Implementation:**
- Implementation: `nes-physical-operators/src/`
- Header: `nes-physical-operators/include/`
- Must implement ExecutablePipelineStage interface
- Tests: `nes-physical-operators/tests/`

**New Optimization Rule:**
- Implementation: `nes-query-optimizer/src/` (organize by rule category if needed)
- Header: `nes-query-optimizer/include/`
- Register in TraitRegistry: CMakeLists.txt `create_registries_for_component(Trait)`
- Tests: `nes-query-optimizer/tests/`

**New Lowering Rule (Compiler Rule):**
- Implementation: `nes-query-compiler/src/`
- Header: `nes-query-compiler/private/` for internal details
- Register in LoweringRuleRegistry: CMakeLists.txt `create_registries_for_component(LoweringRule)`
- Tests: `nes-query-compiler/tests/`

**Utilities/Helpers:**
- Shared across components: `nes-common/include/Util/` or `nes-common/include/` (new category)
- Component-specific: `[component]/src/` as internal header or `include/` if public

**Tests:**
- Unit tests: Colocated with code in `tests/UnitTests/`
- Integration tests: `nes-systests/` for system-level behavior
- Test utilities: `nes-common/tests/Util/include/BaseUnitTest.hpp`, `BaseIntegrationTest.hpp`

## Special Directories

**vcpkg/:**
- Purpose: C++ dependency management
- Generated: Yes (manifest created by vcpkg)
- Committed: Partially (manifest tracked, binary packages not)

**cmake/:**
- Purpose: CMake helper modules and macros
- Generated: No
- Committed: Yes

**.nix/:**
- Purpose: Nix package manager configuration
- Generated: No
- Committed: Yes
- Note: Provides reproducible dev environment

**docker/:**
- Purpose: Container image definitions
- Generated: No
- Committed: Yes
- Usage: Build via `docker build -f docker/[image]/[Image].dockerfile docker/[image]/`

**grpc/:**
- Purpose: Protocol buffer definitions
- Generated: Yes (generated C++ code from .proto)
- Committed: Only .proto source files

**.github/**
- Purpose: GitHub Actions CI/CD workflows
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-03-14*
