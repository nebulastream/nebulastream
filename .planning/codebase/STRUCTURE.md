# Codebase Structure

**Analysis Date:** 2026-03-13

## Directory Layout

```
nebulastream2/
├── nes-sql-parser/             # SQL parsing and statement binding
├── nes-logical-operators/       # Logical operator definitions, schemas
├── nes-query-optimizer/         # Optimizer phases, trait system, rules
├── nes-physical-operators/      # Physical operator implementations
├── nes-query-compiler/          # Query-to-executable compilation
├── nes-query-engine/            # Task scheduler, query catalog, execution management
├── nes-runtime/                 # NodeEngine, pipelines, execution contexts
├── nes-executable/              # CompiledQueryPlan, ExecutablePipeline structures
├── nes-sources/                 # Source abstraction, catalog, descriptors
├── nes-sinks/                   # Sink abstraction, catalog, descriptors
├── nes-input-formatters/        # CSV, JSON, custom format parsers
├── nes-frontend/                # Query submission APIs, CLI, REPL
├── nes-single-node-worker/      # Standalone worker binary, gRPC server
├── nes-nautilus/                # Code generation interface, buffer refs, hashes
├── nes-common/                  # Identifiers, logging, utilities, windowing
├── nes-data-types/              # Type system, schema, serialization
├── nes-memory/                  # Memory allocation, TupleBuffer
├── nes-configurations/          # Configuration classes and parsing
├── nes-plugins/                 # Plugin implementations (JSON, TCP, Checksum)
├── nes-systests/                # End-to-end system test suite
├── grpc/                        # gRPC service definitions
├── cmake/                       # CMake build macros and helpers
├── docs/                        # Documentation, design proposals
└── scripts/                     # Build and setup scripts
```

## Directory Purposes

**nes-sql-parser:**
- Purpose: Parse SQL queries, validate against schema, produce logical plans
- Contains: ANTLR grammar files, AntlrSQLQueryParser, StatementBinder
- Key files: `include/SQLQueryParser/AntlrSQLQueryParser.hpp`, `include/SQLQueryParser/StatementBinder.hpp`

**nes-logical-operators:**
- Purpose: Define and manipulate logical query plans as operator trees
- Contains: Base LogicalOperator concept, specific operators (Selection, Projection, Join, Union, Aggregation, Sources, Sinks), window types
- Key files: `include/Operators/LogicalOperator.hpp`, subdirectories under `include/Operators/`

**nes-query-optimizer:**
- Purpose: Apply transformation rules to optimize logical/physical plans
- Contains: Phases (trait inference, matching, lowering), legacy optimizer, serialization, trait definitions
- Key files: `include/Traits/` (trait system), `private/Phases/`, `private/LegacyOptimizer/`

**nes-physical-operators:**
- Purpose: Implement runtime semantics for query operations
- Contains: Physical operators for aggregation (hashmaps), joins, functions, watermarks, slice stores
- Key files: `include/PhysicalOperator.hpp`, subdirectories: `Aggregation/`, `Join/`, `Functions/`, `Watermark/`, `SliceStore/`

**nes-query-compiler:**
- Purpose: Lower physical plans to executable code/IR
- Contains: Lowering rules translating operator semantics to generated code, compilation phases
- Key files: `include/QueryCompiler.hpp`, `private/LoweringRules/`, `private/Phases/`

**nes-query-engine:**
- Purpose: Manage query execution lifecycle and task scheduling
- Contains: QueryEngine (main coordinator), ThreadPool, QueryCatalog, RunningQueryPlan state tracking
- Key files: `include/QueryEngine.hpp`, `include/QueryEngineConfiguration.hpp`

**nes-runtime:**
- Purpose: Provide execution environment for compiled pipelines
- Contains: NodeEngine (entrance point), pipeline stages, execution contexts, buffer management coordination
- Key files: `include/Runtime/NodeEngine.hpp`, `include/Pipelines/CompiledExecutablePipelineStage.hpp`

**nes-executable:**
- Purpose: Define structures for compiled, executable query representations
- Contains: CompiledQueryPlan (abstract plan), ExecutablePipeline, sources and sinks structures
- Key files: `include/CompiledQueryPlan.hpp`, `include/ExecutablePipelineStage.hpp`

**nes-sources:**
- Purpose: Abstract data source ingestion
- Contains: Source interface (fillTupleBuffer), descriptors, provider factory, catalog, LogicalSource representation
- Key files: `include/Sources/Source.hpp`, `include/Sources/SourceDescriptor.hpp`, `include/Sources/SourceProvider.hpp`

**nes-sinks:**
- Purpose: Abstract result output
- Contains: Sink interface, descriptors, provider factory, catalog, format parsers (CSV, JSON, Print, File)
- Key files: `include/Sinks/Sink.hpp`, `include/Sinks/SinkDescriptor.hpp`

**nes-input-formatters:**
- Purpose: Deserialize various data formats into typed records
- Contains: InputFormatter interface, CSV/JSON/custom implementations, parser registry
- Key files: `include/InputFormatters/`, `registry/` for dynamic type support

**nes-frontend:**
- Purpose: Expose query submission and management APIs
- Contains: QueryManager (state + submission backend), CLI app, REPL app, backends for embedded and gRPC submission
- Key files: `include/QueryManager/QueryManager.hpp`, `apps/nes-repl/`, `apps/cli/`

**nes-single-node-worker:**
- Purpose: Standalone executable providing NodeEngine with gRPC API
- Contains: SingleNodeWorkerStarter, gRPC service bindings, configuration
- Key files: `src/SingleNodeWorkerStarter.cpp`, gRPC service definitions in `grpc/`

**nes-nautilus:**
- Purpose: Interface and utilities for generated code execution
- Contains: Record (row interface), RecordBuffer (batch interface), HashMap/PagedVector references, hash functions
- Key files: `include/Nautilus/Interface/Record.hpp`, `include/Nautilus/Interface/RecordBuffer.hpp`, `include/Nautilus/Interface/HashMap/`

**nes-common:**
- Purpose: Shared utilities and identifiers
- Contains: ID types (QueryId, OperatorId, etc.), Logger, windowing definitions, sequencing
- Key files: `include/Identifiers/Identifiers.hpp`, `include/Util/Logger/`, `include/Windowing/`

**nes-data-types:**
- Purpose: Type system and schema representation
- Contains: DataType hierarchy (Integer, Float, String, Array, etc.), Schema, serialization support
- Key files: `include/DataTypes/DataType.hpp`, `include/DataTypes/Schema.hpp`

**nes-memory:**
- Purpose: Memory management for execution
- Contains: BufferManager, TupleBuffer (fixed-size record container), memory allocation policies
- Key files: `include/Runtime/BufferManager.hpp` (in nes-runtime), but buffers defined in `nes-memory/`

**nes-configurations:**
- Purpose: Configuration loading and representation
- Contains: WorkerConfiguration, various subsystem configs
- Key files: `include/Configurations/`

**nes-plugins:**
- Purpose: Example and built-in plugin implementations
- Contains: JSONInputFormatter, TCPSource, VoidSink, ChecksumSink
- Location: `InputFormatters/`, `Sources/`, `Sinks/` subdirectories

**nes-systests:**
- Purpose: End-to-end test suite and benchmarks
- Contains: Test queries organized by category (operator, function, formatter, etc.), test framework, system test infrastructure
- Key files: Tests in YAML-like `.test` format, `systest/` contains C++ harness

**grpc:**
- Purpose: gRPC protocol definitions
- Contains: .proto files for SingleNodeWorkerRPCService, query plan serialization, data type serialization
- Key files: `SingleNodeWorkerRPCService.proto`, `SerializableQueryPlan.proto`

**cmake:**
- Purpose: Build system macros and configuration
- Contains: Helper macros, sanitizer setup, dependency imports
- Key files: `macros.cmake`, `ImportDependencies.cmake`, `Sanitizers.cmake`

## Key File Locations

**Entry Points:**
- `nes-single-node-worker/src/SingleNodeWorkerStarter.cpp`: Worker process startup
- `nes-frontend/apps/nes-repl/main.cpp`: REPL client main
- `nes-frontend/apps/cli/main.cpp`: CLI client main

**Core APIs:**
- `nes-sql-parser/include/SQLQueryParser/AntlrSQLQueryParser.hpp`: SQL → LogicalPlan
- `nes-query-optimizer/include/QueryOptimizer.hpp`: LogicalPlan → PhysicalPlan
- `nes-query-compiler/include/QueryCompiler.hpp`: PhysicalPlan → CompiledQueryPlan
- `nes-runtime/include/Runtime/NodeEngine.hpp`: Execution orchestration
- `nes-query-engine/include/QueryEngine.hpp`: Task scheduling and coordination
- `nes-frontend/include/QueryManager/QueryManager.hpp`: Query submission and state

**Configuration:**
- `nes-single-node-worker/interface/SingleNodeWorkerConfiguration.hpp`: Worker settings
- `nes-runtime/include/Runtime/NodeEngineBuilder.hpp`: Factory for NodeEngine
- `CMakeLists.txt`: Project-level build config

**Testing:**
- `nes-systests/systest/`: C++ test harness and infrastructure
- Individual operator tests: `nes-physical-operators/tests/`, `nes-query-optimizer/tests/`

## Naming Conventions

**Files:**
- Headers: `.hpp` (C++ headers, in `include/` directories)
- Implementation: `.cpp` (source files, in `src/` directories)
- Tests: `*Test.cpp` or `*Tests.cpp` (unit/component tests, in `tests/` or `tests/UnitTests/`)
- System tests: `.test` (YAML format queries with expected output)

**Directories:**
- `include/`: Public header declarations
- `src/`: Implementation files (mirrors include structure)
- `tests/`: Test suite (mirrors src structure)
- `private/`: Internal implementation headers not for external use
- `interface/`: Public-facing interfaces in modules with multiple concerns
- `registry/`: Plugin registries and templates for extensibility

**Classes/Types:**
- Operators end in "Operator": `SelectionLogicalOperator`, `AggregationPhysicalOperator`
- Descriptors end in "Descriptor": `SourceDescriptor`, `SinkDescriptor`
- Managers end in "Manager": `QueryManager`, `BufferManager`
- Providers end in "Provider": `SourceProvider`, `SinkProvider`, `InputFormatterProvider`
- Concepts end in "Concept": `LogicalOperatorConcept`, `PhysicalOperatorConcept`

**Functions/Methods:**
- camelCase for methods: `fillTupleBuffer()`, `getOutputSchema()`
- getters: `get*()` pattern
- setters: `set*()` or `with*()` (for immutable pattern)
- type queries: `is*()` or `has*()` for booleans

## Where to Add New Code

**New Query Operator (Logical):**
- Declare class in: `nes-logical-operators/include/Operators/[OperatorName]LogicalOperator.hpp`
- Implement in: `nes-logical-operators/src/Operators/[OperatorName]LogicalOperator.cpp`
- Add tests in: `nes-logical-operators/tests/`

**New Query Operator (Physical):**
- Declare class in: `nes-physical-operators/include/[Category]/[OperatorName]PhysicalOperator.hpp` (categories: Aggregation, Join, Functions, Watermark)
- Implement in: `nes-physical-operators/src/[Category]/[OperatorName]PhysicalOperator.cpp`
- Add tests in: `nes-physical-operators/tests/`

**New Source Connector:**
- Declare in: `nes-sources/include/Sources/[ConnectorName]Source.hpp`
- Implement in: `nes-sources/src/Sources/[ConnectorName]Source.cpp`
- Register in plugin: `nes-plugins/Sources/[ConnectorName]Source/` (if plugin-based)
- Add descriptor support in: `nes-sources/include/Sources/SourceDescriptor.hpp`

**New Sink Connector:**
- Declare in: `nes-sinks/include/Sinks/[ConnectorName]Sink.hpp`
- Implement in: `nes-sinks/src/Sinks/[ConnectorName]Sink.cpp`
- Register in plugin: `nes-plugins/Sinks/[ConnectorName]Sink/` (if plugin-based)

**New Input Format:**
- Declare in: `nes-input-formatters/include/InputFormatters/[FormatName]InputFormatter.hpp`
- Implement in: `nes-input-formatters/src/InputFormatters/[FormatName]InputFormatter.cpp`
- Register in plugin: `nes-plugins/InputFormatters/[FormatName]InputFormatter/`

**New Optimization Rule:**
- Declare in: `nes-query-optimizer/private/Phases/[RuleName]Rule.hpp`
- Implement in: `nes-query-optimizer/src/Phases/[RuleName]Rule.cpp`
- Add tests in: `nes-query-optimizer/tests/UnitTests/`

**New System Test:**
- Create `.test` file in: `nes-systests/[category]/[TestName].test` (categories: operator, function, formatter, etc.)
- Define expected input, SQL query, and output in YAML format
- Reference test data in: `nes-systests/testdata/` (small/ or large/)

## Special Directories

**cmake-build-debug/:**
- Purpose: Generated build artifacts and intermediate files
- Generated: Yes (created by CMake)
- Committed: No (.gitignore)

**nes-distributed/:**
- Purpose: (Generated during build) Distributed query management logic
- Generated: Yes
- Committed: No

**.nix/:**
- Purpose: Nix development environment configuration and helpers
- Generated: No
- Committed: Yes

**docs/:**
- Purpose: Architecture documentation, design proposals, development guides
- Generated: No
- Committed: Yes

**grpc_generated_src/ and antlr4_generated_src/:**
- Purpose: Generated code from .proto and ANTLR grammar files
- Generated: Yes (CMake target)
- Committed: No

---

*Structure analysis: 2026-03-13*
