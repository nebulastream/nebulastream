# Architecture

**Analysis Date:** 2026-03-14

## Pattern Overview

**Overall:** Layered query processing pipeline with compile-to-native-code execution model.

**Key Characteristics:**
- SQL queries compiled through logical → optimized → physical → executable plans
- Source-side code generation using Nautilus framework for low-latency execution
- Task-based asynchronous scheduling with backpressure-aware buffering
- Plugin-based extensibility for sources, sinks, operators, and optimization rules
- Reference-counted TupleBuffer zero-allocation memory management

## Layers

**SQL Parsing Layer:**
- Purpose: Parse SQL queries into logical plans using ANTLR grammar
- Location: `nes-sql-parser/`
- Contains: ANTLR grammar definitions, SQL binder, parser implementation
- Depends on: ANTLR runtime, common utilities
- Used by: Frontend CLI/REPL applications

**Logical Plan Layer:**
- Purpose: Represent query semantics independent of execution strategy
- Location: `nes-logical-operators/include/Operators/`, `Plans/LogicalPlan.hpp`
- Contains: Logical operators (Selection, Projection, Join, Aggregation, Source, Sink), window definitions, watermark assignment
- Depends on: Configurations, data types, identifiers
- Used by: Query optimizer, query compiler

**Optimization Layer:**
- Purpose: Transform logical plans into hardware-aware optimized plans using traits and rules
- Location: `nes-query-optimizer/include/`
- Contains: OptimizedPlan, trait system (PlacementTrait, ImplementationTypeTrait, MemoryLayoutTypeTrait), optimization rules registry
- Depends on: Logical operators, configurations, HIGHS linear programming solver
- Used by: Query compiler, distributed query planner

**Query Compiler Layer:**
- Purpose: Compile optimized plans to executable pipelines using code generation via Nautilus
- Location: `nes-query-compiler/include/QueryCompiler.hpp`, `nes-nautilus/`
- Contains: QueryCompiler (pure function: OptimizedPlan → CompiledQueryPlan), LoweringRule registry, code generation infrastructure
- Depends on: Physical operators, Nautilus IR framework, input formatters
- Used by: SingleNodeWorker, distributed execution coordinators

**Runtime Execution Layer:**
- Purpose: Execute compiled query plans with task-based scheduling and buffer management
- Location: `nes-runtime/include/Runtime/`, `nes-query-engine/include/`
- Contains: NodeEngine (entry point), QueryEngine (task scheduling), ExecutableQueryPlan (instantiated runtime artifacts), ExecutablePipelineStage, CompiledExecutablePipelineStage
- Depends on: Memory management, sources, sinks, physical operators, network (for distributed)
- Used by: SingleNodeWorker, frontend applications

**Physical Operators Layer:**
- Purpose: Implement actual query operations at runtime (stateful processing, window building/probing)
- Location: `nes-physical-operators/include/`
- Contains: SourcePhysicalOperator, SelectionPhysicalOperator, WindowBuildPhysicalOperator, WindowProbePhysicalOperator, AggregationBuildPhysicalOperator, AggregationProbePhysicalOperator, aggregation functions (Sum, Count, Min, Max, Avg, Median)
- Depends on: Operator state management, Nautilus interfaces
- Used by: Query compiler, runtime execution

**Memory Management Layer:**
- Purpose: Manage zero-copy buffer allocation and lifecycle with reference counting
- Location: `nes-memory/include/Runtime/`
- Contains: TupleBuffer (reference-counted memory wrapper), BufferManager (fixed-size buffer pool), BufferRecycler (lifecycle management), VariableSizedAccess (variable-length data handling)
- Depends on: Identifiers, error handling
- Used by: All runtime and I/O components

**Data Types Layer:**
- Purpose: Define data type system with serialization support
- Location: `nes-data-types/include/DataTypes/`, `nes-data-types/registry/`
- Contains: Basic types (Int8, Int16, Int32, Int64, Float, Double, Text, Boolean), complex types, serialization backend implementations
- Depends on: Common utilities
- Used by: All query processing components

**I/O Layer:**
- Purpose: Connect external sources and sinks to query engine
- Location: `nes-sources/include/Sources/`, `nes-sinks/include/Sinks/`
- Contains: Source (abstract interface returning FillTupleBufferResult), SourceProvider (catalog), SourceHandle (query-specific source wrapper), Sink (output handler with backpressure), SinkProvider (catalog)
- Depends on: Memory, physical operators, error handling
- Used by: Runtime, sources/sinks plugins

**Common Utilities:**
- Purpose: Shared infrastructure across all layers
- Location: `nes-common/include/`
- Contains: Identifiers (QueryId, TupleBufferId, etc.), logging, error handling, timestamps, threading utilities, reflection
- Depends on: C++ stdlib
- Used by: All other layers

## Data Flow

**Query Registration & Compilation:**

1. Frontend (CLI/REPL) receives SQL string
2. SQL Parser (`AntlrSQLQueryParser`) creates LogicalPlan from SQL text
3. Query Optimizer transforms LogicalPlan → OptimizedPlan (applying placement, implementation, memory layout traits)
4. Query Compiler takes OptimizedPlan and emits CompiledQueryPlan (contains executable pipeline stages via code generation)
5. Runtime instantiates CompiledQueryPlan → ExecutableQueryPlan (creates actual Source, Pipeline, and Sink instances)
6. NodeEngine registers ExecutableQueryPlan with QueryEngine

**Query Execution:**

1. SourceProvider creates Source instances for each logical source in query
2. Sources fill TupleBuffers by parsing input data (CSV, JSON, MQTT, JDBC, etc.)
3. Each filled TupleBuffer triggers Task creation via QueryEngine
4. QueryEngine TaskQueue distributes tasks to ThreadPool workers
5. Workers execute CompiledExecutablePipelineStage code (generated native code from Nautilus)
6. Physical operators process batches: Selection filters, Projection transforms, Window builds state, Aggregation computes, etc.
7. Results flow to Sink stages which apply output formatting (CSV, JSON) and backpressure
8. Source receives backpressure signal, pauses filling if downstream buffers full
9. Sinks emit results to external systems

**State Management:**

- Window state (intermediate aggregations) stored in memory pools managed by OperatorState
- Aggregation slices maintained in OperatorState during build phase, probed during emit phase
- Variable-sized data (text, collections) stored in TupleBuffer with VariableSizedAccess pointers
- Query metadata tracked in QueryLog (lifecycle events), QueryCatalog (running queries)

## Key Abstractions

**TupleBuffer:**
- Purpose: Thread-safe, reference-counted batch storage for records
- Examples: `nes-memory/include/Runtime/TupleBuffer.hpp`
- Pattern: Zero-copy passing by value between components, automatic recycling when refcount reaches zero, supports nested buffers

**ExecutablePipelineStage:**
- Purpose: Unit of executable code in compiled query plan
- Examples: `nes-runtime/include/Pipelines/CompiledExecutablePipelineStage.hpp`, physical operators implementing this interface
- Pattern: Stateful stage processes TupleBuffer input, emits TupleBuffer output, manages operator state

**Plugin Registry:**
- Purpose: Extensible registration of custom sources, sinks, operators, optimization rules
- Examples: LogicalOperatorRegistry, SourceRegistry, SinkCatalog, LoweringRuleRegistry, TraitRegistry
- Pattern: Compile-time code generation via CMake templates registers plugins, runtime lookup by identifier

**OptimizedPlan with Traits:**
- Purpose: Represent query execution plan with hardware-aware metadata
- Examples: PlacementTrait (where to execute), ImplementationTypeTrait (which algorithm), MemoryLayoutTypeTrait (row vs column)
- Pattern: Each logical operator decorated with trait metadata, traits composed into TraitSet, traits influence lowering rules

**Source/Sink Abstraction:**
- Purpose: Unified interface for diverse data connectors
- Examples: `nes-sources/include/Sources/Source.hpp`, `nes-sinks/include/Sinks/Sink.hpp`
- Pattern: Sources yield FillTupleBufferResult (either Data with size or EoS marker), Sinks receive TupleBuffer with backpressure control

## Entry Points

**SingleNodeWorker:**
- Location: `nes-single-node-worker/include/SingleNodeWorker.hpp`
- Triggers: Programmatic initialization of worker with configuration
- Responsibilities:
  - Accepts LogicalPlan from compiled query
  - Orchestrates optimization and compilation
  - Manages NodeEngine lifecycle
  - Provides query lifecycle API (registerQuery, startQuery, stopQuery, unregisterQuery)

**NodeEngine:**
- Location: `nes-runtime/include/Runtime/NodeEngine.hpp`
- Triggers: Called by SingleNodeWorker to manage runtime state
- Responsibilities:
  - Registers/unregisters CompiledQueryPlan instances
  - Starts/stops queries (transitions between StoppedState and RunningState)
  - Provides BufferManager and QueryLog access
  - Coordinates with QueryEngine and SourceProvider

**GrpcService:**
- Location: `nes-single-node-worker/include/GrpcService.hpp`
- Triggers: Remote clients submit queries via gRPC
- Responsibilities:
  - Receives SQL queries over gRPC
  - Interfaces with SingleNodeWorker for query registration
  - Returns QueryId and status to clients

**Frontend CLI/REPL:**
- Location: `nes-frontend/apps/cli/`, `nes-frontend/apps/repl/`
- Triggers: User enters SQL commands
- Responsibilities:
  - Parses SQL via AntlrSQLQueryParser
  - Submits query to worker (local or remote)
  - Receives results, formats output

## Error Handling

**Strategy:** Exception-based with expected<T, Exception> return types for fallible operations.

**Patterns:**
- Most public APIs return `std::expected<T, Exception>` for error recovery
- Internal APIs throw exceptions (C++ try-catch)
- QueryLog tracks query termination states (Success, Failed, Stopped)
- Error handler registered in QueryEngine logs fatal errors with optional stacktrace (controlled by NES_LOG_WITH_STACKTRACE cmake flag)

## Cross-Cutting Concerns

**Logging:** Compile-time log level filtering (TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR) set via NES_LOG_LEVEL cmake flag. Configured per-module (ENGINE_LOG_LEVEL for query-engine). All logging via NES_LOG_* macros from `nes-common/include/Util/Logger/`.

**Validation:** Configuration validation in `nes-configurations/src/Configurations/Validation/`. Source/Sink configuration properties validated at registration time via SourceValidationProvider and SinkDescriptor property validation.

**Authentication:** Not built in; deferred to frontend/gRPC layer. Workers accept authenticated client requests.

**Monitoring:** QueryLog captures all query state transitions. StatisticListener tracks execution metrics (throughput, latency). GoogleEventTracePrinter emits Chrome trace format for visualization.

---

*Architecture analysis: 2026-03-14*
