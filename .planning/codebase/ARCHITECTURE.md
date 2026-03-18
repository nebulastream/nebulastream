# Architecture

**Analysis Date:** 2026-03-13

## Pattern Overview

**Overall:** Multi-layered stream processing engine with pipeline-based query execution, code generation via Nautilus, and plugin extensibility.

**Key Characteristics:**
- Query lifecycle separation: SQL parsing → logical planning → optimization → compilation → runtime execution
- Pipeline-centric execution model with task-based scheduling via ThreadPool
- Type-erased operator patterns using C++20 concepts for extensibility
- Nautilus code generation layer produces optimized machine code
- Modular source/sink providers with descriptor-based instantiation
- Distributed architecture via gRPC with single-node and multi-node execution modes

## Layers

**SQL Parser Layer:**
- Purpose: Convert SQL statements to logical representation, bind to schema
- Location: `nes-sql-parser`
- Contains: ANTLR-based SQL parser, statement binder, logical plan construction
- Depends on: Data types, schema definitions
- Used by: Frontend, query manager

**Logical Planning Layer:**
- Purpose: Represent queries as logical operator DAGs, schema propagation
- Location: `nes-logical-operators`
- Contains: Logical operators (Selection, Projection, Join, Union, Aggregation), window types, sources/sinks
- Depends on: Data types, schemas
- Used by: Query optimizer, query compiler

**Query Optimization Layer:**
- Purpose: Transform logical plans into optimized physical plans using rule-based optimization
- Location: `nes-query-optimizer`
- Contains: Optimization phases, trait system, legacy optimizer, serialization
- Depends on: Logical operators, traits framework
- Used by: Query compiler

**Physical Operators Layer:**
- Purpose: Represent executable operations with runtime semantics
- Location: `nes-physical-operators`
- Contains: Physical operators for aggregation, join, functions, watermarking, slice stores
- Depends on: Nautilus interface types, execution context
- Used by: Query compiler, runtime

**Query Compilation Layer:**
- Purpose: Transform physical plans to executable IR/machine code
- Location: `nes-query-compiler`
- Contains: Lowering rules for operators, compilation phases, MLIR backend support
- Depends on: Physical operators, Nautilus interface
- Used by: Query engine submission

**Execution Layer:**
- Purpose: Provide task scheduling, buffer management, and operator execution
- Location: `nes-query-engine`, `nes-runtime`, `nes-executable`
- Contains: QueryEngine (task coordinator), ThreadPool, QueryCatalog, RunningQueryPlan
- Depends on: Compiled query plans, buffer management, sources/sinks
- Used by: Worker nodes, single-node worker

**I/O Layer:**
- Purpose: Abstract source data ingestion and sink result output
- Location: `nes-sources`, `nes-sinks`, `nes-input-formatters`
- Contains: Source/Sink descriptors, providers, format parsers (CSV, JSON)
- Depends on: Data types, buffer management
- Used by: Runtime execution, pipeline instantiation

**Frontend Layer:**
- Purpose: Expose query submission APIs and client management
- Location: `nes-frontend`
- Contains: QueryManager with backends (embedded/gRPC), CLI and REPL apps
- Depends on: SQL parser, all compilation layers, runtime
- Used by: External clients

**Runtime Infrastructure:**
- Purpose: Memory buffers, configuration, logging, data types
- Location: `nes-common`, `nes-memory`, `nes-data-types`, `nes-configurations`
- Contains: TupleBuffer abstraction, logger, identifiers, type system
- Depends on: None (foundational)
- Used by: All layers

**Code Generation Interface (Nautilus):**
- Purpose: Low-level execution primitives and interfaces for generated code
- Location: `nes-nautilus`
- Contains: Record, RecordBuffer, HashMap/PagedVector references, hash functions
- Depends on: Data types, buffer management
- Used by: Physical operators, compiled pipeline stages

## Data Flow

**Query Submission to Execution:**

1. Frontend receives SQL via REPL/CLI/gRPC client
2. SQL parser (`nes-sql-parser`) tokenizes and binds to logical sources/sinks
3. Logical plan created as operator DAG with source → operators → sink structure
4. Query optimizer (`nes-query-optimizer`) applies rule-based transformations to optimize for traits (distribution, cardinality, resources)
5. Physical plan generated with operator-specific execution strategies
6. Query compiler (`nes-query-compiler`) lowers physical plan to executable stages using Nautilus interface
7. Compiled query plan packaged as set of executable pipelines with sources and sinks
8. QueryEngine instantiates sources/sinks via providers, creates ExecutableQueryPlan
9. ThreadPool distributes pipeline work as tasks via work queue
10. Each task executes operator logic compiled by Nautilus
11. Results flow through pipelines to sinks

**Buffer Flow:**
- SourceThread fills TupleBuffer from Source
- Buffer wrapped in Task, queued to ThreadPool
- Operator execute() methods process Record-by-record or batch
- Downstream pipelines consume via successor pipeline references
- Final pipelines write to Sink

**State Management:**
- QueryEngine maintains QueryCatalog of running queries
- Each query has RunningQueryPlan tracking operator instances and state
- Aggregation/Join operators maintain in-process state via SliceStore (hashmaps, paged vectors)
- Watermark state triggers window closures

## Key Abstractions

**LogicalOperator:**
- Purpose: Represent semantic operations on data streams
- Examples: `SelectionLogicalOperator` (`nes-logical-operators/include/Operators/SelectionLogicalOperator.hpp`), `ProjectionLogicalOperator`
- Pattern: Immutable DAG node with children, traits, schema propagation using C++20 TypedLogicalOperator wrapper
- Used for: Query validation, optimization rules, schema inference

**PhysicalOperator:**
- Purpose: Represent executable operations with runtime dispatch
- Examples: All files in `nes-physical-operators/include/` subdirectories
- Pattern: Type-erased via PhysicalOperatorConcept base, PIMPL pattern for concrete implementations
- Lifecycle: setup() → open() → execute()/batch_execute() → close() → terminate()

**SourceDescriptor/SinkDescriptor:**
- Purpose: Abstract specification of connectors independent of instantiation
- Examples: `nes-sources/include/Sources/SourceDescriptor.hpp`
- Pattern: Declarative config + provider factory pattern for late binding
- Used for: Distributed queries (serialize to remote workers), replay

**ExecutablePipeline:**
- Purpose: Represent a partition of execution between materialization points
- Examples: Compiled to stages in `nes-executable/include/CompiledQueryPlan.hpp`
- Pattern: Directed acyclic graph with source inputs, operator chain, sink outputs
- Properties: Independent scheduling unit, buffers exchanged via TupleBuffer

**Trait System:**
- Purpose: Propagate physical properties (distribution, sort order, resource constraints)
- Location: `nes-query-optimizer/include/Traits`
- Pattern: TraitSet attached to logical/physical operators, rules reason about trait requirements
- Examples: DistributionTrait, SortTrait

## Entry Points

**Single Node Worker:**
- Location: `nes-single-node-worker/src/SingleNodeWorkerStarter.cpp`
- Triggers: Program startup with config
- Responsibilities: Instantiate NodeEngine, source/sink providers, gRPC service for remote submissions

**REPL Client:**
- Location: `nes-frontend/apps/nes-repl/main.cpp`
- Triggers: User SQL input
- Responsibilities: Connect to worker, submit parsed queries, poll status

**Query Submission (Embedded):**
- Location: Frontend QueryManager → EmbeddedWorkerQuerySubmissionBackend
- Triggers: Client calls registerQuery()
- Responsibilities: Validate logical plan, compile, instantiate, queue to QueryEngine

**Query Submission (Remote gRPC):**
- Location: Frontend QueryManager → GRPCQuerySubmissionBackend
- Triggers: Remote worker submission request
- Responsibilities: Serialize descriptor-based plan, transmit via gRPC

## Error Handling

**Strategy:** Result types (std::expected<T, Exception>) throughout public APIs for propagation; compile-time assertions for invariants.

**Patterns:**
- Parser: Validation errors on malformed SQL, schema binding failures
- Optimizer: Trait conflicts, unsupported operation combinations
- Compiler: Code generation failures (MLIR lowering), recursive operator cycles
- Runtime: Buffer exhaustion, source connection failures → logged, query terminated
- QueryEngine: Stop token propagation for graceful shutdown

## Cross-Cutting Concerns

**Logging:** Centralized logger in `nes-common/include/Util/Logger` with compile-time log level filtering (TRACE/DEBUG/INFO/WARN/ERROR/FATAL_ERROR/NONE). Build type determines level (Debug=TRACE, Release=ERROR, Benchmark=NONE).

**Validation:** Schema validation at parse time and compile time; runtime buffer bounds checking via TupleBuffer interface.

**Authentication:** gRPC transport only; no built-in query-level auth (delegated to deployment).

---

*Architecture analysis: 2026-03-13*
