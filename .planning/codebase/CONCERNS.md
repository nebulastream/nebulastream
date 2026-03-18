# Codebase Concerns

**Analysis Date:** 2026-03-14

## Tech Debt

**Memory Alignment Issues in UB Sanitizer:**
- Issue: Undefined behavior sanitizer catches memory alignment violations that must be disabled
- Files: `cmake/Sanitizers.cmake`
- Impact: Memory alignment bugs silently pass in sanitized builds, potentially hiding real UB issues
- Fix approach: Audit codebase for alignment requirements (especially with vectorized code in `nes-nautilus`) and fix root causes instead of disabling sanitizer checks. Reference Issue #799.

**Logger Shutdown Crash in System Tests:**
- Issue: `spdlog::shutdown()` call causes crash in system test cleanup, so it's commented out
- Files: `nes-common/src/Util/Logger/NesLogger.cpp` (lines 201-204)
- Impact: Logger resources are not properly cleaned up, potential memory leaks in long-running system tests
- Fix approach: Investigate root cause in test teardown infrastructure (may involve FFI cleanup order with network code). Ensure proper static destruction ordering. Reference Issue #348.

**Network Connection Resource Leak:**
- Issue: Connections are never removed from active connections map, even when all channels close
- Files: `nes-network/network/src/sender/control.rs` (lines 533-536)
- Impact: Connection handlers and keepalive tasks persist until full NetworkService shutdown, causing resource exhaustion for short-lived connections. In long-running distributed systems, could accumulate hundreds of zombie connection objects.
- Fix approach: Implement connection cleanup when `ActiveTokens` becomes empty. Add reference counting to track active channel count per connection.

**Per-Operator Lexical Scope Hack in SQL Parser:**
- Issue: Column names in schema declarations must be qualified with source names as temporary workaround
- Files: `nes-sql-parser/src/CommonParserFunctions.cpp` (line 291), `nes-sql-parser/src/CommonParserFunctions.cpp` (line 65)
- Impact: Parser is fragile to schema changes; refactoring source names breaks queries. Schema inference logic is incomplete.
- Fix approach: Implement proper per-operator schema inference without name qualification requirements. Reference Issue #764.

**OperatorHandler in Pipeline Execution Context:**
- Issue: OperatorHandler map should not be part of pipeline execution context interface
- Files: `nes-executable/include/PipelineExecutionContext.hpp` (lines 62-64)
- Impact: Creates coupling between pipeline execution and operator management; makes it difficult to refactor execution model
- Fix approach: Remove OperatorHandler management from PipelineExecutionContext and create separate operator lifecycle management. Reference Issue #30.

**Sink Binding in Legacy Optimizer:**
- Issue: Sink binding logic remains in legacy optimizer instead of new query binder
- Files: `nes-query-optimizer/src/LegacyOptimizer/SinkBindingRule.cpp` (line 34)
- Impact: Dual code paths for sink binding; new optimizer improvements don't propagate to legacy path
- Fix approach: Complete migration of sink binding to new query binder system. Reference Issue #897.

**Identifier Qualification in Type System:**
- Issue: Type identifiers use qualified names when unqualified should work
- Files: `nes-sql-parser/src/StatementBinder.cpp` (line 120)
- Impact: Type system is overly complex; refactoring type names requires updating all uses
- Fix approach: Simplify identifier resolution in type system. Reference Issue #764.

**Tuple Delimiter Size Limitation:**
- Issue: Tuple delimiters cannot exceed one byte in size
- Files: `nes-sources/src/SourceDescriptor.cpp` (line 52), `nes-input-formatters/tests/UnitTests/SpecificSequenceTest.cpp` (line 102)
- Impact: Binary sources with multi-byte delimiters fail silently; one disabled test shows this is known limitation
- Fix approach: Extend delimiter handling to support variable-length delimiters. Add support for escaped delimiters. Reference Issue #651 and #1154.

---

## Known Bugs

**Sequence Number Acknowledgment Tracking:**
- Symptoms: Sequence numbers may not be properly acknowledged when channels close
- Files: `nes-network/network/src/receiver/channel.rs` (lines 69-71)
- Trigger: Channel closure with pending acknowledgments
- Workaround: None documented; system continues but may lose delivery guarantees

**Channel Close Propagation:**
- Symptoms: Close command may fail to propagate to remote side; remote will attempt reconnection
- Files: `nes-network/network/src/receiver/channel.rs` (lines 81-86)
- Trigger: Network partition during close operation
- Workaround: Reconnection attempts will fail with control socket rejection. Consider tombstone tokens.

**Span Field Overwriting in Logging Context:**
- Symptoms: When a nested span overwrites a parent span's field key, the key is removed from parent
- Files: `nes-network/nes-rust-bindings/spdlog/lib.rs` (lines 162-163)
- Trigger: Logging spans with duplicate field names in nested contexts
- Workaround: Currently not an issue because duplicate keys don't exist in actual usage

**Source Buffer Starvation:**
- Symptoms: Sources sometimes need an extra buffer for unknown reasons; pipeline may deadlock
- Files: `nes-input-formatters/tests/UnitTests/SmallFilesTest.cpp` (line 214)
- Trigger: Specific source combinations with small input files
- Workaround: Allocate additional buffers for sources. Actual root cause unknown. Reference Issue #774.

**Multipart Delimiter Tests Disabled:**
- Symptoms: Test for delimiters larger than one character is disabled and never runs
- Files: `nes-input-formatters/tests/UnitTests/SpecificSequenceTest.cpp`
- Trigger: Multipart tuple delimiters in input formatters
- Workaround: None; functionality simply not implemented

---

## Security Considerations

**Unsafe Rust in FFI Bindings:**
- Risk: Unsafe extern "C++" blocks for C++ interop may allow memory violations if C++ code violates contract
- Files: `nes-network/nes-rust-bindings/network/lib.rs` (line 58), `nes-network/nes-rust-bindings/spdlog/lib.rs` (lines 41, 55-56)
- Current mitigation: Limited to internal networking and logging systems; not user-facing
- Recommendations: Add safety documentation for each unsafe block; create safe wrapper types; consider reducing C++ FFI surface area

**Exception Handling Coverage:**
- Risk: Some exception handlers catch all exceptions without specific type
- Files: `nes-frontend/apps/repl/Repl.cpp` (line 535)
- Current mitigation: Exceptions are logged
- Recommendations: Use more specific exception types; avoid bare catch blocks. Audit all C++ exception handlers.

**Input Validation in SQL Parser:**
- Risk: Parser handles untrusted SQL input; injection vectors unclear
- Files: `nes-sql-parser/` (multiple files, especially `AntlrSQLQueryPlanCreator.cpp`)
- Current mitigation: ANTLR-based parsing should prevent injection through grammar
- Recommendations: Add explicit tests for malicious inputs; implement query size limits; rate-limit parsing

---

## Performance Bottlenecks

**Large Monolithic Test Files:**
- Problem: Test files exceeding 1000 lines indicate complex test scenarios
- Files: `nes-query-engine/tests/QueryEngineTest.cpp` (1381 lines), `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp` (1017 lines), `nes-systests/systest/src/SystestBinder.cpp` (1011 lines)
- Cause: Multiple test scenarios combined; parsing and test setup time increased
- Improvement path: Refactor into smaller test suites; parametrize tests; improve test infrastructure speed

**Compilation Time with 134 CMake Files:**
- Problem: 134 CMakeLists.txt files indicate deep dependency tree and incremental rebuild issues
- Files: Throughout entire `nes-*` module structure
- Cause: Tight coupling between modules; header dependencies not well isolated
- Improvement path: Reduce inter-module dependencies; use forward declarations; consider modularization refactor

**Debug Build Instrumentation:**
- Problem: When `NES_DEBUG_TUPLE_BUFFER_LEAKS` is enabled, additional overhead for buffer tracking
- Files: `CMakeLists.txt` (line 95-96) configures heavyweight instrumentation
- Cause: No performance metrics to determine impact or optimization points
- Improvement path: Add performance benchmarks showing overhead; implement selective instrumentation

**Plan Rendering with ASCII Conversion:**
- Problem: Rendering query plans involves replacing ASCII branches with Unicode, complex string operations
- Files: `nes-common/include/Util/PlanRenderer.hpp` (lines 122-124)
- Cause: Relies on even/odd line detection for branch replacement, fragile
- Improvement path: Implement proper tree rendering without post-processing conversion. Reference Issue #685.

---

## Fragile Areas

**Memory Casting and Type Conversions:**
- Files: 116+ occurrences of `cast`, `reinterpret_cast`, `static_cast`, `dynamic_cast` across codebase
- Why fragile: Type system violations during runtime conversions; refactoring class hierarchies breaks casts
- Safe modification: Audit all casts for safety; replace with safer alternatives (shared_ptr<T>, visitor pattern); add static assertions where possible
- Test coverage: Insufficient test coverage for cast operations; no negative tests for invalid casts

**Unsafe String Operations:**
- Files: `nes-physical-operators/src/Functions/ConcatPhysicalFunction.cpp`, `nes-nautilus/tests/UnitTests/VarValTest.cpp`, `nes-plugins/Sources/TCPSource/TCPSource.cpp`, and 5 more
- Why fragile: Use of `memcpy`, `strcpy`, `sprintf` without bounds checking in 8 files
- Safe modification: Replace with safe string operations; use std::string or fmt::format
- Test coverage: Missing fuzz tests for string input

**Query Engine State Transitions:**
- Files: `nes-query-engine/QueryEngine.cpp` (994 lines), `nes-query-engine/tests/QueryEngineTest.cpp` (1381 lines)
- Why fragile: Documented race conditions and deadlock avoidance code scattered throughout; lock release timing critical
- Safe modification: Require explicit lock guard analysis; consider state machine formalization
- Test coverage: Limited under high concurrency; tests skip under TSAN

**Circular Dependencies in Testing Infrastructure:**
- Files: `nes-executable/tests/TestUtils/TestTaskQueue.cpp`
- Why fragile: Test infrastructure has circular dependencies between components
- Safe modification: Refactor test infrastructure to break cycles; use dependency injection
- Test coverage: Circular dependency itself has no test

**Session/Context Management in Distributed Queries:**
- Files: `nes-distributed/` (multiple components), `nes-frontend/tests/DistributedPlanningTest.cpp` (662 lines)
- Why fragile: Distributed query sessions span multiple workers; session lifecycle is implicit
- Safe modification: Implement explicit session lifecycle management; add session validation
- Test coverage: Only 662 lines of distributed planning tests for entire subsystem

---

## Scaling Limits

**Connection Handler Accumulation in Network Service:**
- Current capacity: Unknown; connections never removed from active map
- Limit: After ~10,000 short-lived connections, observable memory/connection limit issues likely
- Scaling path: Implement connection cleanup with reference counting; add connection pool limits; monitor with metrics

**Buffer Management Under Concurrent Queries:**
- Current capacity: Memory allocator pools buffers but multiple concurrent queries can exhaust
- Limit: With large tuple buffers (>1MB) and many operators, buffer pool exhaustion triggers spills
- Scaling path: Implement adaptive buffer pool sizing; add buffer allocation metrics; consider off-heap storage

**CMake Build System Scalability:**
- Current capacity: 134 CMakeLists.txt files; 815 C++ source files
- Limit: Incremental builds noticeably slow (>30s for single file change in core modules)
- Scaling path: Reduce module coupling; implement proper header isolation; consider Bazel/Ninja improvements

**Distributed Topology Complexity:**
- Current capacity: System tested with small topologies (10s of workers)
- Limit: Catalyst/topology optimization algorithms likely O(n²) or worse in worker count
- Scaling path: Profile optimizer with large topologies; implement approximate solutions; add incremental planning

---

## Dependencies at Risk

**Legacy Optimizer Still In Use:**
- Risk: New query optimizer is incomplete; queries fall back to legacy optimizer
- Files: `nes-query-optimizer/src/LegacyOptimizer/` (entire legacy path)
- Impact: New optimizations/fixes don't apply to legacy queries; long-term maintenance burden
- Migration plan: Complete new optimizer feature parity; remove legacy optimizer entirely. Related to Issue #897.

**ANTLR Parser Maintainability:**
- Risk: Parser complexity (1017-line single file for SQL creation) makes changes error-prone
- Files: `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp`, `nes-sql-parser/src/StatementBinder.cpp` (574 lines)
- Impact: SQL parser is bottleneck for feature requests; bugs propagate through system
- Migration plan: Modularize parser; add property-based tests; consider alternative parsing approaches

**Heavyweight Test Data Files:**
- Risk: Systest data validation (824-line file) is tightly coupled to specific formats
- Files: `nes-systests/systest/src/SystestResultCheck.cpp` (824 lines), `nes-systests/systest/src/SystestParser.cpp` (693 lines)
- Impact: Adding new result formats or test types requires modifying core infrastructure
- Migration plan: Implement pluggable result validators; separate format handling from test logic

---

## Missing Critical Features

**Multiple Sinks Not Supported:**
- Problem: Parser explicitly documents that only single sink per query is supported
- Blocks: Cannot fan out query results to multiple destinations (e.g., database + file simultaneously)
- Files: `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp` (line 87)
- Workaround: Create multiple queries with same source data

**Multi-byte Tuple Delimiters:**
- Problem: Input formatters hard-limited to single-byte delimiters
- Blocks: Sources with complex delimiters (multi-character sequences, escaped chars) cannot be parsed
- Files: `nes-sources/src/SourceDescriptor.cpp` (line 52)
- Workaround: Pre-process data to use single-byte delimiters

**Optimizer Configuration in Topology Files:**
- Problem: Optimizer configs must be hardcoded at compile-time or CLI
- Blocks: Cannot specify per-node optimizer settings in distributed deployments
- Files: `nes-frontend/apps/cli/CLIStarter.cpp` (line 161)
- Workaround: Launch separate workers with different CLI flags

**Schema Inference Without Source Name Qualification:**
- Problem: Schema derivation requires matching source names in source/sink declarations
- Blocks: Renaming sources breaks queries; complex naming schemes required
- Files: `nes-sql-parser/tests/StatementBinderTest.cpp` (line 298)
- Workaround: Maintain strict source naming conventions

---

## Test Coverage Gaps

**Delimiter Edge Cases:**
- What's not tested: Multi-byte delimiters, escaped delimiters, delimiters with special chars
- Files: `nes-input-formatters/tests/UnitTests/` (delimiter tests)
- Risk: Source data with complex delimiters silently fails; data loss undetected
- Priority: **High** - Data integrity issue

**Network Channel Closure Under Partition:**
- What's not tested: Channel close behavior during network partitions, delayed Close ACKs
- Files: `nes-network/network/src/receiver/channel.rs`, `nes-network/network/src/sender/control.rs`
- Risk: Partial failures can leave dangling channels; reconnection semantics unclear
- Priority: **High** - Distributed correctness

**Concurrent Query Execution with Limited Buffers:**
- What's not tested: Multiple concurrent queries exhausting buffer pool
- Files: `nes-memory/` (buffer manager), `nes-query-engine/` (query execution)
- Risk: Deadlock or data loss under resource contention
- Priority: **High** - Production stability

**Distributed Optimizer Edge Cases:**
- What's not tested: Optimizer behavior with complex topologies, placement failures, cascading failures
- Files: `nes-frontend/tests/DistributedPlanningTest.cpp` (662 lines, limited coverage)
- Risk: Query plans may be suboptimal or invalid in adversarial deployments
- Priority: **Medium** - Performance and correctness

**Type System Coercion:**
- What's not tested: Implicit type conversions in joins, aggregations, casts
- Files: `nes-data-types/src/DataTypes/DataType.cpp` (joins return UNDEFINED type on mismatch)
- Risk: Silent type conversion failures; queries produce wrong results
- Priority: **Medium** - Correctness

**Stack Trace with TSAN:**
- What's not tested: Stack trace resolution under thread sanitizer (deliberately disabled)
- Files: `CMakeLists.txt` (lines 64-71) disables `NES_LOG_WITH_STACKTRACE` under TSAN
- Risk: Cannot diagnose threading issues that require stack traces
- Priority: **Low** - Debugging capability

---

*Concerns audit: 2026-03-14*
