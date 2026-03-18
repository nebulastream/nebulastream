# Codebase Concerns

**Analysis Date:** 2026-03-13

## Architectural Issues

**Query Manager Monolithic Design (High Priority):**
- Issue: The Query Manager has evolved into a problematic monolithic hub that undermines system reliability and maintainability.
  - Pervasive tight coupling throughout codebase with components like QueryCompilation and ExecutableQueryPlan depending directly on internal Query Manager state
  - Components cannot be tested in isolation due to implementation details leaking across system boundaries
  - Inconsistent threading model: query registration is asynchronous while termination blocks the requesting thread (behavior undocumented)
  - Complex web of reconfiguration messages, reference counting mechanisms, and bloated 7-state machine with opaque transition logic
  - Only understandable through detailed source code analysis, making onboarding difficult
- Files: `nes-query-engine/QueryEngine.cpp` (985 lines), `nes-query-engine/RunningQueryPlan.cpp`
- Impact: Impairs development velocity, increases regression risk, blocks new feature implementation
- Fix approach: Refactor per design doc `docs/design/20240927_QueryManager.md` - separate concerns, expose minimal interface, move to async-only operations, implement comprehensive tests

**Query Lifecycle Timing Bug:**
- Issue: Tasks do not keep pipelines alive during shutdown, allowing PendingPipelineStop tasks to overtake processing tasks for previously received data
- Files: `nes-query-engine/QueryEngine.cpp`, `nes-query-engine/RunningQueryPlan.cpp`
- Impact: Data loss or incorrect query termination behavior
- Fix approach: Use `PendingPipelineStopTask` with extra reference counter for each `RunningQueryPlanNode` (referenced in QueryManager design doc, section on reference counting)

**Logger Shutdown Bug:**
- Issue: Calling `spdlog::shutdown()` causes system tests to fail in `std::__hash_table::__deallocate_node()`
- Files: `nes-common/src/Util/Logger/NesLogger.cpp` (line 198)
- Impact: System shutdown may hang or crash in test scenarios
- Fix approach: Investigate root cause in spdlog integration or std::hash_table deallocation order (TODO #348)

## Technical Debt

**SQL Parser Schema Qualification Issues:**
- Issue: Map-of-maps structure used for config options instead of proper identifier lists
- Files: `nes-sql-parser/src/CommonParserFunctions.cpp` (line 65)
- Impact: Parser fragility, difficult to maintain schema qualification logic
- Fix approach: Implement proper identifier list structures (TODO #764 - long standing)

**Column Qualification Hack:**
- Issue: Column names in schema declarations qualified as workaround for per-operator-lexical-scopes
- Files: `nes-sql-parser/src/CommonParserFunctions.cpp` (line 255)
- Impact: Fragile workaround indicates incomplete schema handling design
- Fix approach: Refactor to eliminate need for qualification hack (TODO #764)

**Identifier System Design Debt:**
- Issue: Identifiers.hpp marked for refactor - current design suggests issues with identifier handling
- Files: `nes-common/include/Identifiers/Identifiers.hpp` (line 24)
- Impact: Ongoing identifier handling bugs and inconsistencies
- Fix approach: Conduct full refactor of identifier system (TODO #601)

**PlanRenderer Complexity:**
- Issue: Plan renderer has incomplete implementation for DAG simplification and layer balancing
- Files: `nes-common/include/Util/PlanRenderer.hpp` (631 lines, multiple TODOs)
- Impact: Query plan visualization may have crossing branches or incorrect layout
- Fix approach: Complete TODO #685 items for DAG edge crossing reduction and node repositioning

**Multiple Sink Support Missing:**
- Issue: System only supports single sinks; multiple sink queries unsupported
- Files: `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp` (line 87)
- Impact: Limits query expressiveness, blocks queries with multiple output destinations
- Fix approach: Add full multi-sink support (TODO #421)

**Comparison Binding Shortcut:**
- Issue: Comparison binding uses non-standard approach instead of normal binding
- Files: `nes-sql-parser/src/StatementBinder.cpp` (line 81)
- Impact: May miss edge cases in comparison operators during query binding
- Fix approach: Replace with normal comparison binding (TODO #897, related to #764)

**Schema Qualification Logic Centralization:**
- Issue: Qualified field logic scattered instead of centralized
- Files: `nes-data-types/include/DataTypes/Schema.hpp` (line 53)
- Impact: Maintenance burden, inconsistent schema qualification behavior
- Fix approach: Move all qualified field logic to central place (TODO #764)

**Source Sink Binding Refactor:**
- Issue: Sink binding logic in legacy query optimizer, needs migration to new query binder
- Files: `nes-query-optimizer/src/LegacyOptimizer/SinkBindingRule.cpp` (line 34)
- Impact: Tight coupling between optimizer and binding logic, blocks query compilation refactoring
- Fix approach: Move sink binding to new query binder (TODO #897)

**Tuple Buffer Management (XXX - Unsafe noexcept):**
- Issue: Cannot make `release` noexcept without ensuring MemorySegment is noexcept destructible
- Files: `nes-memory/TupleBufferImpl.cpp` (line 72-75)
- Impact: Error handling in buffer recycling not fully exception-safe
- Fix approach: Redesign noexcept guarantee or convert error handling to assertions

## Known Limitations

**Tuple Delimiter Size Limitation:**
- Issue: Sources do not support delimiters larger than one byte
- Files: `nes-sources/src/SourceDescriptor.cpp` (line 51), `nes-input-formatters/tests/UnitTests/SpecificSequenceTest.cpp` (line 102)
- Impact: Limited input format support for streaming data
- Fix approach: Extend parser to support multi-byte delimiters (TODO #651)

**Small Files Input Limitation:**
- Issue: Input formatter sometimes requires an extra buffer for small files (reason unknown)
- Files: `nes-input-formatters/tests/UnitTests/SmallFilesTest.cpp` (line 214)
- Impact: Unpredictable behavior with small file inputs, inefficient buffering (TODO #774)
- Fix approach: Investigate root cause and fix buffer allocation logic

**Single Node Worker Design:**
- Issue: QueryLog listener marked for removal as part of single node worker redesign
- Files: `nes-runtime/src/Listeners/QueryLog.cpp` (line 52), `nes-runtime/include/Listeners/QueryLog.hpp` (line 71)
- Impact: Legacy logging code may be removed in future versions (TODO #34)
- Fix approach: Migrate off deprecated QueryLog before single node worker redesign completes

**PipelineExecutionContext Coupling:**
- Issue: OperatorHandler needs to be removed from PipelineExecutionContext
- Files: `nes-executable/include/PipelineExecutionContext.hpp` (line 60)
- Impact: Tight coupling between execution context and specific operator types
- Fix approach: Decouple handler from context (TODO #30)

**C++20 Concepts Not Used:**
- Issue: Current operator/function inheritance could be replaced with C++20 Concepts but not yet adopted
- Files: `nes-physical-operators/include/PhysicalOperator.hpp` (line 51)
- Impact: Type constraints less precise, harder to understand valid operator implementations
- Fix approach: Investigate and adopt C++20 Concepts (TODO #875)

## Testing Gaps

**Query Manager Unit Tests:**
- What's not tested: Core Query Manager state transitions, edge cases in concurrent query operations
- Files: `nes-query-engine/QueryEngine.cpp` (no dedicated unit test file for core engine logic)
- Risk: Query Manager bugs only caught by integration/system tests; regression risk high
- Priority: High - design doc (20240927_QueryManager.md) explicitly cites lack of unit tests as major debt

**Legacy Test Workaround:**
- Issue: Test workaround exists for schema inference - test should be removed when proper schema inference implemented
- Files: `nes-sql-parser/tests/StatementBinderTest.cpp` (line 296)
- Impact: Test carries forward limitation that source names must match sink names in queries
- Priority: Medium - remove when schema inference completed (TODO #764)

## Dependency and Build System Issues

**Dependency Management Complexity:**
- Issue: Dependencies managed via four different channels: vendored libraries, FetchContent, VCPKG, and external repository
- Files: Documented in `docs/design/20240710_dependency-management.md`
- Problems:
  - Vendored libraries (magic_enum, yaml, backwards) cause licensing and version tracking issues
  - FetchContent builds dependencies with strict NES compiler warnings, causing third-party build failures
  - External repository approach (for LLVM/clang) has not been updated for 2.5+ years
  - VCPKG dependency conflicts not easily managed across different build configurations
- Impact: Long build times, difficult onboarding, no binary caching for fresh builds
- Fix approach: Shift to exclusive VCPKG-based dependency management with manifest mode (per design doc)

**Sanitizer Configuration Limitation:**
- Issue: VCPKG currently requires setting toolchain before CMake project initialization
- Files: Documented in `docs/design/20240710_dependency-management.md`
- Impact: Sanitizer builds (AddressSanitizer, ThreadSanitizer, MemorySanitizer) require separate build configurations
- Fix approach: Use VCPKG overlay triplets to specify different compiler flags per sanitizer type

**Binary Caching Missing:**
- Issue: Fresh builds require all VCPKG dependencies to be built locally for each toolchain
- Files: Related to build system organization
- Impact: Significant time investment on first build for new developers
- Fix approach: Implement binary cache (via VCPKG-cache-http) or distribute pre-built docker image with cached dependencies

## Concurrency and Memory Safety

**Weak Reference Lifecycle Tracking:**
- Issue: System uses 15+ `if (auto strong = weak_ptr.lock())` patterns indicating potential dangling reference issues
- Files: Throughout `nes-query-engine/`, especially `QueryEngine.cpp`
- Risk: Race conditions if weak_ptr target destroyed between lock checks
- Mitigation: Current implementation appears to handle this via callbacks, but needs comprehensive review

**Reference Count Edge Cases:**
- Issue: Complex reference counting in TupleBuffer and QueryPlan nodes
- Files: `nes-memory/TupleBufferImpl.cpp`, `nes-memory/TupleBufferImpl.hpp` (line 73-76)
- Risk: Reference count underflow/overflow in edge cases or concurrent scenarios
- Mitigation: INVARIANT assertions check for invalid counters at cleanup time (lines 78)

**spdlog Async Logger State (Potential Race):**
- Issue: Async logger periodic flusher uses `std::make_unique` and manual reset
- Files: `nes-common/src/Util/Logger/NesLogger.cpp` (line 142, 173-174)
- Risk: Race condition if logger shutdown called while async flush in progress
- Mitigation: Uses atomic bool for shutdown guard (line 171)

## Performance Considerations

**Large Test Files:**
- Largest source file: `nes-query-engine/tests/QueryEngineTest.cpp` (1,381 lines)
- Largest production file: `nes-query-engine/QueryEngine.cpp` (985 lines)
- Impact: These files are harder to maintain, understand, and test
- Recommendation: Consider splitting into multiple focused test suites and modules

**TupleBuffer Reference Counting Overhead:**
- Issue: Every TupleBuffer copy/move involves atomic operations on reference counter
- Files: `nes-memory/TupleBuffer.cpp` (line 53), `nes-memory/TupleBufferImpl.hpp`
- Impact: High-frequency buffers (millions per second) may have atomic contention
- Status: Acceptable trade-off for memory safety; mentioned in code comments

## Scaling Limits

**Query Engine Thread Model:**
- Issue: Query Manager design doc states "will not support multiple Queues or pinning of Queries to subset of worker-threads"
- Files: `docs/design/20240927_QueryManager.md` (Non-Goals section)
- Current capacity: Single queue for all queries across all worker threads
- Limit: May hit contention on single admission queue at high query volumes
- Future scaling: Requires architectural redesign if multiple independent query queues needed

## Critical Code Path Risks

**Pipeline Stop Race (Known Issue):**
- Problem: Original system where PendingPipelineStop could overtake data processing tasks
- Files: `nes-query-engine/QueryEngine.cpp`, `nes-query-engine/RunningQueryPlan.cpp`
- Current mitigation: Extra reference counter for RunningQueryPlanNode per design doc
- Status: Partially addressed but needs full integration testing

**Exception Safety in Query Failure:**
- Issue: Query failure callbacks may be invoked during stack unwinding
- Files: `nes-query-engine/QueryEngine.cpp` (lines 73-83, `injectQueryFailureUnsafe` function)
- Impact: Complex error handling flow during query termination
- Status: Currently uses weak_ptr version (`injectQueryFailure`) to reduce lifetime issues

---

*Concerns audit: 2026-03-13*
