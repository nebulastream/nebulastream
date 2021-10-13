# NebulaStream 0.1.0 (In-Progress) Release Notes

## Introduction:
## Breaking Changes:
## Components:
### - Benchmarking
1. Introduce Benchmark source #2252
2. Introduce basic set of stateful and stateless benchmarks #1900 #1828 
### - Stream Processing
1. Add event dispatching support at operator level [#2129](https://github.com/nebulastream/nebulastream/issues/2129)
2. Make NES numa-aware #2135
### - Complex Event Processing
Add a mapping for the three binary Simple Event Algebra Operators AND [1815], SEQ [1817] and OR [1816] to the available Analytical Stream Processing Operators
   - AND to Cartesian Product (akka joinWith + map() for keys currently) 
   - SEQ to Cartesian Product (akka joinWith + map() for keys currently) + filter() on timestamp for order of events 
   - OR to Union (+map() function for source information, to note that the map() is only there for convenience but might be helpful for more complex patterns)  
### - Rest-API
Query Submission
   - Enable query submission using protobuf message [#2325](https://github.com/nebulastream/nebulastream/pull/2325)
### - Optimizer
Extended support for Multi-Query Optimization.
   - Added support for identifying partial sharing using different strategies [2136](https://github.com/nebulastream/nebulastream/issues/2136)
   - Added a hybrid approach for merging queries [2183](https://github.com/nebulastream/nebulastream/issues/2183) 

Operator Placement
   - Integrate ILP-based Operator Placement [#2149](https://github.com/nebulastream/nebulastream/pull/2325)
### - Runtime
1. Add new `DynamicTupleBuffer` abstraction to operate on tuple buffers, without knowledge of the underling memory layout.
2. Add experimental support for columnar layouts [2081](https://github.com/nebulastream/nebulastream/tree/2081-queryoptimizer-phase-choose-mem-layout).
   Introduce `--memoryLayoutPolicy` in the coordinator configuration.
   - `FORCE_ROW_LAYOUT` forces a row memory layout for all operators of an query.
   - `FORCE_COLUMN_LAYOUT` forces a columnar memory layout for all operators of an query.
3. Add support to PAPI Profiler [#2310](https://github.com/nebulastream/nebulastream/issues/2310)
4. Fix Numa allocation and CPU mapping in containers [#2271](https://github.com/nebulastream/nebulastream/issues/2271)
### - Query Compiler
1. Add configuration flags for query compiler [#2194](https://github.com/nebulastream/nebulastream/issues/2194)
    - `--queryCompilerCompilationStrategy` Selects the optimization level of a query.
    - `--queryCompilerPipeliningStrategy` Selects the pipelining strategy, i.e. operator  fusion or operator at a time.
    - `--queryCompilerOutputBufferOptimizationLevel` Selects the buffer optimization level
### - Fault Tolerance
1. Add a node storage unit, which temporary stores all tuple buffers that “flow” through it. [2203](https://github.com/nebulastream/nebulastream/issues/2203)
2. Add a lineage data structure that keep track of a mapping between old and new tuple buffer ids. [2264](https://github.com/nebulastream/nebulastream/issues/2264)
3. Add fault tolerance and lineage flags to the query context, by default these flags are disabled. [2298](https://github.com/nebulastream/nebulastream/issues/2298)
### - Monitoring
### - Build Management
1. Add Folly as default dependency [#2194](https://github.com/nebulastream/nebulastream/issues/2194)
2. Add CMAKE flag to build dependencies locally [#2313](https://github.com/nebulastream/nebulastream/issues/2313)
   -DNES_BUILD_DEPENDENCIES_LOCAL=1
3. [#2115](https://github.com/nebulastream/nebulastream/issues/2115) Build NebulaStream on ARM Macs
### - UDF Support
1. [#2079](https://github.com/nebulastream/nebulastream/issues/2079) [#2080](https://github.com/nebulastream/nebulastream/issues/2080) Public API to register Java UDFs in the NES coordinator
2. [#117](https://github.com/nebulastream/nebulastream-java-client/issues/117) Register Java UDFs from the Java client
### - Network Stack
1. Add event dispatching support at network level [#2129](https://github.com/nebulastream/nebulastream/issues/2129)