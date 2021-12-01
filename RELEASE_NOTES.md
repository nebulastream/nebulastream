# NebulaStream 0.1.0 (In-Progress) Release Notes

## Introduction:
## Breaking Changes:
## Components:
### - Benchmarking
1. Introduce Benchmark source #2252
2. Introduce basic set of stateful and stateless benchmarks #1900 #1828 
### - Stream Processing
1. Make NES numa-aware #2135
### - Complex Event Processing
Add a mapping for the three binary Simple Event Algebra Operators AND [1815], SEQ [1817] and OR [1816] to the available Analytical Stream Processing Operators
   - AND to Cartesian Product (akka joinWith + map() for keys currently) 
   - SEQ to Cartesian Product (akka joinWith + map() for keys currently) + filter() on timestamp for order of events 
   - OR to Union (+map() function for source information, to note that the map() is only there for convenience but might be helpful for more complex patterns)  
### - Rest-API
### - Optimizer
### - Runtime
1. Add new `DynamicTupleBuffer` abstraction to operate on tuple buffers, without knowledge of the underling memory layout.
2. Add experimental support for columnar layouts [2081](https://github.com/nebulastream/nebulastream/tree/2081-queryoptimizer-phase-choose-mem-layout).
   Introduce `--memoryLayoutPolicy` in the coordinator configuration.
   - `FORCE_ROW_LAYOUT` forces a row memory layout for all operators of an query.
   - `FORCE_COLUMN_LAYOUT` forces a columnar memory layout for all operators of an query.
### - Query Compiler
1. Add configuration flags for query compiler [#2194](https://github.com/nebulastream/nebulastream/issues/2194)
    - `--queryCompilerCompilationStrategy` Selects the optimization level of a query.
    - `--queryCompilerPipeliningStrategy` Selects the pipelining strategy, i.e. operator  fusion or operator at a time.
    - `--queryCompilerOutputBufferOptimizationLevel` Selects the buffer optimization level
### - Fault Tolerance
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
