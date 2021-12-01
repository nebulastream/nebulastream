# NebulaStream 0.1.0 (In-Progress) Release Notes

## Introduction:
## Breaking Changes:
## Components:
### - Stream Processing
1. Make NES numa-aware #2135
2. Introduce Benchmark source #2252
3. Introduce basic set of stateful and stateless benchmarks #1900 #1828 
### - Complex Event Processing
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
### - UDF Support
### - Network Stack
