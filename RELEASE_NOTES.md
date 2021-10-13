# NebulaStream 0.1.0 (In-Progress) Release Notes

## Introduction:
## Breaking Changes:
## Components:
### - Stream Processing
### - Complex Event Processing
### - Rest-API
### - Optimizer
### - Runtime
### - Query Compiler
1. Add configuration flags for query compiler [#2194](https://github.com/nebulastream/nebulastream/issues/2194)
    - `--queryCompilerCompilationStrategy` Selects the optimization level of a query.
    - `--queryCompilerPipeliningStrategy` Selects the pipelining strategy, i.e. operator  fusion or operator at a time.
    - `--queryCompilerOutputBufferOptimizationLevel` Selects the buffer optimization level
### - Fault Tolerance
### - Monitoring
### - Build Management
1. Add Folly as default dependency [#2194](https://github.com/nebulastream/nebulastream/issues/2194)
### - UDF Support
### - Network Stack