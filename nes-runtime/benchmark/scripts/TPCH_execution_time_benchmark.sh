#!/bin/bash

id="TPCH_$(date +'%Y%m%d%H%M%S').csv"
scaleFactor=1
compileIterations=1
executionWarmup=10
executionBenchmark=10

# Array of compiler arguments
queries=("q1" "q3" "q6")
compilerArgs=("PipelineCompiler" "CPPPipelineCompiler" "BCInterpreter" "BabelfishPipelineCompiler", "FlounderCompiler")

# Iterate over each compiler argument in the array
for query in "${queries[@]}"; do
  for compilerArg in "${compilerArgs[@]}"; do
    command="../tpch-benchmark query=$query identifier=$id scaleFactor=$scaleFactor compiler=$compilerArg compileIterations=$compileIterations executionWarmup=$executionWarmup executionBenchmark=$executionBenchmark"
    echo $command
    eval $command
  done
done
