#!/bin/bash

id="TPCH_$(date +'%Y%m%d%H%M%S').csv"
scaleFactor=0.01
compileIterations=10
executionWarmup=0
executionBenchmark=0

# Array of compiler arguments
queries=("q1" "q3" "q6")
compilerArgs=("PipelineCompiler" "CPPPipelineCompiler")

# Iterate over each compiler argument in the array
for query in "${queries[@]}"; do
  for compilerArg in "${compilerArgs[@]}"; do
    command="../tpch-benchmark query=$query identifier=$id scaleFactor=$scaleFactor compiler=$compilerArg compileIterations=$compileIterations executionWarmup=$executionWarmup executionBenchmark=$executionBenchmark"
    echo $command
    eval $command
  done
done
