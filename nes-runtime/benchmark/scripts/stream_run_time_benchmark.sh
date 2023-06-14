#!/bin/bash

id="TPCH_$(date +'%Y%m%d%H%M%S').csv"
string_to_append="ScaleFactor,Query,Compiler,CompileTime,ExecutionTime"
echo "$string_to_append" >>$id
compileIterations=1
executionWarmup=5
executionBenchmark=10

# Array of compiler arguments
queries=("NX1" "NX2" "YSB")
compilerArgs=("PipelineCompiler" "CPPPipelineCompiler" "BCInterpreter" "FlounderCompiler" "MIRCompiler")

# Iterate over each compiler argument in the array
for query in "${queries[@]}"; do
  for compilerArg in "${compilerArgs[@]}"; do
    command="../stream-benchmark query=$query identifier=$id  compiler=$compilerArg compileIterations=$compileIterations executionWarmup=$executionWarmup executionBenchmark=$executionBenchmark"
    echo $command
    eval $command
  done
done
python3 ./process_output.py $id
