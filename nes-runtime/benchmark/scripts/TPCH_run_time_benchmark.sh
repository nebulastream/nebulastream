#!/bin/bash

id="TPCH_$(date +'%Y%m%d%H%M%S').csv"
string_to_append="ScaleFactor,Query,Compiler,CompileTime,ExecutionTime"
echo "$string_to_append" >>$id
scaleFactors=("0.001" "0.01" "0.1" "1")
compileIterations=1
executionWarmup=0
executionBenchmark=1

# Array of compiler arguments
queries=("q1" "q3" "q6")
compilerArgs=("PipelineCompiler" "CPPPipelineCompiler" "BCInterpreter" "BabelfishPipelineCompiler" "PipelineInterpreter")

# Iterate over each compiler argument in the array
for scaleFactor in "${scaleFactors[@]}"; do
  for query in "${queries[@]}"; do
    for compilerArg in "${compilerArgs[@]}"; do
      command="../tpch-benchmark query=$query identifier=$id scaleFactor=$scaleFactor compiler=$compilerArg compileIterations=$compileIterations executionWarmup=$executionWarmup executionBenchmark=$executionBenchmark"
      echo $command
      eval $command
    done
  done
done
python3 ./process_output.py $id