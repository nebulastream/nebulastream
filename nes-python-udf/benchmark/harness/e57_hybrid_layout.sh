#!/bin/bash
# E57: hybrid layout of the UDF process batches (fields that no UDF reads or writes in a row region) against Arrow only.
# Worker side (T w2 p16, inc): no UDF, pipeline break only (--no-calls) and UDF process, each in both layouts.
# Python side (T w12 p16, lcg, 21 unused fields): both layouts, for the load of the UDF source thread.
# Boost off, huge pages for the pool, 1 file source.
cd /data-ssd/lukas/claude-worktrees/nebulastream-a028c2b88b466dcf/python-udfs-perf-eval-c8fd23/nes-python-udf/benchmark
common=(--experiment E57 --input 480m --memory 8589934592 --worker-argument=--worker.python_udf.process_nursery=1M --environment NES_UDF_SHARED_PROCESSES=1 --environment UDF_BENCH_POOL_NODE=0)
for n in 0 9 21; do
  echo "load $(cut -d' ' -f1 /proc/loadavg) unused $n"
  python3 harness/run_experiment.py "${common[@]}" --layouts "T w2 p16" --udfs native_inc --unused $n --note "no UDF, $n unused fields" 2>&1 | grep "rc "
  for h in "" --hybrid; do
    python3 harness/run_experiment.py "${common[@]}" --layouts "T w2 p16" --udfs inc --unused $n --no-calls $h --note "pipeline break only ${h:-arrow}, $n unused fields" 2>&1 | grep "rc "
    python3 harness/run_experiment.py "${common[@]}" --layouts "T w2 p16" --udfs inc --unused $n $h --note "UDF process ${h:-arrow}, $n unused fields" 2>&1 | grep "rc "
  done
done
for h in "" --hybrid; do
  echo "load $(cut -d' ' -f1 /proc/loadavg) python side ${h:-arrow}"
  python3 harness/run_experiment.py "${common[@]}" --layouts "T w12 p16" --udfs lcg --unused 21 $h --note "python side ${h:-arrow}, 21 unused fields" 2>&1 | grep "rc "
done
