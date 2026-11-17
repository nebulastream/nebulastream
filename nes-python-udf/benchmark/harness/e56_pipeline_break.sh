#!/bin/bash
# E56: the pipeline break around an OUT_OF_PROCESS UDF without the process (--no-calls: emit to Arrow | scan from Arrow), against the
# query without a UDF (native_inc) and with the UDF process (inc). Worker side, T w2 p16, boost off, huge pages for the pool, 1 file source.
cd /data-ssd/lukas/claude-worktrees/nebulastream-a028c2b88b466dcf/python-udfs-perf-eval-c8fd23/nes-python-udf/benchmark
common=(--experiment E56 --input 480m --memory 8589934592 --layouts "T w2 p16" --worker-argument=--worker.python_udf.process_nursery=1M --environment NES_UDF_SHARED_PROCESSES=1 --environment UDF_BENCH_POOL_NODE=0)
for n in 0 9 21; do
  echo "load $(cut -d' ' -f1 /proc/loadavg) unused $n"
  python3 harness/run_experiment.py "${common[@]}" --udfs native_inc --unused $n --note "no UDF, $n unused fields" 2>&1 | grep "rc "
  python3 harness/run_experiment.py "${common[@]}" --udfs inc --unused $n --no-calls --note "pipeline break only, $n unused fields in the batch" 2>&1 | grep "rc "
  python3 harness/run_experiment.py "${common[@]}" --udfs inc --unused $n --note "UDF process, $n unused fields in the buffer of the UDF" 2>&1 | grep "rc "
done
