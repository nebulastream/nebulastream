#!/bin/bash
# E55: unused fields in the buffer of the UDF against fields created after the UDF (stand-in for sending only the relevant columns).
# Boost off, huge pages for the pool, 1 file source. $1 = python (lcg, T w12 p16) or worker (inc and native_inc, T w2 p16).
cd /data-ssd/lukas/claude-worktrees/nebulastream-a028c2b88b466dcf/python-udfs-perf-eval-c8fd23/nes-python-udf/benchmark
common=(--experiment E55 --input 480m --memory 8589934592 --worker-argument=--worker.python_udf.process_nursery=1M --environment NES_UDF_SHARED_PROCESSES=1 --environment UDF_BENCH_POOL_NODE=0)
if [ "$1" = python ]; then L="T w12 p16"; udfs="lcg"; else L="T w2 p16"; udfs="inc native_inc"; fi
for u in $udfs; do for n in 0 9 21; do
  echo "load $(cut -d' ' -f1 /proc/loadavg) $u unused $n in buffer"
  python3 harness/run_experiment.py "${common[@]}" --layouts "$L" --udfs $u --unused $n --note "$1 side, $n unused fields in the buffer of the UDF" 2>&1 | grep "rc "
  if [ $n != 0 ] && [ $u != native_inc ]; then
    echo "load $(cut -d' ' -f1 /proc/loadavg) $u unused $n after"
    python3 harness/run_experiment.py "${common[@]}" --layouts "$L" --udfs $u --unused $n --unused-after --note "$1 side, $n unused fields created after the UDF" 2>&1 | grep "rc "
  fi
done; done
