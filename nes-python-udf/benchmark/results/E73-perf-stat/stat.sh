#!/bin/bash
# stat.sh LABEL: perf stat of the filterstopwords Numba x10 query, whole process, appended to $S/stat.txt
cd /data-ssd/lukas/claude-worktrees/nebulastream-a028c2b88b466dcf/python-udfs-perf-eval-c8fd23
S=/tmp/claude-2545/-data-ssd-lukas-claude-worktrees-nebulastream-a028c2b88b466dcf-python-udfs-perf-eval-c8fd23/af5b8720-7207-407f-82df-37469afa0a76/scratchpad
export NES_NUMBA_PYTHON=$S/numba/venv/bin/python
T=$(python3 -c "import sys; sys.path.insert(0,'nes-python-udf/benchmark/harness'); import udf_perf_profile as p; print(p.single_query_test('filterstopwords','numba'))")
rm -rf $S/stat-work
perf stat -x, -o $S/stat-$1.csv -e task-clock,cycles,instructions,L1-dcache-load-misses,cache-misses,dTLB-load-misses,page-faults -- \
  cmake-build-relwithdebinfo-libstdcxx-none/nes-systests/systest/systest -t $T --data /dev/shm/streamudfbench-data --workingDir $S/stat-work \
  --query-timeout 1200 -c nes-python-udf/benchmark/harness/single-node.yaml -- --worker.query_engine.number_of_worker_threads=1 >/dev/null 2>&1
echo "rc $?"; rm -f $T
