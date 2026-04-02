#!/bin/bash
set -euo pipefail
cd /home/zeuchste-ldap/nesScheduler/nebulastream

echo "=== Step 1: Configure + Build ==="
./.nix/nix-cmake.sh -B cmake-build-release -DCMAKE_BUILD_TYPE=Release -G Ninja
./.nix/nix-cmake.sh --build cmake-build-release -j 64 --target systest

echo "=== Step 2: Regenerate data ==="
mkdir -p nes-systests/testdata/huge/ysb
if [ ! -f nes-systests/testdata/huge/ysb/ysb_10k_data_50000000.csv ]; then
    bash scripts/benchmarking/replicate_ysb_data.sh nes-systests/testdata/large/ysb/ysb_10k_data_479M.csv 10
    mv nes-systests/testdata/large/ysb/ysb_10k_data_50000000.csv nes-systests/testdata/huge/ysb/
fi

echo "=== Step 3: Run benchmark ==="
TESTFILE="nes-systests/benchmark/YahooStreamingBenchmark_10k_huge_stateless.test"
SYSTEST="./cmake-build-release/nes-systests/systest/systest"
OUTCSV="/tmp/hybrid_benchmark.csv"

echo "threads,scheduling,time_seconds,bytes_per_second,tuples_per_second" > "$OUTCSV"

for SCHED in GLOBAL_QUEUE BATCH_PULL HYBRID_QUEUE; do
  for T in 1 2 4 8 16 32 64; do
    WORK="/tmp/hb_${SCHED}_t${T}"
    mkdir -p "$WORK/tests"

    sed 's|TYPE File;|TYPE Benchmark SET('"'"'memory'"'"' AS `SOURCE`.MODE);|g' \
        "$TESTFILE" > "$WORK/tests/test.test"

    CMD="$SYSTEST -b --show-query-performance -t $WORK/tests/test.test --workingDir $WORK"
    CMD+=" -- --worker.query_engine.number_of_worker_threads=$T"
    CMD+=" --worker.default_query_execution.execution_mode=COMPILER"
    CMD+=" --worker.query_engine.work_dealing_strategy=$SCHED"
    CMD+=" --worker.query_engine.batch_pull_size=4"

    if [ "$T" -le 32 ] && command -v numactl &>/dev/null; then
      CMD="numactl --cpunodebind=0 --membind=0 $CMD"
    fi

    printf "%s T=%s ... " "$SCHED" "$T"
    eval "$CMD" > /dev/null 2>&1 || true

    RESULTS=$(find "$WORK" -name BenchmarkResults.json 2>/dev/null | head -1)
    if [ -n "$RESULTS" ]; then
      python3 -c "
import json, sys
for r in json.load(open(sys.argv[1])):
    print(f'$T,$SCHED,{r[\"time\"]},{r[\"bytesPerSecond\"]},{r[\"tuplesPerSecond\"]}')
" "$RESULTS" >> "$OUTCSV"
      echo "done"
    else
      echo "FAIL"
    fi
    rm -rf "$WORK"
  done
done

echo "=== Results ==="
cat "$OUTCSV"
echo "=== DONE ==="
