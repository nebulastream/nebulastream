#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

TESTFILE="${1:-benchmark/YahooStreamingBenchmark_10k_huge_stateless.test}"
OUTCSV="/tmp/batch_sweep.csv"
SYSTEST_BIN="./cmake-build-release/nes-systests/systest/systest"

echo "threads,batch_size,time_seconds,bytes_per_second,tuples_per_second" > "$OUTCSV"

for BS in 1 2 4 8 16 32; do
  for T in 1 2 4 8 16 32 64; do
    WORK="/tmp/bs${BS}_t${T}"
    mkdir -p "$WORK"

    # Use the benchmark script's approach: call run_benchmark_source_modes.sh
    # which handles test file generation correctly
    bash "$SCRIPT_DIR/run_benchmark_source_modes.sh" \
        --build-dir cmake-build-release \
        --test-file "$TESTFILE" \
        --iterations 1 \
        --modes memory \
        --scheduling BATCH_PULL \
        --work-stealing false \
        --producer-local false \
        --batch-pull-size "$BS" \
        --worker-threads "$T" \
        --output "$WORK/results.csv" 2>/dev/null

    # Extract data (skip header)
    if [ -f "$WORK/results.csv" ] && [ "$(wc -l < "$WORK/results.csv")" -gt 1 ]; then
      tail -n +2 "$WORK/results.csv" | while IFS=, read -r threads sched ws pl mode iter query time bps tps; do
        echo "$T,$BS,$time,$bps,$tps" >> "$OUTCSV"
      done
      printf "BS=%s T=%s ... done\n" "$BS" "$T"
    else
      printf "BS=%s T=%s ... FAIL\n" "$BS" "$T"
    fi
    rm -rf "$WORK"
  done
done

echo "=== Results ==="
cat "$OUTCSV"
