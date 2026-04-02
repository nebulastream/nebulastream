#!/bin/bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

OUTCSV="${1:-/local-ssd/zeuchste/hybrid_batch_sweep.csv}"
TESTFILE_STATELESS="benchmark/YahooStreamingBenchmark_10k_huge_stateless.test"
TESTFILE_STATEFUL="benchmark/YahooStreamingBenchmark_10k_huge.test"

echo "threads,batch_size,scheduling,workload,time_seconds,bytes_per_second,tuples_per_second" > "$OUTCSV"

for WORKLOAD in stateless stateful; do
  if [ "$WORKLOAD" = "stateless" ]; then
    TESTFILE="$TESTFILE_STATELESS"
  else
    TESTFILE="$TESTFILE_STATEFUL"
  fi

  for BS in 1 2 4 8 16 32 64; do
    for T in 1 2 4 8 16 32 64; do
      for SCHED in BATCH_PULL HYBRID_QUEUE; do
        WORK="/local-ssd/zeuchste/bsweep_${WORKLOAD}_${SCHED}_bs${BS}_t${T}"
        rm -rf "$WORK"

        bash "$SCRIPT_DIR/run_benchmark_source_modes.sh" \
            --build-dir cmake-build-release \
            --test-file "$TESTFILE" \
            --iterations 1 \
            --modes memory \
            --work-dealing "$SCHED" \
            --work-stealing false \
            --producer-local false \
            --batch-pull-size "$BS" \
            --worker-threads "$T" \
            --output "$WORK/results.csv" 2>/dev/null || true

        if [ -f "$WORK/results.csv" ] && [ "$(wc -l < "$WORK/results.csv")" -gt 1 ]; then
          tail -n +2 "$WORK/results.csv" | while IFS=, read -r threads sched ws pl mode iter query time bps tps; do
            echo "$T,$BS,$SCHED,$WORKLOAD,$time,$bps,$tps" >> "$OUTCSV"
          done
          printf "%s %s BS=%s T=%s ... done\n" "$WORKLOAD" "$SCHED" "$BS" "$T"
        else
          printf "%s %s BS=%s T=%s ... FAIL\n" "$WORKLOAD" "$SCHED" "$BS" "$T"
        fi
        rm -rf "$WORK"
      done
    done
  done
done

echo "=== DONE ==="
echo "Results: $OUTCSV"
