#!/bin/bash
# Run mixed workload benchmark: concurrent queries with different complexities
# Uses -n 2 (two concurrent queries) instead of -b (benchmark mode)
# Measures total wall-clock time as the performance metric
set -uo pipefail

cd "$(dirname "$0")/../.."

SYSTEST="./cmake-build-release/nes-systests/systest/systest"
TESTFILE="${1:-nes-systests/benchmark/YahooStreamingBenchmark_10k_huge_mixed.test}"
OUTCSV="${2:-/local-ssd/zeuchste/mixed_results.csv}"

echo "threads,scheduling,wall_seconds" > "$OUTCSV"

for T in 1 2 4 8 16 32 64; do
  for ENTRY in "GLOBAL_QUEUE:false" "PER_THREAD_ROUND_ROBIN:false" "PER_THREAD_SMALLEST_QUEUE:false" "PER_THREAD_CHOOSE_TWO:false" "PER_THREAD_ADAPTIVE:false" "PER_THREAD_ROUND_ROBIN:true"; do
    SCHED=${ENTRY%%:*}
    PL=${ENTRY##*:}
    if [ "$PL" = "true" ]; then LABEL="PRODUCER_LOCAL"; else LABEL="$SCHED"; fi

    WORK="/local-ssd/zeuchste/mixed_${LABEL}_t${T}"
    mkdir -p "$WORK"

    CMD="$SYSTEST -n 2 --show-query-performance -t $TESTFILE --workingDir $WORK"
    CMD+=" -- --worker.query_engine.number_of_worker_threads=$T"
    CMD+=" --worker.default_query_execution.execution_mode=COMPILER"
    CMD+=" --worker.query_engine.work_dealing_strategy=$SCHED"
    CMD+=" --worker.query_engine.producer_local=$PL"

    if [ "$T" -le 32 ] && [ -x /usr/bin/numactl ]; then
        CMD="/usr/bin/numactl --cpunodebind=0 --membind=0 $CMD"
    fi

    printf "%s T=%s ... " "$LABEL" "$T"

    # Measure wall-clock time
    START_TIME=$(date +%s.%N)
    eval "$CMD" > /dev/null 2>&1 || true
    END_TIME=$(date +%s.%N)
    WALL_SECONDS=$(echo "$END_TIME - $START_TIME" | bc)

    echo "$T,$LABEL,$WALL_SECONDS" >> "$OUTCSV"
    printf "done (%.1fs)\n" "$WALL_SECONDS"

    rm -rf "$WORK"
  done
done

echo "=== DONE ==="
cat "$OUTCSV"
