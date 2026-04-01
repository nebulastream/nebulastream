#!/bin/bash
# Run all three workloads (stateless, stateful, highfreq) with all hybrid strategies and batch sizes
cd /local-ssd/zeuchste/nesScheduler/nebulastream

SYSTEST="./cmake-build-release/nes-systests/systest/systest"
SYSTEST_DIR="nes-systests"
OUTCSV="/local-ssd/zeuchste/all_benchmarks.csv"

echo "threads,batch_size,scheduling,workload,time_seconds,bytes_per_second,tuples_per_second" > "$OUTCSV"

run_one() {
    local SCHED="$1" T="$2" BS="$3" WORKLOAD="$4" TESTFILE="$5"
    local WORK="/local-ssd/zeuchste/bench_${WORKLOAD}_${SCHED}_bs${BS}_t${T}"
    mkdir -p "$WORK"

    local CMD="$SYSTEST -b --show-query-performance -t $SYSTEST_DIR/$TESTFILE --workingDir $WORK"
    CMD+=" -- --worker.query_engine.number_of_worker_threads=$T"
    CMD+=" --worker.default_query_execution.execution_mode=COMPILER"
    CMD+=" --worker.query_engine.scheduling_strategy=$SCHED"
    CMD+=" --worker.query_engine.batch_pull_size=$BS"

    if [ "$T" -le 32 ]; then
        local NUMACTL=$(command -v numactl 2>/dev/null || echo /usr/bin/numactl)
        if [ -x "$NUMACTL" ]; then
            CMD="$NUMACTL --cpunodebind=0 --membind=0 $CMD"
        fi
    fi

    eval "$CMD" > /dev/null 2>&1 || true

    local RESULTS=$(find "$WORK" -name BenchmarkResults.json 2>/dev/null | head -1)
    if [ -n "$RESULTS" ]; then
        python3 -c "
import json, sys
for r in json.load(open(sys.argv[1])):
    print(f'$T,$BS,$SCHED,$WORKLOAD,{r[\"time\"]},{r[\"bytesPerSecond\"]},{r[\"tuplesPerSecond\"]}')
" "$RESULTS" >> "$OUTCSV"
        printf "%s %s BS=%s T=%s ... done\n" "$WORKLOAD" "$SCHED" "$BS" "$T"
    else
        printf "%s %s BS=%s T=%s ... FAIL\n" "$WORKLOAD" "$SCHED" "$BS" "$T"
    fi
    rm -rf "$WORK"
}

# Run batch size sweep for BATCH_PULL and HYBRID_QUEUE
for WORKLOAD_INFO in \
    "stateless:benchmark/YahooStreamingBenchmark_10k_huge_stateless.test" \
    "stateful:benchmark/YahooStreamingBenchmark_10k_huge.test" \
    "highfreq:benchmark/YahooStreamingBenchmark_10k_huge_highfreq.test"; do

    WORKLOAD="${WORKLOAD_INFO%%:*}"
    TESTFILE="${WORKLOAD_INFO##*:}"
    echo "=== $WORKLOAD ==="

    # GLOBAL_QUEUE (no batch size, just one run per thread count)
    for T in 1 2 4 8 16 32 64; do
        run_one GLOBAL_QUEUE "$T" 1 "$WORKLOAD" "$TESTFILE"
    done

    # BATCH_PULL and HYBRID_QUEUE with batch sizes
    for BS in 1 2 4 8 16 32 64; do
        for T in 1 2 4 8 16 32 64; do
            for SCHED in BATCH_PULL HYBRID_QUEUE; do
                run_one "$SCHED" "$T" "$BS" "$WORKLOAD" "$TESTFILE"
            done
        done
    done
done

echo "=== ALL DONE ==="
wc -l "$OUTCSV"
