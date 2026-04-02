#!/bin/bash
# Run benchmarks locally with work analytics enabled
# Compares: GLOBAL_QUEUE (baseline) vs PRODUCER_LOCAL (best dealing) vs PRODUCER_LOCAL+CHOOSE_TWO stealing
set -uo pipefail
cd "$(dirname "$0")/../.."

SYSTEST="docker run --rm --workdir /src -v $(pwd):/src nebulastream/nes-development:local ./cmake-build-docker/nes-systests/systest/systest"
OUTDIR="benchmark_analytics"
mkdir -p "$OUTDIR"

# Use small dataset for local testing
TESTFILES=(
    "stateless:nes-systests/benchmark_small/YahooStreamingBenchmark.test"
)

# Check if large dataset exists
if [ -f "nes-systests/testdata/large/ysb/ysb_10k_data_479M.csv" ]; then
    TESTFILES+=(
        "stateful:nes-systests/benchmark/YahooStreamingBenchmark_10k.test"
    )
fi

echo "threads,config,workload,time,bps,tps" > "$OUTDIR/results.csv"

for WORKLOAD_INFO in "${TESTFILES[@]}"; do
    WORKLOAD="${WORKLOAD_INFO%%:*}"
    TESTFILE="${WORKLOAD_INFO##*:}"
    echo "=== $WORKLOAD ==="

    for T in 1 2 4 8; do
        for CFG in \
            "BASELINE:GLOBAL_QUEUE:NONE:false" \
            "DEALING:PER_THREAD_ROUND_ROBIN:NONE:true" \
            "STEALING:PER_THREAD_ROUND_ROBIN:CHOOSE_TWO:false" \
            "BOTH:PER_THREAD_ROUND_ROBIN:CHOOSE_TWO:true"; do
            LABEL=${CFG%%:*}
            REST=${CFG#*:}
            DEALING=${REST%%:*}
            REST2=${REST#*:}
            STEALING=${REST2%%:*}
            PL=${REST2##*:}

            WORK="$OUTDIR/${WORKLOAD}_${LABEL}_t${T}"
            mkdir -p "$WORK"

            printf "%s %s T=%s ... " "$WORKLOAD" "$LABEL" "$T"

            docker run --rm --workdir /src -v "$(pwd):/src" nebulastream/nes-development:local \
                ./cmake-build-docker/nes-systests/systest/systest -b --show-query-performance \
                -t "$TESTFILE" --workingDir "$WORK" \
                -- --worker.query_engine.number_of_worker_threads="$T" \
                --worker.default_query_execution.execution_mode=COMPILER \
                --worker.query_engine.work_dealing_strategy="$DEALING" \
                --worker.query_engine.work_stealing_strategy="$STEALING" \
                --worker.query_engine.producer_local="$PL" \
                --worker.query_engine.work_analytics=true \
                > /dev/null 2>&1 || true

            # Collect BenchmarkResults
            RES=$(find "$WORK" -name BenchmarkResults.json 2>/dev/null | head -1)
            if [ -n "$RES" ] && [ "$(cat "$RES")" != "null" ]; then
                python3 -c "
import json, sys
for r in json.load(open(sys.argv[1])):
    t = r['time']; b = r['bytesPerSecond']; tp = r['tuplesPerSecond']
    print(f'$T,$LABEL,$WORKLOAD,{t},{b},{tp}')
" "$RES" >> "$OUTDIR/results.csv"
                echo "done"
            else
                echo "FAIL"
            fi

            # Save analytics CSV if produced
            if [ -f "work_analytics.csv" ]; then
                mv work_analytics.csv "$WORK/analytics.csv"
            fi

            rm -rf "$WORK/results" "$WORK/BenchmarkResults.json" 2>/dev/null
        done
    done
done

echo ""
echo "=== Results ==="
cat "$OUTDIR/results.csv"
echo ""
echo "=== Analytics files ==="
find "$OUTDIR" -name "analytics.csv" | sort
echo "=== DONE ==="
