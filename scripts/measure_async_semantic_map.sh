#!/usr/bin/env bash
# Drives the sync-versus-async measurements for SEM_MAP and prints the timings. It asserts nothing:
# the .test files assert the results, this script reports how long each took. Reading the numbers is
# the reader's job, because a number from one machine is not a claim.
#
#   scripts/measure_async_semantic_map.sh responsiveness   # needs Ollama, ~6 min
#   scripts/measure_async_semantic_map.sh throughput        # mock backend only, no model needed
#   scripts/measure_async_semantic_map.sh probe             # needs Ollama, ~20 min
#
# responsiveness is the measurement the framework exists for: a worker with ONE thread runs the slow
# query and an ordinary query at the same time, and what matters is what the ordinary query costs.
#
# throughput uses the mock backend's delay suffix, so it measures this framework alone rather than a
# model server. Note that a real endpoint caps concurrency of its own: Ollama serves 4 requests in
# parallel by default (OLLAMA_NUM_PARALLEL), and a synchronous query on 4 worker threads already
# reaches that, which is why a real-model throughput comparison says little without raising it.
set -euo pipefail

BUILD_DIR=${BUILD_DIR:-cmake-build-debug}
SYSTEST=${SYSTEST:-$BUILD_DIR/nes-systests/systest/systest}
DATA_DIR=${DATA_DIR:-$BUILD_DIR/bench-data}
SUITE=${SUITE:-nes-systests/semantic}

if [[ ! -x $SYSTEST ]]; then
    echo "systest not found at $SYSTEST; build it first (ninja systest)" >&2
    exit 1
fi
if [[ ! -d $DATA_DIR/async ]]; then
    echo "generating input files into $DATA_DIR"
    scripts/generate_async_benchmark_data.py --out-dir "$DATA_DIR"
fi

run() {
    local label=$1 threads=$2 concurrent=$3
    shift 3
    echo
    echo "=============================================================================="
    echo "$label  (worker threads: $threads, queries at once: $concurrent)"
    echo "=============================================================================="
    "$SYSTEST" --show-query-performance --workingDir "$BUILD_DIR/measure" --data "$DATA_DIR" \
        -n "$concurrent" "$@" -- --worker.query_engine.number_of_worker_threads="$threads"
}

case ${1:-responsiveness} in
responsiveness)
    T=$SUITE/AsyncResponsiveness.test
    # Query 3 holds no model. Its time beside query 1 versus beside query 2 is the whole point.
    run "Reference: the ordinary query on its own" 1 1 -t "$T:3"
    run "Synchronous slow query beside the ordinary one" 1 2 -t "$T:1" "$T:3"
    run "Asynchronous slow query beside the ordinary one" 1 2 -t "$T:2" "$T:3"
    ;;
throughput)
    T=$SUITE/SemanticMapAsyncThroughput.test
    # One worker thread throughout for the asynchronous queries: they do not use the pool, and
    # holding it at one makes that visible.
    run "Single buffer, asynchronous, MAX_CONCURRENCY 1" 1 1 -t "$T:1"
    run "Single buffer, asynchronous, MAX_CONCURRENCY 10" 1 1 -t "$T:2"
    for q in 3 4 5 6; do
        run "Many buffers, asynchronous, query $q" 1 1 -t "$T:$q"
    done
    run "Many buffers, synchronous, concurrency pinned to one" 1 1 -t "$T:7"
    # The synchronous path buys throughput with worker threads, so it is scanned over them.
    for threads in 1 4 8; do
        run "Many buffers, synchronous, MAX_CONCURRENCY 30" "$threads" 1 -t "$T:8"
    done
    ;;
probe)
    T=$SUITE/AsyncBenchmarkProbe.test
    for query in 1 2 3; do
        run "Probe, query $query" 4 1 -t "$T:$query"
    done
    ;;
*)
    echo "unknown mode '$1' (expected responsiveness, throughput or probe)" >&2
    exit 1
    ;;
esac
