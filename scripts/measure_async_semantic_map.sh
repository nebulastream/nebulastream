#!/usr/bin/env bash
# Measures what the asynchronous operator framework is for. Two questions, one answer each:
#
#   1. Does throughput rise with LLM.MAX_CONCURRENCY?  Queries 1 to 4 of
#      nes-systests/semantic/SemanticMapAsyncThroughput.test run the same 30 records at 200 ms per
#      request with maxConcurrency 1, 5, 10 and 30. Query 5 is the synchronous path over the same
#      delay, as the point of comparison.
#
#   2. Does a waiting operator hold up unrelated work?  Query 6 has no model in it. It is run
#      alongside the slow query on a worker with a single thread, once against the asynchronous
#      path and once against the synchronous one. The interesting number is how long query 6 takes.
#
# Nothing here asserts a timing: the test file asserts the results, this script prints the times.
# Reading them is the reader's job, because a number from one machine is not a claim.
set -euo pipefail

BUILD_DIR=${BUILD_DIR:-cmake-build-debug}
SYSTEST=${SYSTEST:-$BUILD_DIR/nes-systests/systest/systest}
TEST_FILE=${TEST_FILE:-nes-systests/semantic/SemanticMapAsyncThroughput.test}
WORK_DIR=${WORK_DIR:-$BUILD_DIR/async-measurement}

if [[ ! -x $SYSTEST ]]; then
    echo "systest binary not found at $SYSTEST; build it first (ninja systest)" >&2
    exit 1
fi

mkdir -p "$WORK_DIR"

run() {
    local label=$1 threads=$2 concurrent=$3
    shift 3
    echo
    echo "=============================================================================="
    echo "$label  (worker threads: $threads, concurrent queries: $concurrent)"
    echo "=============================================================================="
    "$SYSTEST" \
        --show-query-performance \
        --groups Semantic-Nightly \
        --workingDir "$WORK_DIR" \
        -n "$concurrent" \
        "$@" \
        -- --worker.query_engine.number_of_worker_threads="$threads"
}

# Question 1: one query at a time, so the time printed is that query's own.
for query in 01 02 03 04; do
    run "Throughput, asynchronous, query $query" 4 1 -t "$TEST_FILE:$query"
done
run "Throughput, synchronous, query 05" 4 1 -t "$TEST_FILE:05"

# Question 2: the slow query and the ordinary one at the same time, on a single worker thread.
run "Responsiveness, asynchronous slow query beside an ordinary one" 1 2 -t "$TEST_FILE:01" -t "$TEST_FILE:06"
run "Responsiveness, synchronous slow query beside an ordinary one" 1 2 -t "$TEST_FILE:05" -t "$TEST_FILE:06"
run "Responsiveness, the ordinary query on its own, for reference" 1 1 -t "$TEST_FILE:06"
