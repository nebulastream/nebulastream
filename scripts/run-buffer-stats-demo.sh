#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end demo of the buffer manager statistics stream.
#
# Starts an embedded worker with --enable_buffer_statistics, runs a Generator query to put the buffer
# pool under load, and runs a second query that reads the pool's own statistics through an InProcess
# source. Everything happens in one process, so there are no ports or containers to coordinate.
#
# This exists instead of a systest: the statistics source never reaches end-of-stream (an idle pool
# only means nothing has happened yet), and how many buffers a query churns through is not
# reproducible, so there is no stable expected-result block for a systest to compare against. The
# checks below assert the invariants that *are* stable.
#
# Unlike the task statistics stream, the observing query cannot amplify itself here: the listener
# counts events and emits one row per --buffer_statistics_interval_ms regardless of how many events
# arrived, so the row rate is bounded by the wall clock.

set -eo pipefail

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

BUILD_DIR="cmake-build-debug"
IMAGE="nebulastream/nes-development:local"
DURATION=12
INTERVAL_MS=100
DO_BUILD=1
# Deliberately not a bare `-j`. A single Debug link of this project peaks at 1.7-2.7 GB (mold materialises a
# 0.4-1 GB output binary out of ~400 static archives), and a measured `-j 6` build peaks around 21 GB. Ninja's
# default of nproc+2 therefore demands far more than a typical developer machine has, and it wedges in
# dirty-page writeback before the OOM killer ever gets a chance to fire.
BUILD_JOBS=4

usage()
{
    cat <<EOF
Usage: $(basename "$0") [options]

  --duration <seconds>   How long to let the queries run (default: ${DURATION}, minimum: 6)
  --interval <ms>        Statistics flush interval (default: ${INTERVAL_MS})
  --build-dir <dir>      CMake build directory, relative to the repo root (default: ${BUILD_DIR})
  --image <image>        Development container image (default: ${IMAGE})
  --jobs <n>             Parallel build jobs (default: ${BUILD_JOBS}; budget ~3 GB of RAM per job)
  --no-build             Use the binaries as they are, do not build first
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)  DURATION="$2";    shift 2 ;;
        --interval)  INTERVAL_MS="$2"; shift 2 ;;
        --build-dir) BUILD_DIR="$2";   shift 2 ;;
        --image)     IMAGE="$2";       shift 2 ;;
        --jobs)      BUILD_JOBS="$2";  shift 2 ;;
        --no-build)  DO_BUILD=0;       shift ;;
        -h|--help)   usage; exit 0 ;;
        *)           echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$DURATION" -lt 6 ]]; then
    echo "--duration has to be at least 6 seconds, the queries need time to start up" >&2
    exit 2
fi

# CMake bakes absolute paths into its cache, so an existing build directory dictates where the repo
# has to be mounted. Reusing it beats a full reconfigure of a project this size.
CACHE="${BUILD_DIR}/CMakeCache.txt"
if [[ -f "$CACHE" ]]; then
    MOUNT_PATH="$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "$CACHE")"
fi
MOUNT_PATH="${MOUNT_PATH:-$REPO_ROOT}"

run_in_container()
{
    docker run --rm --workdir "$MOUNT_PATH" -v "${REPO_ROOT}:${MOUNT_PATH}" "$IMAGE" "$@"
}

echo "==> Repository mounted at ${MOUNT_PATH} in ${IMAGE}"

if [[ ! -f "$CACHE" ]]; then
    echo "==> No build directory yet, configuring (this takes a while)"
    run_in_container cmake -B "$BUILD_DIR"
fi

if [[ "$DO_BUILD" -eq 1 ]]; then
    echo "==> Building nes-repl-embedded"
    run_in_container cmake --build "$BUILD_DIR" -j "$BUILD_JOBS" --target nes-repl-embedded
fi

OUT_REL="${BUILD_DIR}/buffer-stats-demo"
OUT_HOST="${REPO_ROOT}/${OUT_REL}"
OUT_CONTAINER="${MOUNT_PATH}/${OUT_REL}"
rm -rf "$OUT_HOST"
mkdir -p "$OUT_HOST"

# Leave the load query enough runtime to churn through buffers, but let it finish before we tear
# everything down, so the statistics stream shows the pool under load and afterwards at rest.
LOAD_RUNTIME_MS=$(( (DURATION - 4) * 1000 ))

BUFFER_STATS_FIELDS="ts_us UINT64, interval_ms UINT64, pooled_total UINT64, pooled_available UINT64,
       pooled_available_min UINT64, pooled_acquired UINT64, pooled_recycled UINT64,
       pooled_request_failures UINT64, unpooled_allocated UINT64, unpooled_bytes_requested UINT64,
       unpooled_bytes_in_use UINT64, unpooled_chunks_allocated UINT64, unpooled_chunks_released UINT64,
       unpooled_request_failures UINT64, rows_dropped UINT64"

cat > "${OUT_HOST}/demo.sql" <<EOF
-- A Generator query, purely to put the buffer pool under load. The VARSIZED payload is what drives the
-- unpooled columns of the statistics stream; a fixed-size schema alone would leave them at zero.
CREATE LOGICAL SOURCE endless(ts UINT64, payload VARSIZED);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       ${LOAD_RUNTIME_MS} AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 500' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 10000000 1, RANDOMSTR 512 8192' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK loadSink(ts UINT64, length UINT64) TYPE File
       SET('${OUT_CONTAINER}/load.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The pool's own statistics. FEED_NAME has to match the worker's --buffer_statistics_feed.
CREATE LOGICAL SOURCE bufferStats(${BUFFER_STATS_FIELDS});
CREATE PHYSICAL SOURCE FOR bufferStats TYPE InProcess SET(
       'buffer_events' AS "SOURCE".FEED_NAME,
       'CSV' as INPUT_FORMATTER."TYPE");
CREATE SINK statsSink(${BUFFER_STATS_FIELDS}) TYPE File
       SET('${OUT_CONTAINER}/stats.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The observer goes first, so that it sees the pool before the load query touches it.
SELECT ts_us, interval_ms, pooled_total, pooled_available, pooled_available_min, pooled_acquired,
       pooled_recycled, pooled_request_failures, unpooled_allocated, unpooled_bytes_requested,
       unpooled_bytes_in_use, unpooled_chunks_allocated, unpooled_chunks_released,
       unpooled_request_failures, rows_dropped
       FROM bufferStats INTO statsSink;
SELECT ts, OCTET_LENGTH(payload) as length FROM endless INTO loadSink;
EOF

echo "==> Running both queries for ${DURATION}s, one statistics row every ${INTERVAL_MS}ms"
# The REPL exits as soon as stdin closes, so holding stdin open is what keeps the queries running.
run_in_container bash -c "
    cd '${OUT_CONTAINER}' &&
    ( cat demo.sql; sleep ${DURATION} ) |
    '${MOUNT_PATH}/${BUILD_DIR}/nes-frontend/apps/nes-repl-embedded' -d -- --enable_buffer_statistics=true \
        --buffer_statistics_interval_ms=${INTERVAL_MS} > repl.out 2>&1
" || { echo "The REPL failed, see ${OUT_REL}/repl.out" >&2; exit 1; }

STATS="${OUT_HOST}/stats.csv"
if [[ ! -s "$STATS" ]]; then
    echo "FAIL: no statistics were written to ${OUT_REL}/stats.csv" >&2
    exit 1
fi

data_rows() { tail -n +2 "$STATS"; }
# Sum of a 1-based column over all data rows.
sum_col()   { data_rows | awk -F, -v c="$1" '{ total += $c } END { printf "%d", total + 0 }'; }
max_col()   { data_rows | awk -F, -v c="$1" '{ if ($c > m) m = $c } END { printf "%d", m + 0 }'; }

ROWS=$(data_rows | wc -l)
POOL_TOTAL=$(max_col 3)
ACQUIRED=$(sum_col 6)
RECYCLED=$(sum_col 7)
FAILURES=$(sum_col 8)
UNPOOLED=$(sum_col 9)
CHUNKS_ALLOC=$(sum_col 12)
CHUNKS_FREED=$(sum_col 13)
PEAK_IN_USE=$(data_rows | awk -F, -v t="$POOL_TOTAL" '{ if (t - $5 > m) m = t - $5 } END { printf "%d", m + 0 }')
# A row whose reported fill level exceeds the pool size would mean the gauges are wrong.
OVERSHOOT=$(data_rows | awk -F, -v t="$POOL_TOTAL" '($4 > t) || ($5 > t) { n++ } END { printf "%d", n + 0 }')
# Finding no drop warnings is the good case, but it makes grep exit non-zero under 'pipefail'.
DROPS=$(cat "${OUT_HOST}"/*.log 2>/dev/null | grep -c "dropped so far" || true)

echo
echo "==> Results in ${OUT_REL}/"
printf '    statistics rows        : %s\n' "$ROWS"
printf '    pool size              : %s buffers\n' "$POOL_TOTAL"
printf '    peak buffers in use    : %s\n' "$PEAK_IN_USE"
printf '    pooled acquired        : %s\n' "$ACQUIRED"
printf '    pooled recycled        : %s\n' "$RECYCLED"
printf '    acquire failures       : %s\n' "$FAILURES"
printf '    unpooled buffers       : %s\n' "$UNPOOLED"
printf '    unpooled chunks a/f    : %s / %s\n' "$CHUNKS_ALLOC" "$CHUNKS_FREED"
echo
echo "    first rows:"
sed -n '2,4p' "$STATS" | sed 's/^/        /'
echo

CHECK_FAILURES=0
check()
{
    if [[ "$2" == "$3" ]]; then
        printf '    [ ok ] %s\n' "$1"
    else
        printf '    [FAIL] %s (expected %s, got %s)\n' "$1" "$3" "$2"
        CHECK_FAILURES=$((CHECK_FAILURES + 1))
    fi
}

echo "==> Checks"
check "statistics rows were produced"          "$([[ "$ROWS" -gt 0 ]] && echo yes || echo no)" "yes"
check "the pool reported its size"             "$([[ "$POOL_TOTAL" -gt 0 ]] && echo yes || echo no)" "yes"
check "the load query churned through buffers" "$([[ "$ACQUIRED" -gt 0 ]] && echo yes || echo no)" "yes"
check "the VARSIZED payload hit unpooled memory" "$([[ "$UNPOOLED" -gt 0 ]] && echo yes || echo no)" "yes"
# Every acquired buffer is eventually recycled, but a buffer acquired in the last interval before
# shutdown is recycled after the final row, so the two totals only have to agree within the pool size.
check "acquires and recycles balance out"      \
    "$([[ $((ACQUIRED - RECYCLED)) -le "$POOL_TOTAL" && $((RECYCLED - ACQUIRED)) -le "$POOL_TOTAL" ]] && echo yes || echo no)" "yes"
check "no row reported more than a full pool"  "$OVERSHOOT" "0"
check "every unpooled chunk was freed again"   "$CHUNKS_ALLOC" "$CHUNKS_FREED"
check "no rows were dropped"                   "$DROPS" "0"

echo
if [[ "$CHECK_FAILURES" -ne 0 ]]; then
    echo "${CHECK_FAILURES} check(s) failed. Logs: ${OUT_REL}/repl.out and ${OUT_REL}/nes-repl.log" >&2
    exit 1
fi
echo "All checks passed. Inspect ${OUT_REL}/stats.csv for the full stream."
