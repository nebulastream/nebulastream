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

# End-to-end demo of the query engine task statistics stream.
#
# Starts an embedded worker with --enable_task_statistics, runs a Generator query to give the engine
# something to do, and runs a second query that reads the engine's own task events through an
# InProcess source. Everything happens in one process, so no ports or containers to coordinate.
#
# This exists instead of a systest: the statistics source never reaches end-of-stream (an idle engine
# only means nothing has happened yet), and the number of events an engine produces is not
# reproducible, so there is no stable expected-result block for a systest to compare against. The
# checks below assert the invariants that *are* stable.

set -eo pipefail

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

BUILD_DIR="cmake-build-debug"
IMAGE="nebulastream/nes-development:local"
DURATION=12
DO_BUILD=1

usage()
{
    cat <<EOF
Usage: $(basename "$0") [options]

  --duration <seconds>   How long to let the queries run (default: ${DURATION}, minimum: 6)
  --build-dir <dir>      CMake build directory, relative to the repo root (default: ${BUILD_DIR})
  --image <image>        Development container image (default: ${IMAGE})
  --no-build             Use the binaries as they are, do not build first
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)  DURATION="$2"; shift 2 ;;
        --build-dir) BUILD_DIR="$2";  shift 2 ;;
        --image)     IMAGE="$2";      shift 2 ;;
        --no-build)  DO_BUILD=0;      shift ;;
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
    run_in_container cmake --build "$BUILD_DIR" -j --target nes-repl-embedded
fi

OUT_REL="${BUILD_DIR}/engine-stats-demo"
OUT_HOST="${REPO_ROOT}/${OUT_REL}"
OUT_CONTAINER="${MOUNT_PATH}/${OUT_REL}"
rm -rf "$OUT_HOST"
mkdir -p "$OUT_HOST"

# Leave the load query enough runtime to produce work, but let it finish before we tear everything
# down, so the statistics stream shows a query starting and ending rather than being cut off.
LOAD_RUNTIME_MS=$(( (DURATION - 4) * 1000 ))

cat > "${OUT_HOST}/demo.sql" <<EOF
-- A Generator query, purely to give the engine something to do.
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       ${LOAD_RUNTIME_MS} AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 500' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK loadSink(ts UINT64) TYPE File
       SET('${OUT_CONTAINER}/load.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The engine's own task events. FEED_NAME has to match the worker's --task_statistics_feed.
CREATE LOGICAL SOURCE engineStats(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id VARSIZED,
       pipeline_id UINT64, task_id UINT64, tuples UINT64);
CREATE PHYSICAL SOURCE FOR engineStats TYPE InProcess SET(
       'engine_events' AS "SOURCE".FEED_NAME,
       'CSV' as INPUT_FORMATTER."TYPE");
CREATE SINK statsSink(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id VARSIZED,
       pipeline_id UINT64, task_id UINT64, tuples UINT64) TYPE File
       SET('${OUT_CONTAINER}/stats.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The observer goes first, so that it sees the load query start up.
SELECT event_type, ts_us, thread_id, query_id, pipeline_id, task_id, tuples
       FROM engineStats INTO statsSink;
SELECT ts FROM endless INTO loadSink;
EOF

echo "==> Running both queries for ${DURATION}s"
# The REPL exits as soon as stdin closes, so holding stdin open is what keeps the queries running.
run_in_container bash -c "
    cd '${OUT_CONTAINER}' &&
    ( cat demo.sql; sleep ${DURATION} ) |
    '${MOUNT_PATH}/${BUILD_DIR}/nes-frontend/apps/nes-repl-embedded' -d -- --enable_task_statistics=true \
        > repl.out 2>&1
" || { echo "The REPL failed, see ${OUT_REL}/repl.out" >&2; exit 1; }

STATS="${OUT_HOST}/stats.csv"
if [[ ! -s "$STATS" ]]; then
    echo "FAIL: no statistics were written to ${OUT_REL}/stats.csv" >&2
    exit 1
fi

data_rows()   { tail -n +2 "$STATS"; }
count_event() { data_rows | cut -d, -f1 | grep -cx "$1" || true; }

ROWS=$(data_rows | wc -l)
QUERY_IDS=$(data_rows | cut -d, -f4 | sort -u | wc -l)
STARTS=$(count_event TASK_START)
DONES=$(count_event TASK_DONE)
EMITS=$(count_event TASK_EMIT)
# Finding no drop warnings is the good case, but it makes grep exit non-zero under 'pipefail'.
DROPS=$(cat "${OUT_HOST}"/*.log 2>/dev/null | grep -c "dropped so far" || true)

echo
echo "==> Results in ${OUT_REL}/"
printf '    statistics rows : %s\n' "$ROWS"
printf '    TASK_START      : %s\n' "$STARTS"
printf '    TASK_DONE       : %s\n' "$DONES"
printf '    TASK_EMIT       : %s\n' "$EMITS"
printf '    observed queries: %s\n' "$QUERY_IDS"
echo
echo "    tasks per pipeline:"
data_rows | awk -F, '$1=="TASK_DONE" {print $5}' | sort -n | uniq -c \
    | awk '{printf "        pipeline %s: %s tasks\n", $2, $1}'
echo
echo "    first rows:"
sed -n '2,4p' "$STATS" | sed 's/^/        /'
echo

FAILURES=0
check()
{
    if [[ "$2" == "$3" ]]; then
        printf '    [ ok ] %s\n' "$1"
    else
        printf '    [FAIL] %s (expected %s, got %s)\n' "$1" "$3" "$2"
        FAILURES=$((FAILURES + 1))
    fi
}

echo "==> Checks"
check "statistics rows were produced"            "$([[ "$ROWS" -gt 0 ]] && echo yes || echo no)" "yes"
check "every started task also completed"        "$STARTS" "$DONES"
check "the statistics query excluded itself"     "$QUERY_IDS" "1"
check "no events were dropped"                   "$DROPS" "0"

echo
if [[ "$FAILURES" -ne 0 ]]; then
    echo "${FAILURES} check(s) failed. Logs: ${OUT_REL}/repl.out and ${OUT_REL}/nes-repl.log" >&2
    exit 1
fi
echo "All checks passed. Inspect ${OUT_REL}/stats.csv for the full stream."
