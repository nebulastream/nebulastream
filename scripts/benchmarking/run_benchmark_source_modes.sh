#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# Benchmark script that runs YahooStreamingBenchmark with each BenchmarkSource
# mode (file, memory, in_place, tmpfs_cold, tmpfs_warm) and compares results.
#
# Usage:
#   ./run_benchmark_source_modes.sh [OPTIONS]
#
# Options:
#   --build-dir <path>      CMake build directory (default: cmake-build-debug)
#   --test-file <path>      .test file relative to nes-systests/ (default: benchmark_small/YahooStreamingBenchmark.test)
#   --data <path>           Test data directory (passed to systest --data)
#   --iterations <n>        Iterations per mode (default: 3)
#   --modes <m1,m2,...>     Comma-separated modes (default: file,memory,in_place,tmpfs_cold,tmpfs_warm)
#   --tmpfs-path <path>     tmpfs mount point (default: /dev/shm)
#   --worker-threads <n>    Worker threads (default: 4)
#   --output <path>         Output CSV path (default: benchmark_results.csv)
#   --help                  Show this message
# =============================================================================

set -euo pipefail

# ---- Defaults ----
BUILD_DIR="cmake-build-debug"
TEST_FILE="benchmark_small/YahooStreamingBenchmark.test"
DATA_DIR=""
ITERATIONS=3
MODES="file,memory,in_place,tmpfs_cold,tmpfs_warm"
TMPFS_PATH="/dev/shm"
WORKER_THREADS=4
OUTPUT_CSV="benchmark_results.csv"

# ---- Parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir)       BUILD_DIR="$2";        shift 2 ;;
        --test-file)       TEST_FILE="$2";        shift 2 ;;
        --data)            DATA_DIR="$2";         shift 2 ;;
        --iterations)      ITERATIONS="$2";       shift 2 ;;
        --modes)           MODES="$2";            shift 2 ;;
        --tmpfs-path)      TMPFS_PATH="$2";       shift 2 ;;
        --worker-threads)  WORKER_THREADS="$2";   shift 2 ;;
        --output)          OUTPUT_CSV="$2";       shift 2 ;;
        --help)
            sed -n '/^# Usage:/,/^# ====/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---- Locate tools and files ----
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYSTEST_DIR="${REPO_ROOT}/nes-systests"
SYSTEST_BIN="${REPO_ROOT}/${BUILD_DIR}/systest/systest"

if [[ ! -x "$SYSTEST_BIN" ]]; then
    echo "ERROR: systest binary not found at $SYSTEST_BIN"
    echo "       Build it first: cmake --build ${BUILD_DIR} -j --target systest"
    exit 1
fi

SOURCE_TEST="${SYSTEST_DIR}/${TEST_FILE}"
if [[ ! -f "$SOURCE_TEST" ]]; then
    echo "ERROR: Test file not found: $SOURCE_TEST"
    exit 1
fi

# ---- Setup ----
WORK_BASE=$(mktemp -d "${REPO_ROOT}/${BUILD_DIR}/benchmark_modes_XXXXXX")
trap 'rm -rf "$WORK_BASE"' EXIT

IFS=',' read -ra MODE_LIST <<< "$MODES"

echo ""
echo "================================================================="
echo " BenchmarkSource Mode Comparison"
echo " Test file:       ${TEST_FILE}"
echo " Modes:           ${MODES}"
echo " Iterations:      ${ITERATIONS}"
echo " Worker threads:  ${WORKER_THREADS}"
echo " Output CSV:      ${OUTPUT_CSV}"
echo " Working dir:     ${WORK_BASE}"
echo "================================================================="
echo ""

# ---- Generate per-mode .test files ----
generate_test_file() {
    local mode="$1"
    local out_dir="${WORK_BASE}/tests"
    mkdir -p "$out_dir"
    local out_file="${out_dir}/YSB_${mode}.test"

    local set_clause
    if [[ "$mode" == "tmpfs_cold" || "$mode" == "tmpfs_warm" ]]; then
        set_clause="SET('${mode}' AS \`SOURCE\`.MODE, '${TMPFS_PATH}' AS \`SOURCE\`.TMPFS_PATH)"
    else
        set_clause="SET('${mode}' AS \`SOURCE\`.MODE)"
    fi

    # Replace "TYPE File" with "TYPE Benchmark SET(...)"
    # Handle both "TYPE File;" and "TYPE File" (end of line)
    sed -E "s|TYPE File;|TYPE Benchmark ${set_clause};|g; s|TYPE File$|TYPE Benchmark ${set_clause}|g" \
        "$SOURCE_TEST" > "$out_file"

    echo "$out_file"
}

# ---- CSV header ----
echo "mode,iteration,query,time_seconds,bytes_per_second,tuples_per_second" > "$OUTPUT_CSV"

# ---- Run benchmarks ----
for mode in "${MODE_LIST[@]}"; do
    # Check tmpfs availability for tmpfs modes
    if [[ "$mode" == "tmpfs_cold" || "$mode" == "tmpfs_warm" ]]; then
        if [[ ! -d "$TMPFS_PATH" ]]; then
            echo "SKIP: mode=$mode (tmpfs path '$TMPFS_PATH' not found)"
            continue
        fi
    fi

    echo "--- Mode: $mode ---"
    test_file=$(generate_test_file "$mode")

    for iter in $(seq 1 "$ITERATIONS"); do
        work_dir="${WORK_BASE}/${mode}/run_${iter}"
        mkdir -p "$work_dir"

        # Build command
        cmd=("$SYSTEST_BIN" -b --show-query-performance
            -t "$test_file"
            --workingDir "$work_dir"
        )
        if [[ -n "$DATA_DIR" ]]; then
            cmd+=(--data "$DATA_DIR")
        fi
        cmd+=(--
            "--worker.query_engine.number_of_worker_threads=${WORKER_THREADS}"
            "--worker.default_query_execution.execution_mode=COMPILER"
        )

        printf "  Iteration %d/%d... " "$iter" "$ITERATIONS"
        if "${cmd[@]}" > "${work_dir}/stdout.log" 2>&1; then
            # Parse BenchmarkResults.json and append to CSV
            results_json="${work_dir}/BenchmarkResults.json"
            if [[ -f "$results_json" ]]; then
                python3 - "$results_json" "$mode" "$iter" >> "$OUTPUT_CSV" <<'PARSE_EOF'
import json, sys
with open(sys.argv[1]) as f:
    mode, iteration = sys.argv[2], sys.argv[3]
    for r in json.load(f):
        name = r.get("query name", "?")
        t = r.get("time", 0)
        bps = r.get("bytesPerSecond", 0)
        tps = r.get("tuplesPerSecond", 0)
        print(f"{mode},{iteration},{name},{t},{bps},{tps}")
PARSE_EOF
                echo "done"
            else
                echo "WARNING: no BenchmarkResults.json"
            fi
        else
            echo "FAILED (see ${work_dir}/stdout.log)"
        fi
    done
    echo ""
done

# ---- Print summary ----
python3 "${SCRIPT_DIR}/summarize_benchmark_results.py" "$OUTPUT_CSV"
