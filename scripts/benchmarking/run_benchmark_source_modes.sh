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
# Supports both local execution and remote execution via SSH.
#
# Usage:
#   ./run_benchmark_source_modes.sh [OPTIONS]
#
# Local options:
#   --build-dir <path>      CMake build directory (default: cmake-build-debug)
#   --test-file <path>      .test file relative to nes-systests/ (default: benchmark_small/YahooStreamingBenchmark.test)
#   --data <path>           Test data directory (passed to systest --data)
#   --iterations <n>        Iterations per mode (default: 3)
#   --modes <m1,m2,...>     Comma-separated source modes (default: file,memory,in_place,tmpfs_cold,tmpfs_warm)
#   --scheduling <s1,s2,..> Comma-separated scheduling strategies (default: GLOBAL_QUEUE)
#                           Options: GLOBAL_QUEUE, PER_THREAD_ROUND_ROBIN, PER_THREAD_SMALLEST_QUEUE
#   --work-stealing <t/f,..> Comma-separated work stealing settings (default: false)
#   --tmpfs-path <path>     tmpfs mount point (default: /dev/shm)
#   --worker-threads <n>    Worker threads (default: 4). Comma-separated for multi-thread sweep (e.g., 1,2,4,8,16)
#   --output <path>         Output CSV path (default: benchmark_results.csv)
#
# Remote options (builds natively via Nix, no Docker required):
#   --remote <user@host>    Run benchmarks on a remote node via SSH
#   --remote-repo <path>    Path to the repo on the remote node (default: ~/nebulastream)
#   --remote-build <path>   Build directory on the remote node (default: cmake-build-release)
#   --remote-data <path>    Test data directory on the remote node
#   --remote-pass <pass>    SSH password (uses sshpass; also settable via REMOTE_PASS env var)
#   --ssh-opts <opts>       Extra SSH options (e.g., "-p 2222 -i ~/.ssh/mykey")
#   --skip-sync             Skip rsync of the local repo to the remote node
#   --skip-build            Skip building on the remote node (assume already built)
#   --build-jobs <n>        Number of parallel build jobs on remote (default: $(nproc) on remote)
#   --cmake-opts <opts>     Extra CMake configure options for remote build
#
#   --help                  Show this message
# =============================================================================

set -euo pipefail

# ---- Defaults ----
BUILD_DIR="cmake-build-debug"
TEST_FILE="benchmark_small/YahooStreamingBenchmark.test"
DATA_DIR=""
ITERATIONS=3
MODES="file,memory,in_place,tmpfs_cold,tmpfs_warm"
SCHEDULING="GLOBAL_QUEUE"
WORK_STEALING="false"
TMPFS_PATH="/dev/shm"
WORKER_THREADS=4
OUTPUT_CSV="benchmark_results.csv"

# Remote defaults
REMOTE=""
REMOTE_REPO=""
REMOTE_BUILD=""
REMOTE_DATA=""
SSH_OPTS=""
SKIP_SYNC=false
SKIP_BUILD=false
BUILD_JOBS=""
CMAKE_OPTS=""

# ---- Parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir)       BUILD_DIR="$2";        shift 2 ;;
        --test-file)       TEST_FILE="$2";        shift 2 ;;
        --data)            DATA_DIR="$2";         shift 2 ;;
        --iterations)      ITERATIONS="$2";       shift 2 ;;
        --modes)           MODES="$2";            shift 2 ;;
        --scheduling)      SCHEDULING="$2";       shift 2 ;;
        --work-stealing)   WORK_STEALING="$2";    shift 2 ;;
        --tmpfs-path)      TMPFS_PATH="$2";       shift 2 ;;
        --worker-threads)  WORKER_THREADS="$2";   shift 2 ;;
        --output)          OUTPUT_CSV="$2";       shift 2 ;;
        --remote)          REMOTE="$2";           shift 2 ;;
        --remote-repo)     REMOTE_REPO="$2";      shift 2 ;;
        --remote-build)    REMOTE_BUILD="$2";     shift 2 ;;
        --remote-data)     REMOTE_DATA="$2";      shift 2 ;;
        --remote-pass)     REMOTE_PASS="$2";      shift 2 ;;
        --ssh-opts)        SSH_OPTS="$2";         shift 2 ;;
        --skip-sync)       SKIP_SYNC=true;        shift ;;
        --skip-build)      SKIP_BUILD=true;       shift ;;
        --build-jobs)      BUILD_JOBS="$2";       shift 2 ;;
        --cmake-opts)      CMAKE_OPTS="$2";       shift 2 ;;
        --help)
            sed -n '/^# Usage:/,/^# ====/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ============================================================================
# Remote execution mode
# ============================================================================
if [[ -n "$REMOTE" ]]; then
    # Resolve remote defaults
    : "${REMOTE_REPO:=~/nebulastream}"
    : "${REMOTE_BUILD:=cmake-build-release}"

    # Resolve remote password auth
    REMOTE_PASS="${REMOTE_PASS:-}"

    echo ""
    echo "================================================================="
    echo " Remote BenchmarkSource Mode Comparison"
    echo " Remote host:     ${REMOTE}"
    echo " Remote repo:     ${REMOTE_REPO}"
    echo " Remote build:    ${REMOTE_BUILD}"
    echo " Test file:       ${TEST_FILE}"
    echo " Source modes:    ${MODES}"
    echo " Scheduling:      ${SCHEDULING}"
    echo " Work stealing:   ${WORK_STEALING}"
    echo " Iterations:      ${ITERATIONS}"
    echo " Worker threads:  ${WORKER_THREADS}"
    echo " Skip sync:       ${SKIP_SYNC}"
    echo " Skip build:      ${SKIP_BUILD}"
    echo " Output CSV:      ${OUTPUT_CSV} (local)"
    echo "================================================================="
    echo ""

    # Build the SSH/SCP/rsync command prefixes
    # Support password auth via sshpass if REMOTE_PASS is set
    SSH_BASE_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=30)
    SCP_BASE_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=30)
    if [[ -n "$SSH_OPTS" ]]; then
        read -ra ssh_extra <<< "$SSH_OPTS"
        SSH_BASE_OPTS+=("${ssh_extra[@]}")
        # Convert -p <port> to -P <port> for scp, pass other opts through
        scp_extra=()
        i=0
        while [[ $i -lt ${#ssh_extra[@]} ]]; do
            if [[ "${ssh_extra[$i]}" == "-p" && $((i+1)) -lt ${#ssh_extra[@]} ]]; then
                scp_extra+=("-P" "${ssh_extra[$((i+1))]}")
                i=$((i+2))
            else
                scp_extra+=("${ssh_extra[$i]}")
                i=$((i+1))
            fi
        done
        SCP_BASE_OPTS+=("${scp_extra[@]}")
    fi

    if [[ -n "$REMOTE_PASS" ]]; then
        if ! command -v sshpass &>/dev/null; then
            echo "ERROR: sshpass is required for password auth. Install with: brew install sshpass / apt install sshpass"
            exit 1
        fi
        SSH_CMD=(sshpass -p "$REMOTE_PASS" ssh "${SSH_BASE_OPTS[@]}")
        SCP_CMD=(sshpass -p "$REMOTE_PASS" scp "${SCP_BASE_OPTS[@]}")
        RSYNC_SSH="sshpass -p $REMOTE_PASS ssh ${SSH_BASE_OPTS[*]}"
    else
        SSH_BASE_OPTS+=(-o BatchMode=yes)
        SCP_BASE_OPTS+=(-o BatchMode=yes)
        SSH_CMD=(ssh "${SSH_BASE_OPTS[@]}")
        SCP_CMD=(scp "${SCP_BASE_OPTS[@]}")
        RSYNC_SSH="ssh ${SSH_BASE_OPTS[*]}"
    fi

    # Test SSH connectivity
    echo "Testing SSH connection to ${REMOTE}..."
    if ! "${SSH_CMD[@]}" "$REMOTE" "echo ok" > /dev/null 2>&1; then
        echo "ERROR: Cannot SSH to ${REMOTE}."
        if [[ -n "$REMOTE_PASS" ]]; then
            echo "       Check password and that sshpass is installed."
        else
            echo "       Ensure SSH key auth is set up, or set REMOTE_PASS env var."
            echo "       Try: ssh-copy-id ${REMOTE}"
        fi
        exit 1
    fi
    echo "SSH connection OK."
    echo ""

    # ---- Step 1: Sync local repo to remote ----
    if [[ "$SKIP_SYNC" == false ]]; then
        echo "Syncing local repo to ${REMOTE}:${REMOTE_REPO}..."
        "${SSH_CMD[@]}" "$REMOTE" "mkdir -p ${REMOTE_REPO}"

        rsync -az --delete \
            -e "$RSYNC_SSH" \
            --exclude '.git' \
            --exclude 'cmake-build-*' \
            --exclude '.cache' \
            --exclude '__pycache__' \
            --exclude '*.o' \
            --exclude '*.a' \
            --exclude '*.so' \
            "${REPO_ROOT}/" \
            "${REMOTE}:${REMOTE_REPO}/"

        # Also sync any generated test data (e.g., doubled CSV files) that live in the build dir
        if [[ -n "$DATA_DIR" && -d "${REPO_ROOT}/${DATA_DIR}" ]]; then
            echo "Syncing test data from ${DATA_DIR}..."
            "${SSH_CMD[@]}" "$REMOTE" "mkdir -p ${REMOTE_REPO}/${DATA_DIR}"
            rsync -az \
                -e "$RSYNC_SSH" \
                "${REPO_ROOT}/${DATA_DIR}/" \
                "${REMOTE}:${REMOTE_REPO}/${DATA_DIR}/"
        fi

        echo "Sync complete."
        echo ""
    else
        echo "Skipping rsync (--skip-sync)."
        echo ""
    fi

    # ---- Step 2: Ensure Nix is available on remote ----
    echo "Checking for Nix on ${REMOTE}..."
    if ! "${SSH_CMD[@]}" "$REMOTE" "command -v nix > /dev/null 2>&1"; then
        echo "  Nix not found. Installing Nix on ${REMOTE}..."
        "${SSH_CMD[@]}" "$REMOTE" "sh <(curl -L https://nixos.org/nix/install) --daemon --yes" \
            > /tmp/nes_remote_nix_install.log 2>&1 || {
            echo "  ERROR: Nix installation failed. Last 30 lines:"
            tail -30 /tmp/nes_remote_nix_install.log
            exit 1
        }
        echo "  Nix installed successfully."
    else
        echo "  Nix is available."
    fi

    # Ensure flakes are enabled on the remote
    "${SSH_CMD[@]}" "$REMOTE" "mkdir -p ~/.config/nix && grep -q 'experimental-features' ~/.config/nix/nix.conf 2>/dev/null || echo 'experimental-features = nix-command flakes' >> ~/.config/nix/nix.conf"

    # Helper: run a command inside the Nix dev shell on the remote host
    # Writes a temp script on the remote, then executes it inside nix develop
    remote_nix() {
        local encoded_cmd
        encoded_cmd=$(printf '%s' "$1" | base64)
        "${SSH_CMD[@]}" "$REMOTE" "echo '${encoded_cmd}' | base64 -d > /tmp/nes_remote_cmd.sh && chmod +x /tmp/nes_remote_cmd.sh && cd ${REMOTE_REPO} && nix develop .#default --command bash /tmp/nes_remote_cmd.sh; rm -f /tmp/nes_remote_cmd.sh"
    }

    if [[ "$SKIP_BUILD" == false ]]; then
        echo "Configuring and building on ${REMOTE} (via Nix)..."

        # Determine parallel jobs
        if [[ -z "$BUILD_JOBS" ]]; then
            BUILD_JOBS=$("${SSH_CMD[@]}" "$REMOTE" "nproc 2>/dev/null || echo 4")
        fi

        echo "  Configuring (cmake via Nix dev shell)..."
        CMAKE_CMD="./.nix/nix-cmake.sh -B ${REMOTE_BUILD} -DCMAKE_BUILD_TYPE=Release -G Ninja"
        if [[ -n "$CMAKE_OPTS" ]]; then
            CMAKE_CMD+=" ${CMAKE_OPTS}"
        fi
        if ! "${SSH_CMD[@]}" "$REMOTE" "cd ${REMOTE_REPO} && ${CMAKE_CMD}" > /tmp/nes_remote_configure.log 2>&1; then
            echo "  ERROR: CMake configure failed. Last 30 lines:"
            tail -30 /tmp/nes_remote_configure.log
            exit 1
        fi

        echo "  Building systest (${BUILD_JOBS} jobs)..."
        if ! "${SSH_CMD[@]}" "$REMOTE" "cd ${REMOTE_REPO} && ./.nix/nix-cmake.sh --build ${REMOTE_BUILD} -j ${BUILD_JOBS} --target systest" > /tmp/nes_remote_build.log 2>&1; then
            echo "  ERROR: Build failed. Last 30 lines:"
            tail -30 /tmp/nes_remote_build.log
            exit 1
        fi
        echo "  Build complete."
        echo ""
    else
        echo "Skipping build (--skip-build)."
        echo ""
    fi

    # ---- Step 3: Run benchmarks on remote (via Nix) ----
    REMOTE_BENCH_CMD="bash scripts/benchmarking/run_benchmark_source_modes.sh"
    REMOTE_BENCH_CMD+=" --build-dir ${REMOTE_BUILD}"
    REMOTE_BENCH_CMD+=" --test-file '${TEST_FILE}'"
    REMOTE_BENCH_CMD+=" --iterations ${ITERATIONS}"
    REMOTE_BENCH_CMD+=" --modes '${MODES}'"
    REMOTE_BENCH_CMD+=" --scheduling '${SCHEDULING}'"
    REMOTE_BENCH_CMD+=" --work-stealing '${WORK_STEALING}'"
    REMOTE_BENCH_CMD+=" --tmpfs-path '${TMPFS_PATH}'"
    REMOTE_BENCH_CMD+=" --worker-threads ${WORKER_THREADS}"
    REMOTE_BENCH_CMD+=" --output /tmp/nes_benchmark_results.csv"
    if [[ -n "$REMOTE_DATA" ]]; then
        REMOTE_BENCH_CMD+=" --data '${REMOTE_DATA}'"
    elif [[ -n "$DATA_DIR" ]]; then
        REMOTE_BENCH_CMD+=" --data '${DATA_DIR}'"
    fi

    echo "Running benchmarks on ${REMOTE}..."
    echo ""
    # Clean up stale results file (may be owned by root from previous Docker runs)
    "${SSH_CMD[@]}" "$REMOTE" "rm -f /tmp/nes_benchmark_results.csv 2>/dev/null || { echo '${REMOTE_PASS}' | sudo -S rm -f /tmp/nes_benchmark_results.csv 2>/dev/null; }; true"
    remote_nix "${REMOTE_BENCH_CMD}"

    # ---- Step 4: Fetch results and chart back ----
    echo ""
    echo "Fetching results from ${REMOTE}..."
    "${SCP_CMD[@]}" "${REMOTE}:/tmp/nes_benchmark_results.csv" "$OUTPUT_CSV"

    # Generate summary and chart locally
    echo ""
    python3 "${SCRIPT_DIR}/summarize_benchmark_results.py" "$OUTPUT_CSV"
    python3 "${SCRIPT_DIR}/plot_benchmark.py" "$OUTPUT_CSV" "${OUTPUT_CSV%.csv}_chart.png" 2>/dev/null || true

    # Clean up remote temp file
    "${SSH_CMD[@]}" "$REMOTE" "rm -f /tmp/nes_benchmark_results.csv" 2>/dev/null || true

    exit 0
fi

# ============================================================================
# Local execution mode
# ============================================================================
SYSTEST_DIR="${REPO_ROOT}/nes-systests"
SYSTEST_BIN="${REPO_ROOT}/${BUILD_DIR}/nes-systests/systest/systest"
if [[ ! -x "$SYSTEST_BIN" ]]; then
    ## Fallback: try without nes-systests prefix (some build configs differ)
    SYSTEST_BIN="${REPO_ROOT}/${BUILD_DIR}/systest/systest"
fi

if [[ ! -x "$SYSTEST_BIN" ]]; then
    echo "ERROR: systest binary not found. Searched:"
    echo "       ${REPO_ROOT}/${BUILD_DIR}/nes-systests/systest/systest"
    echo "       ${REPO_ROOT}/${BUILD_DIR}/systest/systest"
    echo "       Build it first: cmake --build ${BUILD_DIR} -j --target systest"
    exit 1
fi

SOURCE_TEST="${SYSTEST_DIR}/${TEST_FILE}"
if [[ ! -f "$SOURCE_TEST" ]]; then
    echo "ERROR: Test file not found: $SOURCE_TEST"
    exit 1
fi

# ---- Setup ----
WORK_BASE=$(mktemp -d "/tmp/nes_benchmark_modes_XXXXXX")
trap 'rm -rf "$WORK_BASE"' EXIT

IFS=',' read -ra MODE_LIST <<< "$MODES"
IFS=',' read -ra SCHED_LIST <<< "$SCHEDULING"
IFS=',' read -ra THREAD_LIST <<< "$WORKER_THREADS"
IFS=',' read -ra WS_LIST <<< "$WORK_STEALING"

echo ""
echo "================================================================="
echo " BenchmarkSource Mode Comparison (local)"
echo " Test file:       ${TEST_FILE}"
echo " Source modes:    ${MODES}"
echo " Scheduling:      ${SCHEDULING}"
echo " Work stealing:   ${WORK_STEALING}"
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
echo "threads,scheduling,work_stealing,mode,iteration,query,time_seconds,bytes_per_second,tuples_per_second" > "$OUTPUT_CSV"

# ---- Run benchmarks ----
for threads in "${THREAD_LIST[@]}"; do
 for sched in "${SCHED_LIST[@]}"; do
  for ws in "${WS_LIST[@]}"; do
   # Work stealing only applies to per-thread strategies
   if [[ "$sched" == "GLOBAL_QUEUE" && "$ws" == "true" ]]; then
       continue
   fi
   for mode in "${MODE_LIST[@]}"; do
    # Check tmpfs availability for tmpfs modes
    if [[ "$mode" == "tmpfs_cold" || "$mode" == "tmpfs_warm" ]]; then
        if [[ ! -d "$TMPFS_PATH" ]]; then
            echo "SKIP: mode=$mode (tmpfs path '$TMPFS_PATH' not found)"
            continue
        fi
    fi

    ws_label=""
    if [[ "$ws" == "true" ]]; then
        ws_label=" +WS"
    fi
    echo "--- Threads: $threads | Scheduling: ${sched}${ws_label} | Source mode: $mode ---"
    test_file=$(generate_test_file "$mode")

    for iter in $(seq 1 "$ITERATIONS"); do
        work_dir="${WORK_BASE}/${threads}t/${sched}_ws${ws}/${mode}/run_${iter}"
        mkdir -p "$work_dir"

        # Build command — pin to one NUMA node when threads fit
        cmd=()
        if command -v numactl &>/dev/null && [[ "$threads" -le 32 ]]; then
            cmd+=(numactl --cpunodebind=0 --membind=0)
        fi
        cmd+=("$SYSTEST_BIN" -b --show-query-performance
            -t "$test_file"
            --workingDir "$work_dir"
        )
        if [[ -n "$DATA_DIR" ]]; then
            cmd+=(--data "$DATA_DIR")
        fi
        cmd+=(--
            "--worker.query_engine.number_of_worker_threads=${threads}"
            "--worker.default_query_execution.execution_mode=COMPILER"
            "--worker.query_engine.scheduling_strategy=${sched}"
            "--worker.query_engine.work_stealing=${ws}"
        )

        printf "  Iteration %d/%d... " "$iter" "$ITERATIONS"
        "${cmd[@]}" > "${work_dir}/stdout.log" 2>&1 || true

        # Parse BenchmarkResults.json (produced even if queries fail checksum validation)
        results_json="${work_dir}/BenchmarkResults.json"
        if [[ -f "$results_json" ]]; then
            python3 - "$results_json" "$threads" "$sched" "$ws" "$mode" "$iter" >> "$OUTPUT_CSV" <<'PARSE_EOF'
import json, sys
with open(sys.argv[1]) as f:
    threads, sched, ws, mode, iteration = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
    for r in json.load(f):
        name = r.get("query name", "?")
        t = r.get("time", 0)
        bps = r.get("bytesPerSecond", 0)
        tps = r.get("tuplesPerSecond", 0)
        print(f"{threads},{sched},{ws},{mode},{iteration},{name},{t},{bps},{tps}")
PARSE_EOF
            echo "done"
        else
            echo "WARNING: no BenchmarkResults.json (see ${work_dir}/stdout.log)"
        fi
    done
    echo ""
   done
  done
 done
done

# ---- Print summary ----
python3 "${SCRIPT_DIR}/summarize_benchmark_results.py" "$OUTPUT_CSV"
