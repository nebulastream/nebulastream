#!/usr/bin/env sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

repo_root="${NES_REPO_ROOT:-$PWD}"
build_dir="${NES_BUILD_DIR:-$repo_root/cmake-build-debug}"
build_type="${NES_BUILD_TYPE:-Debug}"
topology="${NES_TOPOLOGY_FILE:-$repo_root/nes-systests/configs/topologies/two-nodes-localhost.yaml}"
worker_log_dir="${NES_WORKER_LOG_DIR:-$build_dir/nes-systests/remote-worker-logs}"
inline_event_host="${NES_SYSTEST_INLINE_EVENT_HOST:-127.0.0.1}"

if [ ! -f "$repo_root/CMakeLists.txt" ] || [ ! -x "$repo_root/.nix/nix-cmake.sh" ]; then
  echo "systest-remote: run from repository root or set NES_REPO_ROOT" >&2
  exit 1
fi

if [ ! -f "$topology" ]; then
  echo "systest-remote: topology file not found: $topology" >&2
  exit 1
fi

mkdir -p "$worker_log_dir"

rustc_bin="$(command -v rustc)"
cargo_bin="$(command -v cargo)"

"$repo_root/.nix/nix-cmake.sh" \
  -S "$repo_root" \
  -B "$build_dir" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE="$build_type" \
  -DCMAKE_MAKE_PROGRAM="$repo_root/.nix/ninja" \
  -DCMAKE_C_COMPILER="$repo_root/.nix/cc" \
  -DCMAKE_CXX_COMPILER="$repo_root/.nix/c++" \
  -DRust_COMPILER="$rustc_bin" \
  -DRust_CARGO="$cargo_bin"

"$repo_root/.nix/nix-cmake.sh" \
  --build "$build_dir" \
  --target nes-single-node-worker systest -j4

worker_bin="$build_dir/nes-single-node-worker/nes-single-node-worker"
systest_bin="$build_dir/nes-systests/systest/systest"

if [ ! -x "$worker_bin" ]; then
  echo "systest-remote: missing worker binary: $worker_bin" >&2
  exit 1
fi

if [ ! -x "$systest_bin" ]; then
  echo "systest-remote: missing systest binary: $systest_bin" >&2
  exit 1
fi

worker_pids=()
cleanup() {
  for pid in "${worker_pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-20}"
  local deadline=$((SECONDS + timeout_sec))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_worker() {
  local grpc="$1"
  local connection="$2"
  local log_file="$3"
  "$worker_bin" --grpc="$grpc" --connection="$connection" >"$log_file" 2>&1 &
  local pid=$!
  worker_pids+=("$pid")
  echo "systest-remote: started worker pid=$pid grpc=$grpc connection=$connection log=$log_file"
}

start_worker "localhost:8080" "localhost:9090" "$worker_log_dir/worker-8080.log"
start_worker "localhost:8181" "localhost:9191" "$worker_log_dir/worker-8181.log"

if ! wait_for_port "127.0.0.1" "8080" 30; then
  echo "systest-remote: worker on localhost:8080 did not become ready" >&2
  tail -n 100 "$worker_log_dir/worker-8080.log" >&2 || true
  exit 1
fi
if ! wait_for_port "127.0.0.1" "8181" 30; then
  echo "systest-remote: worker on localhost:8181 did not become ready" >&2
  tail -n 100 "$worker_log_dir/worker-8181.log" >&2 || true
  exit 1
fi

echo "systest-remote: workers are ready, running distributed systest"

has_remote=0
has_cluster=0
for arg in "$@"; do
  if [ "$arg" = "--remote" ] || [ "$arg" = "-r" ]; then
    has_remote=1
  fi
  if [ "$arg" = "--clusterConfig" ] || [ "$arg" = "-c" ]; then
    has_cluster=1
  fi
done

cmd=("$systest_bin")
if [ "$has_remote" -eq 0 ]; then
  cmd+=("--remote")
fi
if [ "$has_cluster" -eq 0 ]; then
  cmd+=("--clusterConfig" "$topology")
fi
cmd+=("$@")

NES_SYSTEST_INLINE_EVENT_HOST="$inline_event_host" "${cmd[@]}"
