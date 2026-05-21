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

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
NES_DEV_IMAGE="${NES_DEV_IMAGE:-nebulastream/nes-development:local-odbc}"
RUNTIME_BASE_IMAGE="${NES_RUNTIME_BASE_IMAGE:-nes-runtime-base:test}"
STOP_TIMEOUT_SECONDS="${STOP_TIMEOUT_SECONDS:-20}"
KEEP_REPRO_DIR="${KEEP_REPRO_DIR:-0}"
NES_BUILD_HELPER="${NES_BUILD_HELPER:-$REPO_ROOT/.claude/skills/nes-build/in-docker.sh}"

find_build_dir() {
  if [ -n "${BUILD_DIR:-}" ]; then
    echo "$BUILD_DIR"
    return
  fi

  if [ -x "$NES_BUILD_HELPER" ]; then
    "$NES_BUILD_HELPER" --paths \
      | sed -n 's/^host build dir:[[:space:]]*//p' \
      | awk '{ print $1 }'
    return
  fi

  for candidate in "$REPO_ROOT/cmake-build-debug" "$REPO_ROOT/build"; do
    if [ -d "$candidate" ]; then
      echo "$candidate"
      return
    fi
  done

  echo "Could not determine build directory." >&2
  echo "Set BUILD_DIR, or set NES_CLI and NEBULASTREAM to prebuilt binaries." >&2
  exit 1
}

find_binary() {
  local build_dir="$1"
  local binary_name="$2"
  local default_path="$3"

  if [ -x "$default_path" ]; then
    echo "$default_path"
    return
  fi

  while IFS= read -r candidate; do
    if [ -x "$candidate" ]; then
      echo "$candidate"
      return
    fi
  done < <(find "$build_dir" -type f -name "$binary_name" -print 2>/dev/null)

  echo "Could not find executable $binary_name below $build_dir" >&2
  exit 1
}

run_with_timeout() {
  local seconds="$1"
  local output_file="$2"
  shift 2

  set +e
  "$@" >"$output_file" 2>&1 &
  local command_pid=$!
  (
    sleep "$seconds"
    kill -TERM "$command_pid" 2>/dev/null
    sleep 2
    kill -KILL "$command_pid" 2>/dev/null
  ) &
  local watchdog_pid=$!

  wait "$command_pid"
  local status=$?
  kill "$watchdog_pid" 2>/dev/null
  wait "$watchdog_pid" 2>/dev/null
  set -e

  if [ "$status" -eq 143 ] || [ "$status" -eq 137 ]; then
    return 124
  fi
  return "$status"
}

build_image_from_binary() {
  local image="$1"
  local binary="$2"
  local binary_name="$3"
  local entrypoint="$4"
  local context_dir
  context_dir="$(mktemp -d)"
  cp "$binary" "$context_dir/$binary_name"
  docker build --load -t "$image" -f - "$context_dir" <<EOF
FROM $RUNTIME_BASE_IMAGE
COPY $binary_name /usr/bin/$binary_name
ENTRYPOINT ["$entrypoint"]
EOF
  rm -rf "$context_dir"
}

if [ -n "${NES_CLI:-}" ] && [ -n "${NEBULASTREAM:-}" ]; then
  if [ ! -x "$NES_CLI" ]; then
    echo "NES_CLI is not executable: $NES_CLI" >&2
    exit 1
  fi
  if [ ! -x "$NEBULASTREAM" ]; then
    echo "NEBULASTREAM is not executable: $NEBULASTREAM" >&2
    exit 1
  fi
else
  BUILD_DIR="$(find_build_dir)"
  if [ -x "$NES_BUILD_HELPER" ]; then
    echo "Building nes-cli and nes-single-node-worker in the dev container..."
    NES_DEV_IMAGE="$NES_DEV_IMAGE" "$NES_BUILD_HELPER" \
      cmake --build "{build}" --target nes-cli nes-single-node-worker -j
  else
    echo "Using existing binaries from $BUILD_DIR."
    echo "Set NES_BUILD_HELPER to build them automatically through the dev container."
  fi
  NES_CLI="${NES_CLI:-$(find_binary "$BUILD_DIR" nes-cli "$BUILD_DIR/nes-frontend/apps/nes-cli")}"
  NEBULASTREAM="${NEBULASTREAM:-$(find_binary "$BUILD_DIR" nes-single-node-worker "$BUILD_DIR/nes-single-node-worker/nes-single-node-worker")}"
fi

if ! docker image inspect "$RUNTIME_BASE_IMAGE" >/dev/null 2>&1; then
  echo "Building runtime base image $RUNTIME_BASE_IMAGE..."
  docker build --load -t "$RUNTIME_BASE_IMAGE" \
    -f "$REPO_ROOT/docker/runtime/RuntimeBase.dockerfile" \
    "$REPO_ROOT/docker/runtime"
fi

suffix="$(date +%s)-$$"
project="nes-file-sink-repro-$suffix"
worker_image="nes-file-sink-repro-worker-$suffix"
cli_image="nes-file-sink-repro-cli-$suffix"
work_dir="$(mktemp -d -t nes-file-sink-repro.XXXXXX)"
compose_file="$work_dir/docker-compose.yaml"
topology_file="$work_dir/topology.yaml"
start_log="$work_dir/start.log"
status_log="$work_dir/status.log"
stop_log="$work_dir/stop.log"

cleanup() {
  docker compose -p "$project" -f "$compose_file" down -v >/dev/null 2>&1 || true
  docker rmi "$worker_image" "$cli_image" >/dev/null 2>&1 || true
  if [ "$KEEP_REPRO_DIR" != "1" ]; then
    rm -rf "$work_dir"
  else
    echo "Keeping reproducer directory: $work_dir"
  fi
}
trap cleanup EXIT

mkdir -p "$work_dir/source-node" "$work_dir/sink-node"

echo "Building temporary runtime images..."
build_image_from_binary "$worker_image" "$NEBULASTREAM" nes-single-node-worker nes-single-node-worker
build_image_from_binary "$cli_image" "$NES_CLI" nes-cli nes-cli

cat >"$topology_file" <<'EOF'
query: |
  SELECT
    *
  FROM
    GENERATOR_SOURCE
  INTO
    BAD_FILE_SINK

sinks:
  - name: BAD_FILE_SINK
    host: sink-node:8080
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: File
    config:
      file_path: /proc/nebulastream/forbidden/out.csv
      output_format: CSV
      append: false
    parser_config:
      type: CSV

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: source-node:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 1000
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: source-node:8080
    data_address: source-node:9090
    max_operators: 10000
    downstream: [ sink-node:8080 ]
  - host: sink-node:8080
    data_address: sink-node:9090
    max_operators: 10000
EOF

cat >"$compose_file" <<EOF
services:
  nes-cli:
    image: $cli_image
    pull_policy: never
    environment:
      XDG_STATE_HOME: /workdir/.xdg-state
    working_dir: /workdir
    command: ["sleep", "infinity"]
    volumes:
      - "$work_dir:/workdir"

  source-node:
    image: $worker_image
    pull_policy: never
    working_dir: /workdir/source-node
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=source-node:8080", "-connect-timeout", "5s"]
      interval: 1s
      timeout: 5s
      retries: 10
      start_period: 60s
    command: [
      "--grpc=source-node:8080",
      "--data_address=source-node:9090",
      "--worker.default_query_execution.execution_mode=INTERPRETER",
      "--worker.query_engine.number_of_worker_threads=1",
    ]
    volumes:
      - "$work_dir:/workdir"

  sink-node:
    image: $worker_image
    pull_policy: never
    working_dir: /workdir/sink-node
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=sink-node:8080", "-connect-timeout", "5s"]
      interval: 1s
      timeout: 5s
      retries: 10
      start_period: 60s
    command: [
      "--grpc=sink-node:8080",
      "--data_address=sink-node:9090",
      "--worker.default_query_execution.execution_mode=INTERPRETER",
      "--worker.query_engine.number_of_worker_threads=1",
    ]
    volumes:
      - "$work_dir:/workdir"
EOF

echo "Starting two-worker CLI topology..."
docker compose -p "$project" -f "$compose_file" up -d --wait

echo "Submitting query with downstream File sink path /proc/nebulastream/forbidden/out.csv..."
docker compose -p "$project" -f "$compose_file" exec -T nes-cli \
  nes-cli -t /workdir/topology.yaml start >"$start_log" 2>&1
query_id="$(tail -n 1 "$start_log")"
echo "Started distributed query: $query_id"

echo "Requesting status. The sink-side local query should report failure."
docker compose -p "$project" -f "$compose_file" exec -T nes-cli \
  nes-cli -t /workdir/topology.yaml status "$query_id" >"$status_log" 2>&1 || true
cat "$status_log"

echo "Stopping query. On affected main revisions this hangs; the script times out after ${STOP_TIMEOUT_SECONDS}s."
if run_with_timeout "$STOP_TIMEOUT_SECONDS" "$stop_log" \
  docker compose -p "$project" -f "$compose_file" exec -T nes-cli nes-cli -t /workdir/topology.yaml stop "$query_id"; then
  echo "Stop returned without timing out. Output:"
  cat "$stop_log"
  exit 0
fi

status=$?
cat "$stop_log"
if [ "$status" -eq 124 ]; then
  echo "Reproduced: stop timed out after ${STOP_TIMEOUT_SECONDS}s."
  echo "Re-run with KEEP_REPRO_DIR=1 to keep logs and generated files."
  exit 124
fi

echo "Stop failed with exit status $status."
exit "$status"
