#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source "$NES_BATS_LIB"

setup_file() {
  nes_cleanup_leaked_resources systest-remote

  nes_require_env NES_SYSTEST
  nes_require_env NES_WORKER
  nes_require_env NES_DIR
  nes_require_env NES_TEST_TMP_DIR
  nes_require_env DATADIR
  nes_require_env NES_RUNTIME_BASE_IMAGE
  nes_require_executable "$NES_SYSTEST"
  nes_require_executable "$NES_WORKER"

  nes_build_runtime_image WORKER_IMAGE nes-worker-systest \
    "$NES_WORKER" nes-single-node-worker
  nes_build_app_image SYSTEST_IMAGE nes-systest-image \
    "$NES_SYSTEST" systest

  export CONTAINER_WORKDIR="/$(cat /proc/sys/kernel/random/uuid)"
  export TESTDATA_DIR=$(realpath "$DATADIR")
  export TESTCONFIG_DIR=$(realpath "$NES_DIR")

  echo "# Using NES_DIR: $NES_DIR" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using SYSTEST_IMAGE: $SYSTEST_IMAGE" >&3
  echo "# Using TESTDATA_DIR: $TESTDATA_DIR" >&3
  echo "# Using TESTCONFIG_DIR: $TESTCONFIG_DIR" >&3
  echo "# Using CONTAINER_WORKDIR: $CONTAINER_WORKDIR" >&3
}

teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $SYSTEST_IMAGE || true
}

INSTANCE_PID=0
setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")

  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3

  export TEST_DIR="$TMP_DIR"
}

teardown() {
  docker compose down -v || true
}

function setup_distributed() {
  # Extract per-worker configs from the topology YAML.
  local topology="$1"
  local worker_count=$(yq '.workers | length' "$topology")
  local config_dir=$(mktemp -d)
  local required_bytes=0

  for i in $(seq 0 $((worker_count - 1))); do
    local has_config=$(yq ".workers[$i] | has(\"config\")" "$topology")
    if [ "$has_config" = "true" ]; then
      local host=$(yq -r ".workers[$i].host" "$topology" | cut -d':' -f1)
      yq ".workers[$i].config" "$topology" > "$config_dir/$host.yaml"
      # Each worker container reserves its declared total_memory_in_bytes on this single host, so accumulate the
      # per-worker budgets (topologies that declare none contribute nothing) to check their sum against host RAM below.
      if grep -q 'total_memory_in_bytes' "$config_dir/$host.yaml"; then
        local budget=$(yq -r '.worker.total_memory_in_bytes' "$config_dir/$host.yaml")
        required_bytes=$((required_bytes + budget))
      fi
    fi
  done

  # Every worker runs as a container on this single host, so the host must back the sum of all per-worker budgets
  # declared in the topology YAML. Fail before starting the compose stack rather than letting a worker OOM mid-run.
  if [ "$required_bytes" -gt 0 ]; then
    local host_mem_bytes=$(awk '/^MemTotal:/ {printf "%d", $2 * 1024}' /proc/meminfo)
    if [ "$host_mem_bytes" -lt "$required_bytes" ]; then
      echo "# systest-remote-test needs ${required_bytes} bytes total across workers but host has only ${host_mem_bytes} bytes" >&3
      rm -rf "$config_dir"
      return 1
    fi
  fi

  # Store generated configs directly in the bind-mounted test directory.
  if [ -n "$(ls -A "$config_dir")" ]; then
    mkdir -p "$TEST_DIR/configs"
    cp "$config_dir"/* "$TEST_DIR/configs/"
  fi
  rm -rf "$config_dir"

  $NES_DIR/nes-systests/systest/remote-test/create_compose.sh "$1" >docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
  fi
  return $exit_code
}

docker_systest() {
  docker compose exec systest systest --log-path $CONTAINER_WORKDIR/systest.log --data /data  --workingDir $CONTAINER_WORKDIR/systest-workdir "$@" >&3
}

# Inference systests load ONNX models via the IREE toolchain. When the IREE tools are not
# available in the worker image (ENABLE_IREE_TESTS=OFF), exclude the Inference group too.
EXTRA_EXCLUDE_GROUPS=()
if [ "$ENABLE_IREE_TESTS" != "ON" ]; then
  EXTRA_EXCLUDE_GROUPS+=(Inference)
fi

@test "two node systest" {
  setup_distributed $NES_DIR/nes-systests/configs/topologies/two-node-with-interpreter.yaml
  run docker_systest -e large tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/two-node-with-interpreter.yaml --remote
  [ "$status" -eq 0 ]
}

@test "8 node systest" {
  setup_distributed $NES_DIR/nes-systests/configs/topologies/8-node.yaml
  run docker_systest -e large tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/8-node.yaml --remote
  [ "$status" -eq 0 ]
}

@test "large scale tests on two nodes" {
  if [ "$ENABLE_DOCKER_LARGE_TESTS" != "ON" ]; then
    skip "Docker-based large tests disabled"
  fi
  setup_distributed $NES_DIR/nes-systests/configs/topologies/two-node-more-capacity.yaml
  run docker_systest -g large -e tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/two-node.yaml --remote
  [ "$status" -eq 0 ]
}
