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

setup_file() {
  # Clean up leaked containers and networks from previous crashed runs
  for net in $(docker network ls --filter label=nes-test=distributed-cli -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter label=nes-test=distributed-cli 2>/dev/null || true
  # Remove all containers (running or stopped) referencing test images
  # from previous runs, so those images can later be deleted
  for img in $(docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q 2>/dev/null); do
    docker ps -aq --filter ancestor="$img" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q | xargs -r docker image rm -f 2>/dev/null || true

  # Validate environment variables
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NES_CLI_TESTDATA" ]; then
    echo "ERROR: NES_CLI_TESTDATA environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    exit 1
  fi

  if [ ! -f "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file does not exist: $NES_CLI" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file is not executable: $NES_CLI" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ -z "$NES_RUNTIME_BASE_IMAGE" ]; then
    echo "ERROR: NES_RUNTIME_BASE_IMAGE environment variable must be set" >&2
    exit 1
  fi

  # Build Docker images with unique tags to avoid collisions when test suites run in parallel
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export WORKER_IMAGE="nes-worker-cli-test-${suffix}"
  local worker_ctx=$(mktemp -d)
  cp $(realpath $NEBULASTREAM) "$worker_ctx/nes-single-node-worker"
  echo "[setup_file] building worker image..." >&2
  docker build --load -t $WORKER_IMAGE -f - "$worker_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  rm -rf "$worker_ctx"
  echo "[setup_file] building CLI image..." >&2
  export CLI_IMAGE="nes-cli-image-${suffix}"
  local cli_ctx=$(mktemp -d)
  cp $(realpath $NES_CLI) "$cli_ctx/nes-cli"
  docker build --load -t $CLI_IMAGE -f - "$cli_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-cli /usr/bin
EOF
  rm -rf "$cli_ctx"

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using CLI_IMAGE: $CLI_IMAGE" >&3
  echo "[setup_file] done" >&2
}

teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $CLI_IMAGE || true
}

setup() {
  # Create temp directory within the mounted workspace (not /tmp)
  # so it's accessible from docker-compose containers running on the host
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  volume=$(docker volume create)
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker cp . $volume_host_container:/data
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "# Using test volume: $TEST_VOLUME" >&3
}

sync_workdir() {
  volume_host_container=$(docker run -d --rm -v $TEST_VOLUME:/data alpine sleep infinite)
  docker cp $volume_host_container:/data/. .
  docker stop -t0 $volume_host_container
}

teardown() {
  sync_workdir || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

function setup_distributed() {
  echo "[setup] composing $1 ..." >&2
  tests/util/create_compose.sh "$1" > docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
  fi
  echo "[setup] containers ready" >&2
  return $exit_code
}

DOCKER_NES_CLI() {
  docker compose exec -T nes-cli nes-cli "$@"
}

query_ids() {
  echo "$1" | jq -r '.[].id'
}

query_state() {
  local id="$1"
  local json="$2"
  echo "$json" | jq -r --argjson id "$id" '.[] | select(.id == $id) | .current_state'
}

fragment_count() {
  local id="$1"
  local json="$2"
  echo "$json" | jq --argjson id "$id" '.[] | select(.id == $id) | .fragments | length'
}

fragment_state() {
  local query_id="$1"
  local host="$2"
  local json="$3"
  echo "$json" | jq -r --argjson id "$query_id" --arg host "$host" \
    '.[] | select(.id == $id) | .fragments[] | select(.host_addr == $host) | .current_state'
}

assert_json_contains() {
  local expected="$1"
  local actual="$2"
  local result
  result=$(echo "$actual" | jq --argjson exp "$expected" 'contains($exp)')
  if [ "$result" != "true" ]; then
    echo "JSON subset check failed"
    echo "Expected (subset): $expected"
    echo "Actual: $actual"
    return 1
  fi
}

@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}

@test "launch multiple queries from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  local count=$(echo "$output" | jq 'length')
  [ "$count" -eq 8 ]

  local all_ids
  all_ids=$(query_ids "$output")

  run DOCKER_NES_CLI stop $(echo "$all_ids" | sed -n '1p')
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI stop $(echo "$all_ids" | sed -n '2p') $(echo "$all_ids" | sed -n '3p') $(echo "$all_ids" | sed -n '4p') $(echo "$all_ids" | sed -n '5p') $(echo "$all_ids" | sed -n '6p')
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI stop $(echo "$all_ids" | sed -n '7p') $(echo "$all_ids" | sed -n '8p')
  [ "$status" -eq 0 ]
}

@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start 'selectaaa DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  sleep 1

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run DOCKER_NES_CLI -s tests/good/distributed-query-deployment.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI status
    [ "$status" -eq 0 ]
    QUERY_ID=$(query_ids "$output" | head -1)
    QUERY_STATE=$(query_state "$QUERY_ID" "$output")
    if [ "$QUERY_STATE" = "Running" ]; then
      break
    fi
  done
  [ "$QUERY_STATE" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
  FRAG_COUNT=$(fragment_count "$QUERY_ID" "$output")
  [ "$FRAG_COUNT" -eq 9 ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/chained-joins-one-fast-source.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  # Poll until at least one fragment completes (the fast source on worker-2 finishes)
  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI status
    [ "$status" -eq 0 ]
    QUERY_ID=$(query_ids "$output" | head -1)
    COMPLETED=$(echo "$output" | jq --argjson id "$QUERY_ID" \
      '[.[] | select(.id == $id) | .fragments[] | select(.current_state == "Completed")] | length')
    if [ "$COMPLETED" -gt 0 ]; then
      break
    fi
  done

  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
  [ "$COMPLETED" -gt 0 ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/chained-joins.yaml

  docker compose stop worker-1

  run DOCKER_NES_CLI -d start
  [ "$status" -eq 1 ]

  sync_workdir
  grep "UNAVAILABLE" nes-cli.log

  docker compose up -d --wait worker-1
  # Now it should work
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]

  docker compose kill worker-1

  # Poll until the query transitions to Failed
  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI status
    [ "$status" -eq 0 ]
    QUERY_STATE=$(query_state "$QUERY_ID" "$output")
    if [ "$QUERY_STATE" = "Failed" ]; then
      break
    fi
  done
  [ "$QUERY_STATE" = "Failed" ]
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  docker compose kill worker-1
  sleep 2
  docker compose up -d --wait worker-1

  # The query should still be marked as failed since the fragment was lost
  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Failed" ]
}

@test "worker status includes fragments" {
  setup_distributed tests/good/select-gen-into-void.yaml

  run DOCKER_NES_CLI -s tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  # Status should include fragments with host_addr
  FRAG_COUNT=$(fragment_count "$QUERY_ID" "$output")
  [ "$FRAG_COUNT" -gt 0 ]

  FRAG_HOST=$(echo "$output" | jq -r --argjson id "$QUERY_ID" \
    '.[] | select(.id == $id) | .fragments[0].host_addr')
  [ -n "$FRAG_HOST" ]
  [ "$FRAG_HOST" != "null" ]
}

@test "launch query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start'"
  [ "$status" -eq 0 ]
}

@test "launch query using 3-nodes topology" {
  setup_distributed tests/good/3-nodes.yaml
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "placement fails with reversed downstream edges" {
  setup_distributed tests/bad/3-nodes-reversed-edges.yaml
  run DOCKER_NES_CLI start
  [ "$status" -eq 1 ]
}

@test "launch and stop query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'nes-cli stop $QUERY_ID'"
  [ "$status" -eq 0 ]
}

@test "query status with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  sleep 1

  run DOCKER_NES_CLI status
  [ "$status" -eq 0 ]
  QUERY_STATE=$(echo "$output" | jq -r '.[0].current_state')
  [ "$QUERY_STATE" = "Running" ]
}

@test "back pressure using worker config" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]

  run DOCKER_NES_CLI status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run DOCKER_NES_CLI stop $QUERY_ID
  # 0 means there is no overwrite and the worker default will be picked.
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "max_pending_acks: 0" worker-2/singleNodeWorker.log
  grep "sender_queue_size: 0" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "back pressure using optimizer flags" {
  setup_distributed tests/good/backpressure-optimizer-flags.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]

  run DOCKER_NES_CLI status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run DOCKER_NES_CLI stop $QUERY_ID
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "max_pending_acks: 25" worker-2/singleNodeWorker.log
  grep "sender_queue_size: 32" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]

  run DOCKER_NES_CLI status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  docker compose stop worker-1

  # Poll until the failure propagates
  for i in $(seq 1 20); do
    sleep 1
    sync_workdir
    if grep -q "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  grep "Backpressure" worker-2/singleNodeWorker.log
  grep "NetworkSink was closed by other side" worker-2/singleNodeWorker.log
  grep "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log

  run DOCKER_NES_CLI status
  [ $status -eq 0 ]
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Failed" ]
}

@test "order of worker termination when backpressure is applied. terminate source" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]

  run DOCKER_NES_CLI status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done
  grep "Backpressure" worker-2/singleNodeWorker.log

  docker compose stop worker-2
  sleep 2

  # worker-1 (sink) should still be running, but overall query should reflect the failure
  run DOCKER_NES_CLI status
  [ $status -eq 0 ]
  WORKER1_STATE=$(fragment_state "$QUERY_ID" "worker-1:8080" "$output")
  [ "$WORKER1_STATE" = "Running" ]
}
