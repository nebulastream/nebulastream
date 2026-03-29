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
  # Clean up leaked containers and networks from previous crashed runs (pipe-specific label)
  for net in $(docker network ls --filter label=nes-test=pipe-plugin -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter label=nes-test=pipe-plugin 2>/dev/null || true
  for img in $(docker images --filter reference='nes-worker-pipe-test-*' --filter reference='nes-cli-pipe-test-*' -q 2>/dev/null); do
    docker ps -aq --filter ancestor="$img" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker images --filter reference='nes-worker-pipe-test-*' --filter reference='nes-cli-pipe-test-*' -q | xargs -r docker image rm -f 2>/dev/null || true

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
  export WORKER_IMAGE="nes-worker-pipe-test-${suffix}"
  local worker_ctx=$(mktemp -d)
  cp $(realpath $NEBULASTREAM) "$worker_ctx/nes-single-node-worker"
  docker build --load -t $WORKER_IMAGE -f - "$worker_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  rm -rf "$worker_ctx"
  export CLI_IMAGE="nes-cli-pipe-test-${suffix}"
  local cli_ctx=$(mktemp -d)
  cp $(realpath $NES_CLI) "$cli_ctx/nes-cli"
  docker build --load -t $CLI_IMAGE -f - "$cli_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-cli /usr/bin
EOF
  rm -rf "$cli_ctx"

  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using CLI_IMAGE: $CLI_IMAGE" >&3
}

teardown_file() {
  echo "# Pipe test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $CLI_IMAGE || true
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR/tests"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  volume=$(docker volume create)
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker cp . $volume_host_container:/data
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "# Using test volume: $TEST_VOLUME" >&3
}

# Copies the worker's working directory from the docker volume back to the host.
# Needed to inspect output files (CSV, logs) written by queries inside the container.
sync_workdir() {
  volume_host_container=$(docker run -d --rm -v $TEST_VOLUME:/data alpine sleep infinite)
  docker cp $volume_host_container:/data/. .
  docker stop -t0 $volume_host_container
}

teardown() {
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

# Generate a docker-compose.yaml from a topology file and start the services.
function setup_distributed() {
  cat > docker-compose.yaml <<COMPOSE
services:
  nes-cli:
    image: $CLI_IMAGE
    pull_policy: never
    environment:
      NES_WORKER_GRPC_ADDR: worker-node:8080
      XDG_STATE_HOME: $(pwd)/.xdg-state
    stop_grace_period: 0s
    working_dir: /workdir
    command: ["sleep", "infinity"]
    volumes:
      - $TEST_VOLUME:/workdir
  worker-node:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: /workdir/worker-node
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=worker-node:8080", "-connect-timeout", "5s"]
      interval: 1s
      timeout: 5s
      retries: 3
      start_period: 0s
    command: [
      "--grpc=worker-node:8080",
      "--worker.default_query_execution.execution_mode=INTERPRETER",
    ]
    volumes:
      - $TEST_VOLUME:/workdir
networks:
  default:
    labels:
      nes-test: pipe-plugin
volumes:
  $TEST_VOLUME:
    external: true
COMPOSE
  docker compose up -d --wait
}

DOCKER_NES_CLI() {
  tail -f /dev/null | docker compose exec -T nes-cli nes-cli "$@"
}

assert_json_contains() {
  local expected="$1"
  local actual="$2"

  local result=$(echo "$actual" | jq --argjson exp "$expected" 'contains($exp)')

  if [ "$result" != "true" ]; then
    echo "JSON subset check failed"
    echo "Expected (subset): $expected"
    echo "Actual: $actual"
    return 1
  fi
}

# Poll query status until it matches one of the expected states or timeout (default 30s).
# Usage: wait_for_status <topo> <query_id> <timeout_secs> <status1> [status2] ...
wait_for_status() {
  local topo="$1"; shift
  local qid="$1"; shift
  local timeout="$1"; shift
  local expected_states=("$@")

  for attempt in $(seq 1 "$timeout"); do
    run DOCKER_NES_CLI -t "$topo" status "$qid"
    if [ "$status" -eq 0 ]; then
      local qs
      qs=$(echo "$output" | jq -r '.[0].query_status')
      for expected in "${expected_states[@]}"; do
        if [ "$qs" = "$expected" ]; then
          return 0
        fi
      done
    fi
    sleep 1
  done
  echo "Timeout after ${timeout}s waiting for query $qid to reach [${expected_states[*]}], last status: $qs" >&3
  return 1
}

# ── Pipe sink/source tests ──────────────────────────────────────────────────

@test "pipe: generator feeds single pipe source" {
  setup_distributed
  TOPO="tests/good/pipe-single-consumer.yaml"

  run DOCKER_NES_CLI -t "$TOPO" start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 2 ]
  PRODUCER_ID="${lines[0]}"
  CONSUMER_ID="${lines[1]}"

  wait_for_status "$TOPO" "$PRODUCER_ID" 10 "Running"
  wait_for_status "$TOPO" "$CONSUMER_ID" 10 "Running"

  run DOCKER_NES_CLI -t "$TOPO" stop "$PRODUCER_ID" "$CONSUMER_ID"
  [ "$status" -eq 0 ]
}

@test "pipe: generator feeds 3 pipe sources" {
  setup_distributed
  TOPO="tests/good/pipe-three-consumers.yaml"

  run DOCKER_NES_CLI -t "$TOPO" start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 4 ]
  query_ids=("${lines[@]}")

  for qid in "${query_ids[@]}"; do
    wait_for_status "$TOPO" "$qid" 10 "Running"
  done

  run DOCKER_NES_CLI -t "$TOPO" stop "${query_ids[@]}"
  [ "$status" -eq 0 ]
}

@test "pipe: stopping producer propagates to consumers" {
  setup_distributed
  TOPO="tests/good/pipe-three-consumers.yaml"

  run DOCKER_NES_CLI -t "$TOPO" start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 4 ]
  PRODUCER_ID="${lines[0]}"
  CONSUMER_1="${lines[1]}"
  CONSUMER_2="${lines[2]}"
  CONSUMER_3="${lines[3]}"

  wait_for_status "$TOPO" "$PRODUCER_ID" 10 "Running"

  run DOCKER_NES_CLI -t "$TOPO" stop "$PRODUCER_ID"
  [ "$status" -eq 0 ]

  # Consumers should stop via EoS propagation
  for cid in "$CONSUMER_1" "$CONSUMER_2" "$CONSUMER_3"; do
    wait_for_status "$TOPO" "$cid" 15 "Stopped" "Finished"
  done
}

@test "pipe: multiple independent pipes running concurrently" {
  setup_distributed
  TOPO="tests/good/pipe-concurrent.yaml"

  run DOCKER_NES_CLI -t "$TOPO" start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 6 ]
  PRODUCER_A="${lines[0]}"
  CONSUMER_A="${lines[1]}"
  PRODUCER_B="${lines[2]}"
  CONSUMER_B="${lines[3]}"
  PRODUCER_C="${lines[4]}"
  CONSUMER_C="${lines[5]}"

  for qid in "$PRODUCER_A" "$CONSUMER_A" "$PRODUCER_B" "$CONSUMER_B" "$PRODUCER_C" "$CONSUMER_C"; do
    wait_for_status "$TOPO" "$qid" 15 "Running"
  done

  # Stop pipe A producer — only pipe A consumer should stop
  run DOCKER_NES_CLI -t "$TOPO" stop "$PRODUCER_A"
  [ "$status" -eq 0 ]

  wait_for_status "$TOPO" "$CONSUMER_A" 15 "Stopped" "Finished"

  # Pipe B and C should still be running
  run DOCKER_NES_CLI -t "$TOPO" status "$PRODUCER_B"
  assert_json_contains '[{"query_status":"Running"}]' "$output"
  run DOCKER_NES_CLI -t "$TOPO" status "$CONSUMER_B"
  assert_json_contains '[{"query_status":"Running"}]' "$output"

  run DOCKER_NES_CLI -t "$TOPO" stop "$PRODUCER_B" "$CONSUMER_B" "$PRODUCER_C" "$CONSUMER_C"
}

@test "pipe: consumer joins while producer is already running" {
  setup_distributed

  run DOCKER_NES_CLI -t tests/good/pipe-topology.yaml start \
    'SELECT * FROM GEN_SOURCE INTO PIPE_SINK'
  [ "$status" -eq 0 ]
  PRODUCER_ID=$output

  wait_for_status tests/good/pipe-topology.yaml "$PRODUCER_ID" 10 "Running"

  # Attach a consumer while producer is already running
  run DOCKER_NES_CLI -t tests/good/pipe-topology.yaml start \
    'SELECT * FROM PIPE_READER INTO VOID_SINK'
  [ "$status" -eq 0 ]
  CONSUMER_ID=$output

  wait_for_status tests/good/pipe-topology.yaml "$CONSUMER_ID" 10 "Running"

  run DOCKER_NES_CLI -t tests/good/pipe-topology.yaml stop "$PRODUCER_ID" "$CONSUMER_ID"
  [ "$status" -eq 0 ]
}

@test "pipe: aggressive chunking with window aggregation" {
  setup_distributed
  TOPO="tests/good/pipe-chunking-topology.yaml"

  PROJ="SELECT val, val AS v02, val AS v03, val AS v04, val AS v05, val AS v06, val AS v07, val AS v08, val AS v09, val AS v10, val AS v11, val AS v12, val AS v13, val AS v14, val AS v15, val AS v16, val AS v17, val AS v18, val AS v19, val AS v20, val AS v21, val AS v22, val AS v23, val AS v24, val AS v25, val AS v26, val AS v27, val AS v28, val AS v29, val AS v30, val AS v31, val AS v32, val AS v33, val AS v34, val AS v35, val AS v36, val AS v37, val AS v38, val AS v39, val AS v40, ts FROM GEN INTO PIPE_SINK"

  run DOCKER_NES_CLI -t "$TOPO" start "$PROJ"
  [ "$status" -eq 0 ]
  PRODUCER_ID=$output

  wait_for_status "$TOPO" "$PRODUCER_ID" 10 "Running"

  PASSTHROUGH="SELECT val, v02, v03, v04, v05, v06, v07, v08, v09, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, v29, v30, v31, v32, v33, v34, v35, v36, v37, v38, v39, v40, ts FROM READER INTO PASSTHROUGH_SINK"
  run DOCKER_NES_CLI -t "$TOPO" start "$PASSTHROUGH"
  [ "$status" -eq 0 ]
  CONSUMER_1=$output

  run DOCKER_NES_CLI -t "$TOPO" start \
    'SELECT COUNT(val) AS val_count FROM READER WINDOW TUMBLING(ts, SIZE 2 SEC) INTO WINDOW_SINK'
  [ "$status" -eq 0 ]
  CONSUMER_2=$output

  wait_for_status "$TOPO" "$CONSUMER_1" 15 "Running"
  wait_for_status "$TOPO" "$CONSUMER_2" 15 "Running"

  # Stop producer — EoS must propagate through chunked buffers to all consumers
  run DOCKER_NES_CLI -t "$TOPO" stop "$PRODUCER_ID"
  [ "$status" -eq 0 ]

  for qid in "$CONSUMER_1" "$CONSUMER_2"; do
    wait_for_status "$TOPO" "$qid" 30 "Stopped" "Finished"
  done

  # Verify window output file was produced
  sync_workdir
  [ -f worker-node/window_output.csv ]
  local line_count
  line_count=$(wc -l < worker-node/window_output.csv)
  [ "$line_count" -gt 1 ]  # at least header + 1 row
}

@test "pipe: data integrity — staggered consumers, no gaps via Gauss" {
  setup_distributed
  TOPO="tests/good/pipe-integrity-topology.yaml"

  verify_no_gaps() {
    local file=$1
    local label=$2
    [ -f "$file" ] || { echo "# $label: FILE NOT FOUND: $file" >&3; return 1; }

    local count min max sum
    count=$(tail -n +2 "$file" | wc -l)
    if [ "$count" -eq 0 ]; then
      echo "# $label: EMPTY FILE" >&3
      return 1
    fi
    min=$(tail -n +2 "$file" | sort -n | head -1)
    max=$(tail -n +2 "$file" | sort -n | tail -1)
    sum=$(tail -n +2 "$file" | awk '{s+=$1} END {print s}')

    local expected_count=$((max - min + 1))
    local expected_sum=$(( count * (min + max) / 2 ))

    echo "# $label: count=$count min=$min max=$max sum=$sum expected_count=$expected_count expected_sum=$expected_sum" >&3

    [ "$count" -eq "$expected_count" ] || { echo "# $label: GAP DETECTED! count=$count but range=$expected_count" >&3; return 1; }
    [ "$sum" -eq "$expected_sum" ] || { echo "# $label: SUM MISMATCH! sum=$sum expected=$expected_sum" >&3; return 1; }
    return 0
  }

  run DOCKER_NES_CLI -t "$TOPO" start 'SELECT id FROM GEN INTO PIPE_SINK'
  [ "$status" -eq 0 ]
  PRODUCER=$output

  wait_for_status "$TOPO" "$PRODUCER" 10 "Running"

  run DOCKER_NES_CLI -t "$TOPO" start 'SELECT id FROM READER INTO FILE_SINK_1'
  [ "$status" -eq 0 ]
  C1=$output

  sleep 1
  run DOCKER_NES_CLI -t "$TOPO" start 'SELECT id FROM READER INTO FILE_SINK_2'
  [ "$status" -eq 0 ]
  C2=$output

  sleep 1
  run DOCKER_NES_CLI -t "$TOPO" start 'SELECT id FROM READER INTO FILE_SINK_3'
  [ "$status" -eq 0 ]
  C3=$output

  # Wait for all queries to finish (generator is finite)
  for qid in "$PRODUCER" "$C1" "$C2" "$C3"; do
    wait_for_status "$TOPO" "$qid" 60 "Stopped" "Finished"
  done

  sync_workdir

  verify_no_gaps worker-node/consumer_1.csv "Consumer1"
  verify_no_gaps worker-node/consumer_2.csv "Consumer2"
  verify_no_gaps worker-node/consumer_3.csv "Consumer3"

  C2_COUNT=$(tail -n +2 worker-node/consumer_2.csv | wc -l)
  C3_COUNT=$(tail -n +2 worker-node/consumer_3.csv | wc -l)
  echo "# Counts: C2=$C2_COUNT C3=$C3_COUNT" >&3
  [ "$C2_COUNT" -ge "$C3_COUNT" ]
}
