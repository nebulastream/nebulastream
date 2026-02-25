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

  # Validate environment variables
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    echo "Usage: NES_CLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NES_CLI_TESTDATA" ]; then
    echo "ERROR: NES_CLI_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_CLI_TESTDATA=/path/to/cli/testdata" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    echo "Usage: NES_TEST_TMP_DIR=/path/to/build/test-tmp" >&2
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

  # Build Docker images with unique tags to avoid collisions when test suites run in parallel
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export WORKER_IMAGE="nes-worker-cli-test-${suffix}"
  # Use minimal build context with only the required binary to avoid sending
  # the entire build directory to the Docker daemon on each build.
  local worker_ctx=$(mktemp -d)
  cp $(realpath $NEBULASTREAM) "$worker_ctx/nes-single-node-worker"
  docker build --load -t $WORKER_IMAGE -f - "$worker_ctx" <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-\$(dpkg --print-architecture) && \
    chmod +x /bin/grpc_health_probe

    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  rm -rf "$worker_ctx"
  export CLI_IMAGE="nes-cli-image-${suffix}"
  local cli_ctx=$(mktemp -d)
  cp $(realpath $NES_CLI) "$cli_ctx/nes-cli"
  docker build --load -t $CLI_IMAGE -f - "$cli_ctx" <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    COPY nes-cli /usr/bin
EOF
  rm -rf "$cli_ctx"

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using CLI_IMAGE: $CLI_IMAGE" >&3
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
  tests/util/create_compose.sh "$1" > docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_NES_CLI() {
  # docker compose exec v2 disconnects the session when its stdin reaches EOF
  # (docker/compose#10418). In bats subshells stdin is closed, so we pipe from
  # tail to keep the connection alive.
  tail -f /dev/null | docker compose exec -T nes-cli nes-cli "$@"
}

# Like DOCKER_NES_CLI but runs an arbitrary bash command inside the container.
# Use this when you need to set up pipes inside the container (e.g., cat file | nes-cli -t -).
DOCKER_BASH() {
  tail -f /dev/null | docker compose exec -T nes-cli bash -c "$@"
}

assert_json_equal() {
  local expected="$1"
  local actual="$2"

  diff <(echo "$expected" | jq --sort-keys .) \
    <(echo "$actual" | jq --sort-keys .)
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

@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}

@test "launch multiple query from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  query_ids=("${lines[@]}")

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[0]}"
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[1]}" "${query_ids[2]}" "${query_ids[3]}" "${query_ids[4]}" "${query_ids[5]}"
  [ "$status" -eq 0 ]

  run DOCKER_NES_CLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[6]}" "${query_ids[7]}"
  [ "$status" -eq 0 ]
}

@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'selectaaa DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run DOCKER_NES_CLI -t tests/good/distributed-query-deployment.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/distributed-query-deployment.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]
  echo "${output}" | jq -e '(. | length) == 3' # 1 global + 2 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/chained-joins-one-fast-source.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+$ ]]
  QUERY_ID=$output

  # Poll until the fast source has stopped and the query becomes PartiallyStopped
  for i in $(seq 1 20); do
    sleep 1
    run DOCKER_NES_CLI status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "PartiallyStopped" ]; then
      break
    fi
  done
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  [ "$QUERY_STATUS" = "PartiallyStopped" ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/chained-joins.yaml

  docker compose stop worker-1

  run DOCKER_NES_CLI -d start

  sync_workdir
  grep "(5001) : query registration call failed; Status: UNAVAILABLE" nes-cli.log
  [ "$status" -eq 1 ]

  docker compose up -d --wait worker-1
  # now it should work
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # This has to be kill not stop. Stop will gracefully shutdown the worker and all queries on that worker.
  # This would cause the query to fail as it was unexpectedly stopped. If we kill the worker: upstream and downstream
  # will wait for the "crashed" worker to return. However this test does not test that as it is currently not possible.
  docker compose kill worker-1
  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]

  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-2:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-3:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-8:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-7:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-4:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-9:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-5:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-6:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # Simulate a crash by killing worker-1.
  docker compose kill worker-1
  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]

  sleep 1

  docker compose up -d --wait worker-1

# While this might not be the most intuitive nor the long-term solution this testcase documents the current behavior.
# The query running on worker-1 is terminated and on restart it is not restarted, this will cause subsequent status
# request to find that the previous local query id is not registered on worker-1, currently this is falsely reported as a ConnectionError.

  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]
  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"

  echo $output
}

@test "worker status" {
  setup_distributed tests/good/select-gen-into-void.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  sleep 1

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]
  assert_json_contains "[{\"query_id\":\"$query_id\", \"query_status\":\"Running\", \"running\": {}, \"started\": {}}]" "$output"

  local_query_id=$(echo "$output" | jq -r '.[1].local_query_id')
  run DOCKER_NES_CLI status
  [ $status -eq 0 ]

  # Expect to find the local query in the worker status
  assert_json_contains "[{\"local_query_id\":\"$local_query_id\", \"query_status\":\"Running\", \"started\": {}}]" "$output"
}

@test "back pressure" {
  setup_distributed tests/good/backpressure.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run DOCKER_NES_CLI stop $query_id
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  docker compose stop worker-1

  # Poll until the sink closure propagates
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

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Failed"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "Failed",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "order of worker termination when backpressure is applied. terminate source" {
  setup_distributed tests/good/backpressure.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

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

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Unreachable"
    },
    {
      "query_status": "Running",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "launch query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_BASH 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start'
  [ "$status" -eq 0 ]
}

@test "launch and stop query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_BASH 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start "select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK"'
  [ "$status" -eq 0 ]

  QUERY_ID=$output

  sleep 1

  run DOCKER_BASH "cat tests/good/select-gen-into-void.yaml | nes-cli -t - stop $QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch named query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name my-test-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be exactly the user-chosen name
  [ "$output" = "my-test-query" ]
}

@test "launch and stop named query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name my-stop-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "my-stop-query" ]

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop my-stop-query
  [ "$status" -eq 0 ]
}

@test "launch and monitor named query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name my-monitor-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "my-monitor-query" ]

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml status my-monitor-query
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r '.[] | select(.query_id == "my-monitor-query" and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch named query from topology yaml" {
  setup_distributed tests/good/named-query.yaml
  run DOCKER_NES_CLI -t tests/good/named-query.yaml start
  [ "$status" -eq 0 ]

  # Output should be exactly the name from the YAML
  [ "$output" = "my-named-query" ]
}

@test "named query collision fails" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name collision-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "collision-query" ]

  # Starting again with the same name should fail
  run DOCKER_NES_CLI -d -t tests/good/select-gen-into-void.yaml start --name collision-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "--name with multiple queries fails" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -d -t tests/good/select-gen-into-void.yaml start --name bad-name 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK' 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]

  sync_workdir
  grep "only be used with a single query" nes-cli.log
}

@test "stop removes state file and allows name reuse" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name reuse-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "reuse-query" ]

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop reuse-query
  [ "$status" -eq 0 ]

  # Starting again with the same name should succeed after stopping
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name reuse-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "reuse-query" ]
}

@test "stop after worker restart removes state and allows name reuse" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name worker-restart-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "worker-restart-query" ]

  sleep 1

  # Kill the worker — the query is lost on the worker side
  docker compose kill worker-1
  docker compose up -d --wait worker-1

  # Stop should succeed (logs warning but removes state file)
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop worker-restart-query
  [ "$status" -eq 0 ]

  # Name should be reusable
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name worker-restart-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "worker-restart-query" ]
}

@test "stop named query while worker is offline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name offline-stop-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "offline-stop-query" ]

  sleep 1

  # Kill the worker and leave it offline
  docker compose kill worker-1

  # Stop should still succeed — logs a warning but removes the state file
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop offline-stop-query
  [ "$status" -eq 0 ]

  # Bring worker back and verify name is reusable
  docker compose up -d --wait worker-1
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start --name offline-stop-query 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  [ "$output" = "offline-stop-query" ]
}

@test "named query rejects invalid characters" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -d -t tests/good/select-gen-into-void.yaml start --name 'My Query!' 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]

  sync_workdir
  grep "Only lowercase letters, digits, and hyphens are allowed" nes-cli.log
}

@test "named query rejects underscores" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -d -t tests/good/select-gen-into-void.yaml start --name 'my_query' 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]

  sync_workdir
  grep "Only lowercase letters, digits, and hyphens are allowed" nes-cli.log
}

@test "named query rejects uppercase" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -d -t tests/good/select-gen-into-void.yaml start --name MyQuery 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]

  sync_workdir
  grep "Only lowercase letters, digits, and hyphens are allowed" nes-cli.log
}

@test "query status with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_BASH 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start "select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK"'
  [ "$status" -eq 0 ]

  QUERY_ID=$output

  sleep 1

  run DOCKER_BASH "cat tests/good/select-gen-into-void.yaml | nes-cli -t - status $QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r '.[0].query_status')
  [ "$QUERY_STATUS" = "Running" ]
}
