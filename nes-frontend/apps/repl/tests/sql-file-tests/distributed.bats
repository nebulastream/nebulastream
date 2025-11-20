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
  # Clean up leaked networks from previous crashed runs
  docker network prune -f --filter label=nes-test=distributed-repl 2>/dev/null || true

  # Validate environment variables
  if [ -z "$NES_REPL" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    echo "Usage: NES_CLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NES_REPL_TESTDATA" ]; then
    echo "ERROR: NES_REPL_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_REPL_TESTDATA=/path/to/repl/testdata" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    echo "Usage: NES_TEST_TMP_DIR=/path/to/build/test-tmp" >&2
    exit 1
  fi

  if [ ! -f "$NES_REPL" ]; then
    echo "ERROR: NES_CLI file does not exist: $NES_REPL" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$NES_REPL" ]; then
    echo "ERROR: NES_CLI file is not executable: $NES_REPL" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  # Build Docker images with unique tags to avoid collisions when test suites run in parallel
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export WORKER_IMAGE="nes-worker-repl-test-${suffix}"
  docker build -t $WORKER_IMAGE -f - $(dirname $(realpath $NEBULASTREAM)) <<EOF
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
  export REPL_IMAGE="nes-repl-image-${suffix}"
  docker build -t $REPL_IMAGE -f - $(dirname $(realpath $NES_REPL)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    COPY nes-repl /usr/bin
EOF

  # Print environment info for debugging
  echo "# Using NES_REPL: $NES_REPL" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using REPL_IMAGE: $REPL_IMAGE" >&3
}

teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $REPL_IMAGE || true
}

setup() {
  # Create temp directory within the mounted workspace (not /tmp)
  # so it's accessible from docker-compose containers running on the host
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cp -r "$NES_REPL_TESTDATA" "$TMP_DIR"
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
  tests/util/create_compose.sh "$1" >docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_NES_REPL() {
  # In Docker-out-of-Docker environments, docker exec tears down the exec
  # session when its stdin pipe closes, even if the process is still running.
  # Work around this by: (1) piping from tail to keep stdin open until
  # nes-repl exits, and (2) reading the SQL file from the volume inside the
  # container instead of piping it via stdin.
  tail -f /dev/null | docker compose exec nes-repl bash -c "nes-repl -f JSON ${ADDITIONAL_NEBULI_FLAGS} </workdir/$1"
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
  setup_distributed tests/topologies/8-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/good/test_large_distributed.sql
  [ "$status" -eq 0 ]

  assert_json_equal '[{"worker":"sink-node:8080"}]' "${lines[0]}"
  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[1]}"
  assert_json_equal '[{"host":"sink-node:8080","parser_config":{"field_delimiter":",","tuple_delimiter":"\n","type":"CSV"},"physical_source_id":1,"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_config":[{"flush_interval_ms":10},{"generator_rate_config":"emit_rate 10"},{"generator_rate_type":"FIXED"},{"generator_schema":"SEQUENCE UINT64 0 10000000 1"},{"max_inflight_buffers":0},{"max_runtime_ms":10000000},{"seed":1},{"stop_generator_when_sequence_finishes":"ALL"}],"source_name":"ENDLESS","source_type":"Generator"}]' "${lines[2]}"
  assert_json_equal '[{"host":"sink-node:8080","schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"sink_config":[{"add_timestamp":false},{"append":false},{"file_path":"out.csv"},{"input_format":"CSV"}],"sink_name":"SOMESINK","sink_type":"File"}]' "${lines[3]}"
  assert_json_equal '[]' "${lines[4]}"
  QUERY_ID=$(echo ${lines[5]} | jq -r '.[0].query_id')

  # One global and one local query
  echo "${lines[6]}" | jq -e '(. | length) == 2'
  echo "${lines[6]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[7]}"
  assert_json_contains "[]" "${lines[8]}"
}

@test "launch multiple queries" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/bad/integer_literal_in_query_without_type_distributed.sql
  [ "$status" -ne 0 ]

  sync_workdir
  grep "invalid query syntax" nes-repl.log
}

@test "launch query and wait for query termination on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit WAIT_FOR_QUERY_TERMINATION" run DOCKER_NES_REPL tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to not terminate while the query is still running due to the WAIT_FOR_QUERY_TERMINATION option
  duration=$((end_time - start_time))
  [ "$duration" -ge 10 ]
}

@test "launch query and terminate query on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit STOP_QUERIES" run DOCKER_NES_REPL tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to terminate within 5 seconds as it is configured to terminate all pending queries on exit
  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]

  # Verify that the implicit STOP QUERY statement was executed and its result matches the query that was created
  # lines[2] is the SELECT query result with query_id
  QUERY_ID=$(echo ${lines[3]} | jq -r '.[0].query_id')
  # The last line should contain the result of the implicit STOP QUERY command with the same query_id
  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[-1]}"
}

@test "default on-exit behavior should keep queries alive" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  run DOCKER_NES_REPL tests/sql-file-tests/good/multiple_queries_distributed.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]
  # The query is configured to produce data for 10s. We expect nes-repl to terminate well under that,
  # as the default on-exit behavior (DO_NOTHING) exits regardless of pending queries.
  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]

  sleep 1
  # Check the log to ensure that the query has been started but not stopped
  sync_workdir
  grep "Starting source with originId" worker-node/singleNodeWorker.log
  ! grep "attempting to stop source" worker-node/singleNodeWorker.log
}
