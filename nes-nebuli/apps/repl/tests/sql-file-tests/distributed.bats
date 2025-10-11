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
  # Validate SYSTEST environment variable once for all tests
  if [ -z "$NEBULI" ]; then
    echo "ERROR: NEBUCLI environment variable must be set" >&2
    echo "Usage: NEBUCLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULI_TESTDATA" ]; then
    echo "ERROR: NEBUCLI_TESTDATA environment variable must be set" >&2
    echo "Usage: NEBUCLI_TESTDATA=/path/to/nebucli/testdata" >&2
    exit 1
  fi

  if [ ! -f "$NEBULI" ]; then
    echo "ERROR: NEBUCLI file does not exist: $NEBULI" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$NEBULI" ]; then
    echo "ERROR: NEBUCLI file is not executable: $NEBULI" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NEBUCLI: $NEBULI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup_file() {
  docker build -t worker-image -f - $(dirname $(realpath $NEBULASTREAM)) <<EOF
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
  docker build -t nebuli-image -f - $(dirname $(realpath $NEBULI)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    COPY nebuli /usr/bin
EOF
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NEBULI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

teardown() {
  docker compose down -v || true
}

function setup_distributed() {
  tests/util/create_compose.sh "$1" >docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_NEBULI() {
  echo "# Additional Flags: ${ADDITIONAL_NEBULI_FLAGS}" >&3
  cat "$1" | docker compose exec -it nebuli nebuli -s worker-node:8080 -f JSON ${ADDITIONAL_NEBULI_FLAGS}
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
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NEBULI tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]

  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[0]}"
  assert_json_equal '[{"parser_config":{"field_delimiter":",","tuple_delimiter":"\n","type":"CSV"},"physical_source_id":1,"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_config":[{"flush_interval_ms":10},{"generator_rate_config":"emit_rate 10"},{"generator_rate_type":"FIXED"},{"generator_schema":"SEQUENCE UINT64 0 10000000 1"},{"max_inflight_buffers":0},{"max_runtime_ms":10000000},{"seed":1},{"stop_generator_when_sequence_finishes":"ALL"}],"source_name":"ENDLESS","source_type":"Generator"}]' "${lines[1]}"
  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"sink_config":[{"add_timestamp":false},{"append":false},{"file_path":"out.csv"},{"input_format":"CSV"}],"sink_name":"SOMESINK","sink_type":"File"}]' "${lines[2]}"
  assert_json_equal '[]' "${lines[3]}"
  QUERY_ID=$(echo ${lines[4]} | jq -r '.[0].local_query_id')

  # One global and one local query
  echo "${lines[5]}" | jq -e '(. | length) == 1'
  echo "${lines[5]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"local_query_id\":\"${QUERY_ID}\"}]" "${lines[6]}"
  assert_json_contains "[]" "${lines[7]}"
}

@test "launch multiple queries" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NEBULI tests/sql-file-tests/good/multiple_queries.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NEBULI tests/sql-file-tests/bad/integer_literal_in_query_without_type.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nebuli.log
}

@test "launch query and wait for query termination on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit WAIT_FOR_QUERY_TERMINATION" run DOCKER_NEBULI tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 3000ms. We expect nebuli to not terminate while the query is still running due to the WAIT_FOR_QUERY_TERMINATION option
  duration=$((end_time - start_time))
  [ "$duration" -ge 3 ]
}

@test "launch query and terminate query on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit STOP_QUERIES" run DOCKER_NEBULI tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 3000ms. We expect nebuli to terminate within 1 second as it is configured to terminate all pending queries on exit
  duration=$((end_time - start_time))
  [ "$duration" -le 1 ]
}

@test "default on-exit behavior should keep queries alive" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  run DOCKER_NEBULI tests/sql-file-tests/good/multiple_queries.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]
  # The query is configured to produce data for 3000ms. We expect nebuli to terminate within 1 second as it is configured to terminate regardless of pending queries on exit
  duration=$((end_time - start_time))
  [ "$duration" -le 1 ]

  sleep 1
  # Check the log to ensure that the query has been started but not stopped
  grep "Starting source with originId" worker-node/singleNodeWorker.log
  ! grep "attempting to stop source" worker-node/singleNodeWorker.log
}
