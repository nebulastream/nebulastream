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
  if [ -z "$NEBUCLI" ]; then
    echo "ERROR: NEBUCLI environment variable must be set" >&2
    echo "Usage: NEBUCLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBUCLI_TESTDATA" ]; then
    echo "ERROR: NEBUCLI_TESTDATA environment variable must be set" >&2
    echo "Usage: NEBUCLI_TESTDATA=/path/to/nebucli/testdata" >&2
    exit 1
  fi

  if [ ! -f "$NEBUCLI" ]; then
    echo "ERROR: NEBUCLI file does not exist: $NEBUCLI" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$NEBUCLI" ]; then
    echo "ERROR: NEBUCLI file is not executable: $NEBUCLI" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NEBUCLI: $NEBUCLI" >&3
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
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  docker build -t nebucli-image -f - $(dirname $(realpath $NEBUCLI)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    COPY nebucli /usr/bin
EOF
}

INSTANCE_PID=0
setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NEBUCLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}
teardown() {
  docker compose down -v || true
}
function setup_distributed() {
  tests/create_compose.sh "$1" >docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_NEBUCLI() {
  docker compose run --rm nebucli nebucli "$@"
}

@test "nebucli shows help" {
  run $NEBUCLI --help
  [ "$status" -eq 0 ]
}
@test "nebucli dump" {
  run $NEBUCLI -t tests/good/crazy-join.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump with debug" {
  run $NEBUCLI -d -t tests/good/crazy-join.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment" {

  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NEBUCLI dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment and adhoc query" {

  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NEBUCLI dump "SELECT * FROM stream INTO void_sink"
  [ "$status" -eq 0 ]
}

# bats file_tags=docker
@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}
@test "launch multiple query from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml

  run DOCKER_NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  query_ids=("${lines[@]}")

  run DOCKER_NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[0]}"
  [ "$status" -eq 0 ]

  run DOCKER_NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[1]}" "${query_ids[2]}" "${query_ids[3]}" "${query_ids[4]}" "${query_ids[5]}"
  [ "$status" -eq 0 ]

  run DOCKER_NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[6]}" "${query_ids[7]}"
  [ "$status" -eq 0 ]
}
@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]
}
@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml start "selectaa double * UINT64(2) from generator_source INTO void_sink"
  [ "$status" -eq 1 ]
}
@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]

  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}
@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]

  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NEBUCLI -t tests/good/select-gen-into-void.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.global_query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run DOCKER_NEBUCLI -t tests/good/distributed-query-deployment.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]
  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NEBUCLI -t tests/good/distributed-query-deployment.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]
  echo "${output}" | jq -e '(. | length) == 3' # 1 global + 2 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.global_query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/crazy-join.yaml

  run DOCKER_NEBUCLI start
  echo $output
  [ "$status" -eq 0 ]
  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NEBUCLI status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.global_query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]

  run DOCKER_NEBUCLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/crazy-join-one-fast-source.yaml

  run DOCKER_NEBUCLI start
  echo $output
  [ "$status" -eq 0 ]
  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NEBUCLI status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.global_query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "PartiallyStopped" ]

  run DOCKER_NEBUCLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/crazy-join.yaml

  docker compose stop worker-1

  run DOCKER_NEBUCLI -d start
  grep "(6002) : query registration call failed; Status: UNAVAILABLE" nebucli.log
  grep "worker-1:8080: Domain name not found" nebucli.log
  [ "$status" -eq 1 ]

  docker compose start worker-1
  # now it should work
  run DOCKER_NEBUCLI start
  [ "$status" -eq 0 ]
}

# bats test_tags=bats:focus
@test "worker goes offline during processing" {
  setup_distributed tests/good/crazy-join.yaml

  run DOCKER_NEBUCLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  docker compose stop worker-1
  run DOCKER_NEBUCLI status "$QUERY_ID"
  echo $output
  [ "$status" -eq 1 ]
}
