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

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3

  # Build Docker images
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
  docker build -t nes-cli-image -f - $(dirname $(realpath $NES_CLI)) <<EOF
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
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

teardown() {
  docker compose down -v || true
}

function setup_distributed() {
  tests/util/create_compose.sh "$1" > docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_NES_CLI() {
  docker compose run --rm nes-cli nes-cli "$@"
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

  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run DOCKER_NES_CLI -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  [ -f "$output" ]
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
  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI -t tests/good/distributed-query-deployment.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]
  echo "${output}" | jq -e '(. | length) == 3' # 1 global + 2 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/crazy-join.yaml

  run DOCKER_NES_CLI start
  echo $output
  [ "$status" -eq 0 ]
  [ -f "$output" ]
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
  setup_distributed tests/good/crazy-join-one-fast-source.yaml

  run DOCKER_NES_CLI start
  echo $output
  cat nes-cli.log
  [ "$status" -eq 0 ]
  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run DOCKER_NES_CLI status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "PartiallyStopped" ]

  run DOCKER_NES_CLI stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/crazy-join.yaml

  docker compose stop worker-1

  run DOCKER_NES_CLI -d start
  grep "(6002) : query registration call failed; Status: UNAVAILABLE" nes-cli.log
  [ "$status" -eq 1 ]

  docker compose start worker-1
  # now it should work
  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/crazy-join.yaml

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
    "grpc_addr": "worker-2:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-3:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-8:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-7:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-4:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-9:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-5:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-6:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/crazy-join.yaml

  run DOCKER_NES_CLI start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # Simulate a crash by killing worker-1.
  docker compose kill worker-1
  run DOCKER_NES_CLI status "$QUERY_ID"
  [ "$status" -eq 0 ]

  sleep 1

  docker compose start worker-1

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
    "grpc_addr": "worker-1:8080",
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

  sleep 5

  run DOCKER_NES_CLI stop $query_id

  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure.yaml

  run DOCKER_NES_CLI start
  [ $status -eq 0 ]
  query_id=$output

  sleep 5

  docker compose stop worker-1
  grep "Backpressure" worker-2/singleNodeWorker.log
  grep "Channel Closed by other side." worker-2/singleNodeWorker.log
  grep "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log
  sleep 2

  run DOCKER_NES_CLI status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Failed"
    },
    {
      "query_status": "ConnectionError",
      "grpc_addr": "worker-1:8080"
    },
    {
      "query_status": "Failed",
      "grpc_addr": "worker-2:8080"
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

  sleep 5

  docker compose stop worker-2
  grep "Backpressure" worker-2/singleNodeWorker.log
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
      "grpc_addr": "worker-1:8080"
    },
    {
      "query_status": "ConnectionError",
      "grpc_addr": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}
