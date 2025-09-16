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

  if [ -z "$HEALTH_PROBE" ]; then
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

INSTANCE_PID=0
setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NEBUCLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  INSTANCE_PID=0

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}
teardown() {
  if [ "$INSTANCE_PID" -ne 0 ]; then
    kill -SIGTERM $INSTANCE_PID
    wait $INSTANCE_PID
  fi
  INSTANCE_PID=0
}

start_nes() {
  $NEBULASTREAM >/dev/null &
  INSTANCE_PID=$!
  $HEALTH_PROBE -addr localhost:8080 -connect-timeout 5s
}

@test "nebucli shows help" {
  run $NEBUCLI --help
  [ "$status" -eq 0 ]
}

@test "launch query from topology" {
  start_nes
  run $NEBUCLI -t tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}
@test "launch multiple query from topology" {
  start_nes
  run $NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  query_ids=("${lines[@]}")

  run $NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[0]}"
  [ "$status" -eq 0 ]

  run $NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[1]}" "${query_ids[2]}" "${query_ids[3]}" "${query_ids[4]}" "${query_ids[5]}"
  [ "$status" -eq 0 ]

  run $NEBUCLI -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[6]}" "${query_ids[7]}"
  [ "$status" -eq 0 ]
}
@test "launch query from commandline" {
  start_nes
  run $NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]
}
@test "launch bad query from commandline" {
  start_nes
  run $NEBUCLI -t tests/good/select-gen-into-void.yaml start "selectaa double * UINT64(2) from generator_source INTO void_sink"
  [ "$status" -eq 1 ]
}
@test "launch and stop query" {
  start_nes
  run $NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]

  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run $NEBUCLI -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}
@test "launch and monitor query" {
  start_nes
  run $NEBUCLI -t tests/good/select-gen-into-void.yaml start "select double from generator_source INTO void_sink"
  [ "$status" -eq 0 ]

  [ -f "$output" ]
  QUERY_ID=$output

  sleep 1

  run $NEBUCLI -t tests/good/select-gen-into-void.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.local_query_id == $query_id) | .query_status')
  echo "Status: $QUERY_STATUS" >&3
  [ "$QUERY_STATUS" = "Running" ]
}
