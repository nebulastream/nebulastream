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

# This File contains all offline tests for nebucli. Offline tests are tests which do not require a worker and are
# completely local to nebucli

setup_file() {
  # Validate SYSTEST environment variable once for all tests
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    echo "Usage: NES_CLI=/path/to/nebucli bats nebucli.bats" >&2
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

  if [ ! -x "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file is not executable: $NES_CLI" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NES_CLI_TESTDATA: $NES_CLI_TESTDATA" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
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

@test "nebucli shows help" {
  run $NES_CLI --help
  [ "$status" -eq 0 ]
}

@test "nebucli dump" {
  run $NES_CLI -t tests/good/crazy-join.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump with debug" {
  run $NES_CLI -d -t tests/good/crazy-join.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment" {

  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment and adhoc query" {

  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NES_CLI dump 'SELECT * FROM stream INTO VOID_SINK'
  [ "$status" -eq 0 ]
}
