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
  if [ -z "$NEBUCLI" ]; then
    echo "ERROR: NEBUCLI environment variable must be set" >&2
    echo "Usage: NEBUCLI=/path/to/nebucli bats nebucli.bats" >&2
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

  if [ ! -x "$NEBUCLI" ]; then
    echo "ERROR: NEBUCLI file is not executable: $NEBUCLI" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NEBUCLI: $NEBUCLI" >&3
  echo "# Using NEBUCLI_TESTDATA: $NEBUCLI_TESTDATA" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NEBUCLI_TESTDATA" "$TMP_DIR"
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

  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NEBUCLI dump 'SELECT * FROM stream INTO VOID_SINK'
  [ "$status" -eq 0 ]
}
