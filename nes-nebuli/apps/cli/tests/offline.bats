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

  # Override XDG_STATE_HOME to prevent polluting user's actual state directory
  export XDG_STATE_HOME="$TMP_DIR/.xdg-state"
  mkdir -p "$XDG_STATE_HOME"

  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
  echo "# Using XDG_STATE_HOME: $XDG_STATE_HOME" >&3
}

teardown() {
  # Clean up temporary directory
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
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

@test "nebucli creates state directory in XDG_STATE_HOME" {
  # XDG_STATE_HOME is already set in setup() to prevent pollution
  # Simulate QueryStateBackend creating the directory
  mkdir -p "$XDG_STATE_HOME/nebucli"

  # Verify directory exists
  [ -d "$XDG_STATE_HOME/nebucli" ]
}

@test "nebucli creates state directory in HOME fallback" {
  # Test HOME fallback by unsetting XDG_STATE_HOME
  unset XDG_STATE_HOME
  export HOME="$TMP_DIR/home"
  mkdir -p "$HOME"

  # Simulate QueryStateBackend creating the fallback directory
  mkdir -p "$HOME/.local/state/nebucli"

  # Verify directory exists
  [ -d "$HOME/.local/state/nebucli" ]
}

@test "nebucli state file format is valid JSON" {
  # XDG_STATE_HOME is already set in setup() to prevent pollution
  mkdir -p "$XDG_STATE_HOME/nebucli"

  # Create a mock query state file
  cat > "$XDG_STATE_HOME/nebucli/42.json" << 'JSONEOF'
{
    "query_id": 42,
    "created_at": "2026-01-16T16:00:00+0000"
}
JSONEOF

  # Verify JSON is valid
  run jq . "$XDG_STATE_HOME/nebucli/42.json"
  [ "$status" -eq 0 ]

  # Verify it contains query_id
  run jq -e '.query_id == 42' "$XDG_STATE_HOME/nebucli/42.json"
  [ "$status" -eq 0 ]

  # Verify it contains created_at
  run jq -e '.created_at' "$XDG_STATE_HOME/nebucli/42.json"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with topology from stdin" {
  run bash -c "cat tests/good/crazy-join.yaml | $NES_CLI -t - dump"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with topology from stdin and adhoc query" {
  run bash -c "cat tests/good/crazy-join.yaml | $NES_CLI -t - dump 'SELECT * FROM stream INTO VOID_SINK'"
  [ "$status" -eq 0 ]
}

@test "topology resolution: -t flag takes priority over env and working directory" {
  # Put a valid topology as topology.yaml in cwd — should be ignored
  cp tests/good/select-gen-into-void.yaml topology.yaml
  # Set env to a different valid topology — should be ignored
  NES_TOPOLOGY_FILE=tests/good/select-gen-into-void.yaml run $NES_CLI -t tests/good/crazy-join.yaml dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: NES_TOPOLOGY_FILE takes priority over working directory" {
  cp tests/good/select-gen-into-void.yaml topology.yaml
  NES_TOPOLOGY_FILE=tests/good/crazy-join.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yaml in working directory" {
  cp tests/good/crazy-join.yaml topology.yaml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yml in working directory" {
  cp tests/good/crazy-join.yaml topology.yml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yaml preferred over topology.yml" {
  cp tests/good/crazy-join.yaml topology.yaml
  echo "invalid yaml: [" > topology.yml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: error when no topology found" {
  run $NES_CLI -d dump
  [ "$status" -eq 1 ]
  grep "Could not find topology file" nes-cli.log
}

@test "topology resolution: -t with nonexistent file" {
  run $NES_CLI -d -t nonexistent.yaml dump
  [ "$status" -eq 1 ]
  grep "does not exist" nes-cli.log
}

@test "topology resolution: -t - with empty stdin" {
  run bash -c "echo -n '' | $NES_CLI -d -t - dump"
  [ "$status" -eq 1 ]
  grep "No topology data received from stdin" nes-cli.log
}

@test "topology resolution: -t - reads from stdin" {
  run bash -c "cat tests/good/crazy-join.yaml | $NES_CLI -t - dump"
  [ "$status" -eq 0 ]
}
