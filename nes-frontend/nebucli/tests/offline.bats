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
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")

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
  run $NES_CLI -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump with debug" {
  run $NES_CLI -d -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment" {

  NES_SETUP_FILE=tests/good/chained-joins.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment and adhoc query" {
  NES_SETUP_FILE=tests/good/select-gen-into-void.yaml run $NES_CLI dump 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "nebucli dump with setup from stdin" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI -s - dump"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with setup from stdin and adhoc query" {
  run bash -c "cat tests/good/select-gen-into-void.yaml | $NES_CLI -s - dump 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'"
  [ "$status" -eq 0 ]
}

@test "setup resolution: -s flag takes priority over env and working directory" {
  # Put a valid setup as setup.yaml in cwd — should be ignored
  cp tests/good/select-gen-into-void.yaml setup.yaml
  # Set env to a different valid topology — should be ignored
  NES_SETUP_FILE=tests/good/select-gen-into-void.yaml run $NES_CLI -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: NES_SETUP_FILE takes priority over working directory" {
  cp tests/good/select-gen-into-void.yaml setup.yaml
  NES_SETUP_FILE=tests/good/chained-joins.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yaml in working directory" {
  cp tests/good/chained-joins.yaml setup.yaml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yml in working directory" {
  cp tests/good/chained-joins.yaml setup.yml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yaml preferred over setup.yml" {
  cp tests/good/chained-joins.yaml setup.yaml
  echo "invalid yaml: [" > setup.yml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: error when no setup found" {
  run $NES_CLI -d dump
  [ "$status" -eq 1 ]
  grep "no setup file found" nes-cli.log
}

@test "setup resolution: -s with nonexistent file" {
  run $NES_CLI -d -s nonexistent.yaml dump
  [ "$status" -eq 1 ]
  grep "No such file" nes-cli.log
}

@test "setup resolution: -s - with empty stdin" {
  run bash -c "echo -n '' | $NES_CLI -d -s - dump"
  [ "$status" -eq 1 ]
  grep "missing field" nes-cli.log
}

@test "setup resolution: -s - reads from stdin" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI -s - dump"
  [ "$status" -eq 0 ]
}

@test "topology validation: reject topology with cycle" {
  run $NES_CLI -d -s tests/good/topology-with-cycle.yaml start
  [ "$status" -eq 1 ]
  grep -i "cycle" nes-cli.log
}

@test "topology without capacity field defaults to infinite capacity" {
  # Create a minimal topology without capacity field
  cat > setup-no-capacity.yaml << 'SETUPEOF'
sinks:
  - name: VOID_SINK
    host_addr: worker-1:8080
    schema:
      - name: VALUE
        type: UINT64
    sink_type: Void
    config: { }
    parser_config: { }
logical_sources:
  - name: stream
    schema:
      - name: VALUE
        type: UINT64
physical_sources:
  - logical_source: stream
    host_addr: worker-1:8080
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: ONE
      seed: 1
      generator_schema: |
        SEQUENCE UINT64 0 100 1
workers:
  - host_addr: worker-1:8080
    data_addr: worker-1:9090
SETUPEOF

  run $NES_CLI -s setup-no-capacity.yaml dump
  [ "$status" -eq 0 ]
}

@test "optimizer configuration: -s with invalid optimizer configuration name" {
  run $NES_CLI -d -s tests/bad/invalid_optimizer_config_name.yaml dump 'SELECT * FROM stream INTO sink'
  [ "$status" -eq 1 ]
  grep "Unrecognized configuration key: 'test_invalid_optimizer_config_name'" nes-cli.log
}

@test "optimizer configuration: -s with invalid optimizer configuration value" {
  run $NES_CLI -d -s tests/bad/invalid_optimizer_config_value.yaml dump 'SELECT * FROM stream INTO sink'
  [ "$status" -eq 1 ]
  grep "Enum for INVALID was not found" nes-cli.log
}

@test "yaml parser should reject unknown keys" {
  run $NES_CLI -d -s tests/bad/invalid_config_with_unknown_keys1.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected one of \`query\`, \`sinks\`, \`logical_sources\`, \`physical_sources\`, \`workers\`, \`optimizer\`" nes-cli.log

  run $NES_CLI -d -s tests/bad/invalid_config_with_unknown_keys2.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected \`name\` or \`schema\`" nes-cli.log

  run $NES_CLI -d -s tests/bad/invalid_config_with_unknown_keys3.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected one of \`host_addr\`, \`data_addr\`, \`max_operators\`, \`peers\`, \`config\`" nes-cli.log
}

