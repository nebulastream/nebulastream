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
  run $NES_CLI -t tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump with debug" {
  run $NES_CLI -d -t tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment" {

  NES_TOPOLOGY_FILE=tests/good/chained-joins.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment and adhoc query" {

  NES_TOPOLOGY_FILE=tests/good/chained-joins.yaml run $NES_CLI dump 'SELECT * FROM stream INTO VOID_SINK'
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
  cat > "$XDG_STATE_HOME/nebucli/550e8400-e29b-41d4-a716-446655440000.json" << 'JSONEOF'
{
    "query_id": "550e8400-e29b-41d4-a716-446655440000",
    "created_at": "2026-01-16T16:00:00+0000"
}
JSONEOF

  # Verify JSON is valid
  run jq . "$XDG_STATE_HOME/nebucli/550e8400-e29b-41d4-a716-446655440000.json"
  [ "$status" -eq 0 ]

  # Verify it contains query_id
  run jq -e '.query_id == "550e8400-e29b-41d4-a716-446655440000"' "$XDG_STATE_HOME/nebucli/550e8400-e29b-41d4-a716-446655440000.json"
  [ "$status" -eq 0 ]

  # Verify it contains created_at
  run jq -e '.created_at' "$XDG_STATE_HOME/nebucli/550e8400-e29b-41d4-a716-446655440000.json"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with topology from stdin" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI -t - dump"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with topology from stdin and adhoc query" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI -t - dump 'SELECT * FROM stream INTO VOID_SINK'"
  [ "$status" -eq 0 ]
}

@test "topology resolution: -t flag takes priority over env and working directory" {
  # Put a valid topology as topology.yaml in cwd — should be ignored
  cp tests/good/select-gen-into-void.yaml topology.yaml
  # Set env to a different valid topology — should be ignored
  NES_TOPOLOGY_FILE=tests/good/select-gen-into-void.yaml run $NES_CLI -t tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: NES_TOPOLOGY_FILE takes priority over working directory" {
  cp tests/good/select-gen-into-void.yaml topology.yaml
  NES_TOPOLOGY_FILE=tests/good/chained-joins.yaml run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yaml in working directory" {
  cp tests/good/chained-joins.yaml topology.yaml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yml in working directory" {
  cp tests/good/chained-joins.yaml topology.yml
  run $NES_CLI dump
  [ "$status" -eq 0 ]
}

@test "topology resolution: topology.yaml preferred over topology.yml" {
  cp tests/good/chained-joins.yaml topology.yaml
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
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI -t - dump"
  [ "$status" -eq 0 ]
}

@test "errors are printed to stderr when console logging is disabled" {
  run $NES_CLI -t nonexistent.yaml dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"does not exist"* ]]
}

@test "topology validation: reject topology with cycle" {
  run $NES_CLI -d -t tests/good/topology-with-cycle.yaml dump
  [ "$status" -eq 1 ]
  grep -i "cycle" nes-cli.log
}

@test "topology without capacity field defaults to infinite capacity" {
  # Create a minimal topology without capacity field
  cat > topology-no-capacity.yaml << 'TOPEOF'
query: |
  SELECT * FROM stream INTO VOID_SINK
sinks:
  - name: VOID_SINK
    host: worker-1:8080
    schema:
      - name: stream$value
        type: UINT64
    type: Void
    config: { }
    parser_config: { }
logical:
  - name: stream
    schema:
      - name: value
        type: UINT64
physical:
  - logical: stream
    host: worker-1:8080
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: ONE
      seed: 1
      generator_schema: |
        SEQUENCE UINT64 0 100 1
workers:
  - host: worker-1:8080
    data_address: worker-1:9090
TOPEOF

  run $NES_CLI -t topology-no-capacity.yaml dump
  [ "$status" -eq 0 ]
  # Verify it actually parsed and shows the plan
  echo "$output" | grep -q "Decomposed Plans"
}

@test "optimizer configuration: -t with invalid optimizer configuration name" {
  run $NES_CLI -d -t tests/bad/invalid_optimizer_config_name.yaml dump
  [ "$status" -eq 1 ]
  grep "invalid config parameter; Unrecognized configuration key: 'test_invalid_optimizer_config_name'" nes-cli.log
}

@test "optimizer configuration: -t with invalid optimizer configuration value" {
  run $NES_CLI -d -t tests/bad/invalid_optimizer_config_value.yaml dump
  [ "$status" -eq 1 ]
  grep "invalid config parameter; Enum for INVALID was not found." nes-cli.log
}

@test "topology validation: reject duplicate worker host" {
  run $NES_CLI -d -t tests/bad/duplicate-worker-host.yaml dump
  [ "$status" -eq 1 ]
  grep -i "duplicate worker" nes-cli.log
}

@test "yaml parser should reject unknown keys" {
  run $NES_CLI -d -t tests/bad/invalid_config_with_unknown_keys1.yaml dump
  [ "$status" -eq 1 ]
  grep "Unknown key 'idontexist'" nes-cli.log
  grep "Expected one of: query, sinks, logical, physical, optimizer, workers" nes-cli.log

  run $NES_CLI -d -t tests/bad/invalid_config_with_unknown_keys2.yaml dump
  [ "$status" -eq 1 ]
  grep "Unknown key 'idontexist'" nes-cli.log
  grep "Expected one of: name, schema" nes-cli.log

  run $NES_CLI -d -t tests/bad/invalid_config_with_unknown_keys3.yaml dump
  [ "$status" -eq 1 ]
  grep "Unknown key 'idontexist'" nes-cli.log
  grep "Expected one of: host, data_address, max_operators, downstream, config" nes-cli.log
}

# --- Error message quality tests ---
# Each test starts from a valid base topology and introduces exactly one error.
# The base topology is tests/good/select-gen-into-void.yaml.

# Helper: apply a sed expression to the base topology and return the path to the modified file.
bad_topology() {
  local out="$TMP_DIR/bad_topology.yaml"
  sed "$@" tests/good/select-gen-into-void.yaml > "$out"
  echo "$out"
}

@test "error: unknown worker key reports key name and suggestions" {
  # Rename 'data_address' to 'bogus_key' in the workers section
  local f=$(bad_topology 's/data_address:/bogus_key:/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Unknown key 'bogus_key'"* ]]
  [[ "$output" == *"workers[0]"* ]]
  [[ "$output" == *"Expected one of:"* ]]
}

@test "error: missing sink host reports path" {
  # Delete the 'host:' line inside the sinks section (between 'sinks:' and the next top-level key)
  sed '/^sinks:/,/^[a-z]/{/^    host:/d;}' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Missing required key 'host'"* ]]
  [[ "$output" == *"sinks[0]"* ]]
}

@test "error: unknown physical source key reports key and suggestions" {
  # Add an extra 'input_format: CSV' line after 'type: Generator' in the physical section
  local f=$(bad_topology '/^physical:/,/^[a-z]/s/^    type: Generator/    type: Generator\n    input_format: CSV/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Unknown key 'input_format'"* ]]
  [[ "$output" == *"physical[0]"* ]]
  [[ "$output" == *"Expected one of:"* ]]
}

@test "error: missing parser_config reports path" {
  # In the physical section, delete 'parser_config:' and its indented children
  sed '/^physical:/,/^[a-z]/{/^    parser_config:/,/^    [a-z]/{/^    parser_config:/d;/^      /d;};}' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Missing required key 'parser_config'"* ]]
  [[ "$output" == *"physical[0]"* ]]
}

@test "error: missing workers key reports path" {
  # Delete everything from 'workers:' to end of file
  sed '/^workers:/,$d' tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Missing required key 'workers'"* ]]
}

@test "error: null workers section reports type error" {
  # Keep 'workers:' but delete all lines after it, leaving the value as null
  sed '/^workers:/,$ { /^workers:/!d; }' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Expected a list at workers"* ]]
}

@test "error: wrong parser_config key reports missing type" {
  # Rename 'type: CSV' to 'input_format: CSV' in parser_config
  local f=$(bad_topology 's/type: CSV/input_format: CSV/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Parser configuration must contain: type"* ]]
}

@test "error: unregistered source type reports type name" {
  # Change source type from 'Generator' to a non-existent type
  local f=$(bad_topology 's/type: Generator/type: NonExistentSource/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"source type 'NonExistentSource' is not registered"* ]]
}

@test "error: unregistered sink type reports type name" {
  # Change sink type from 'Void' to a non-existent type
  local f=$(bad_topology 's/type: Void/type: NonExistentSink/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Invalid configuration for sink"* ]]
  [[ "$output" == *"NonExistentSink"* ]]
}

@test "error: source on non-existing worker reports source name and host" {
  # Change 'host:' only within the physical section to a non-existing worker
  sed '/^physical:/,/^[a-z]/s/host: worker-1:8080/host: no-such-worker:8080/' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"SOURCE(GENERATOR_SOURCE)"* ]]
  [[ "$output" == *"non-existing worker"* ]]
  [[ "$output" == *"no-such-worker:8080"* ]]
}

@test "error: sink on non-existing worker reports sink name and host" {
  # Change 'host:' only within the sinks section to a non-existing worker
  sed '/^sinks:/,/^[a-z]/s/host: worker-1:8080/host: no-such-worker:8080/' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"SINK(VOID_SINK)"* ]]
  [[ "$output" == *"non-existing worker"* ]]
  [[ "$output" == *"no-such-worker:8080"* ]]
}

@test "error: unknown source name reports the name" {
  # Change 'logical: GENERATOR_SOURCE' to a name that doesn't match any logical source
  local f=$(bad_topology 's/logical: GENERATOR_SOURCE/logical: nonexistent_source/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"unknown source name"* ]]
}

@test "error: unknown sink name in query" {
  # Change the sink name in the query (line 30) but keep the sink definition intact
  sed '30s/VOID_SINK/NONEXISTENT_SINK/' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"unknown sink name"* ]]
}

@test "error: sink schema mismatch reports expected vs actual fields" {
  # Rename the sink schema field to something that won't match the source output
  local f=$(bad_topology 's/GENERATOR_SOURCE\$DOUBLE/WRONG\$FIELD/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot infer schema"* ]]
}

@test "error: empty workers list causes placement failure" {
  # Replace the workers section content with an empty list '[]'
  sed '/^workers:/,$ { /^workers:/!d; }; s/^workers:.*/workers: []/' \
    tests/good/select-gen-into-void.yaml > "$TMP_DIR/bad_topology.yaml"
  run $NES_CLI -d -t "$TMP_DIR/bad_topology.yaml" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"non-existing worker"* ]]
}

@test "error: type mismatch in worker config reports path" {
  # Change max_operators from a number to a string to trigger a type conversion error
  local f=$(bad_topology 's/max_operators: 10000/max_operators: not_a_number/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"Invalid value"*"max_operators"* ]]
}

@test "error: all-digit sink name reports specific error" {
  # Rename the sink to an all-digit name which is reserved
  local f=$(bad_topology 's/name: VOID_SINK/name: 12345/')
  run $NES_CLI -d -t "$f" dump
  [ "$status" -eq 1 ]
  [[ "$output" == *"only-digit names are reserved"* ]]
}

