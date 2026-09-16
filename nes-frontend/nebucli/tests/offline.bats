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

source "$NES_BATS_LIB"

setup_file() {
  nes_require_env NES_CLI
  nes_require_env NES_SERVER
  nes_require_env NES_TEST_TMP_DIR
  nes_require_executable "$NES_CLI"
  nes_require_executable "$NES_SERVER"
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NES_SERVER: $NES_SERVER" >&3
}

teardown_file() {
  nes_offline_teardown_file
}

setup() {
  nes_offline_setup
  nes_server_start --worker-mode remote
}

teardown() {
  nes_server_stop 10 || true
}

cli() {
  "$NES_CLI" --coordinator "$NES_SERVER_URL" "$@"
}

@test "nebucli shows help" {
  run "$NES_CLI" --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"--coordinator"* ]]
}

@test "nebucli shows version" {
  run "$NES_CLI" -v
  [ "$status" -eq 0 ]
  [[ "$output" == *"nes-cli"* ]]
}

@test "nebucli dump" {
  run cli -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump with debug" {
  run cli -d -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment" {
  NES_SETUP_FILE=tests/good/chained-joins.yaml run cli dump
  [ "$status" -eq 0 ]
}

@test "nebucli dump using environment and adhoc query" {
  NES_SETUP_FILE=tests/good/select-gen-into-void.yaml run cli dump 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "nebucli dump with setup from stdin" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI --coordinator $NES_SERVER_URL -s - dump"
  [ "$status" -eq 0 ]
}

@test "nebucli dump with setup from stdin and adhoc query" {
  run bash -c "cat tests/good/select-gen-into-void.yaml | $NES_CLI --coordinator $NES_SERVER_URL -s - dump 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'"
  [ "$status" -eq 0 ]
}

@test "nebucli reads the coordinator from the environment" {
  NES_COORDINATOR="$NES_SERVER_URL" run "$NES_CLI" -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: -s flag takes priority over env and working directory" {
  # Put a valid setup as setup.yaml in cwd — should be ignored
  cp tests/good/select-gen-into-void.yaml setup.yaml
  # Set env to a different valid topology — should be ignored
  NES_SETUP_FILE=tests/good/select-gen-into-void.yaml run cli -s tests/good/chained-joins.yaml dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: NES_SETUP_FILE takes priority over working directory" {
  cp tests/good/select-gen-into-void.yaml setup.yaml
  NES_SETUP_FILE=tests/good/chained-joins.yaml run cli dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yaml in working directory" {
  cp tests/good/chained-joins.yaml setup.yaml
  run cli dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yml in working directory" {
  cp tests/good/chained-joins.yaml setup.yml
  run cli dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: setup.yaml preferred over setup.yml" {
  cp tests/good/chained-joins.yaml setup.yaml
  echo "invalid yaml: [" > setup.yml
  run cli dump
  [ "$status" -eq 0 ]
}

@test "setup resolution: error when no setup found" {
  run cli -d dump
  [ "$status" -eq 1 ]
  grep "no setup file found" nes-cli.log
}

@test "setup resolution: -s with nonexistent file" {
  run cli -d -s nonexistent.yaml dump
  [ "$status" -eq 1 ]
  grep "No such file" nes-cli.log
}

@test "setup resolution: -s - with empty stdin" {
  run bash -c "echo -n '' | $NES_CLI --coordinator $NES_SERVER_URL -d -s - dump"
  [ "$status" -eq 1 ]
  grep "missing field" nes-cli.log
}

@test "setup resolution: -s - reads from stdin" {
  run bash -c "cat tests/good/chained-joins.yaml | $NES_CLI --coordinator $NES_SERVER_URL -s - dump"
  [ "$status" -eq 0 ]
}

@test "topology validation: reject topology with cycle" {
  run cli -d -s tests/good/topology-with-cycle.yaml start
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

  run cli -s setup-no-capacity.yaml dump
  [ "$status" -eq 0 ]
}

@test "the optimizer section of a setup file is ignored with a warning" {
  run cli -s tests/good/backpressure-optimizer-flags.yaml dump
  [ "$status" -eq 0 ]
  [[ "$output" == *"optimizer section is ignored"* ]]
}

@test "yaml parser should reject unknown keys" {
  run cli -d -s tests/bad/invalid_config_with_unknown_keys1.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected one of \`query\`, \`sinks\`, \`logical\`, \`logical_sources\`, \`physical\`, \`physical_sources\`, \`models\`, \`workers\`, \`optimizer\`" nes-cli.log

  run cli -d -s tests/bad/invalid_config_with_unknown_keys2.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected \`name\` or \`schema\`" nes-cli.log

  run cli -d -s tests/bad/invalid_config_with_unknown_keys3.yaml dump
  [ "$status" -eq 1 ]
  grep "unknown field \`idontexist\`, expected one of \`host\`, \`host_addr\`, \`data_addr\`, \`data_address\`, \`max_operators\`, \`downstream\`, \`peers\`, \`config\`" nes-cli.log
}

@test "status of an empty coordinator is an empty list" {
  run --separate-stderr cli status
  [ "$status" -eq 0 ]
  assert_json_equal '[]' "$output"
}

@test "sql runs a statement and prints its result as JSON or as a table" {
  run --separate-stderr cli -o json sql 'CREATE LOGICAL SOURCE stream(value UINT64)'
  [ "$status" -eq 0 ]
  assert_json_contains '{"CreatedLogicalSource":{"name":"STREAM"}}' "$output"

  run --separate-stderr cli -o table sql 'CREATE LOGICAL SOURCE other(value UINT64)'
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == +-* ]]
  [[ "$output" == *"OTHER"* ]]
}

@test "an unreachable coordinator is reported" {
  run "$NES_CLI" --coordinator http://127.0.0.1:1 status
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot reach the coordinator"* ]]
}
