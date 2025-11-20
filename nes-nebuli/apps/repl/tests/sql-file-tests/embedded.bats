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
    echo "ERROR: NEBULI environment variable must be set" >&2
    echo "Usage: NEBULI=/path/to/nebuli bats nebuli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULI_TESTDATA" ]; then
    echo "ERROR: NEBULI_TESTDATA environment variable must be set" >&2
    echo "Usage: NEBULI_TESTDATA=/path/to/nebuli/testdata" >&2
    exit 1
  fi

  if [ -z "$SYSTEST_TESTDATA" ]; then
    echo "ERROR: SYSTEST_TESTDATA environment variable must be set" >&2
    echo "Usage: SYSTEST_TESTDATA=/path/to/nes-systest/testdata" >&2
    exit 1
  fi

  if [ ! -f "$NEBULI" ]; then
    echo "ERROR: NEBULI file does not exist: $NEBULI" >&2
    exit 1
  fi

  if [ ! -x "$NEBULI" ]; then
    echo "ERROR: NEBULI file is not executable: $NEBULI" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NEBULI: $NEBULI" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)

  ln -s "$NEBULI_TESTDATA" "$TMP_DIR"
  ln -s "$SYSTEST_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

@test "nebuli shows help" {
  run $NEBULI --help
  [ "$status" -eq 0 ]
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

@test "basic test" {
  ls >&3
  run $NEBULI -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[0]}"
  assert_json_equal '[{"host":"localhost:9090","parser_config":{"field_delimiter":",","tuple_delimiter":"\n","type":"CSV"},"physical_source_id":1,"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_config":[{"flush_interval_ms":10},{"generator_rate_config":"emit_rate 10"},{"generator_rate_type":"FIXED"},{"generator_schema":"SEQUENCE UINT64 0 10000000 1"},{"max_inflight_buffers":0},{"max_runtime_ms":10000000},{"seed":1},{"stop_generator_when_sequence_finishes":"ALL"}],"source_name":"ENDLESS","source_type":"Generator"}]' "${lines[1]}"
  assert_json_equal '[{"host":"localhost:9090","schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"sink_config":[{"add_timestamp":false},{"append":false},{"file_path":"out.csv"},{"input_format":"CSV"}],"sink_name":"SOMESINK","sink_type":"File"}]' "${lines[2]}"
  assert_json_equal '[]' "${lines[3]}"
  QUERY_ID=$(echo ${lines[4]} | jq -r '.[0].global_query_id')

  # One global and one local query
  echo "${lines[5]}" | jq -e '(. | length) == 2'
  echo "${lines[5]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"global_query_id\":\"${QUERY_ID}\"}]" "${lines[6]}"
  assert_json_contains "[]" "${lines[7]}"
}

@test "launch multiple queries distributed" {
  run $NEBULI -f JSON <tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail distributed" {
  run $NEBULI -f JSON <tests/sql-file-tests/bad/integer_literal_in_query_without_type_distributed.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nebuli.log
}

@test "launch multiple queries" {
  run $NEBULI -f JSON <tests/sql-file-tests/good/multiple_queries.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  run $NEBULI -f JSON <tests/sql-file-tests/bad/integer_literal_in_query_without_type.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nebuli.log
}
