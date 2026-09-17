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
  nes_require_env NES_REPL
  nes_require_env NES_SERVER
  nes_require_env NES_TEST_TMP_DIR
  nes_require_executable "$NES_REPL"
  nes_require_executable "$NES_SERVER"
  echo "# Using NES_REPL: $NES_REPL" >&3
  echo "# Using NES_SERVER: $NES_SERVER" >&3
}

teardown_file() {
  nes_offline_teardown_file
}

setup() {
  nes_offline_setup
  nes_server_start --worker-mode embedded
}

teardown() {
  if [ -n "${NES_SERVER_PID:-}" ]; then
    nes_server_stop 10 || true
  fi
}

repl() {
  "$NES_REPL" --coordinator "$NES_SERVER_URL" -f JSON "$@"
}

@test "statements reach the server's coordinator" {
  run repl <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]

  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_equal '8' "$(echo "$results" | jq '. | length')"

  assert_json_contains '{"CreatedLogicalSource":{"name":"ENDLESS","schema":[{"name":"TS"}]}}' "$(echo "$results" | jq '.[0]')"
  assert_json_contains '{"CreatedPhysicalSource":{"logical_source":"ENDLESS","host_addr":"localhost:8080","source_type":"GENERATOR"}}' "$(echo "$results" | jq '.[1]')"
  assert_json_contains '{"CreatedSink":{"name":"SOMESINK","host_addr":"localhost:8080","sink_type":"FILE"}}' "$(echo "$results" | jq '.[2]')"
  assert_json_equal '{"Queries":[]}' "$(echo "$results" | jq '.[3]')"
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[4].CreatedQuery.query')"
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[5].Queries[0].query')"
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[6].DroppedQueries[0]')"
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[7].Queries[0].query')"
}

@test "WAIT_FOR_QUERY_TERMINATION waits for the query the server runs" {
  start_time=$(date +%s)
  run repl --on-exit WAIT_FOR_QUERY_TERMINATION <tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)
  [ "$status" -eq 0 ]

  duration=$((end_time - start_time))
  [ "$duration" -ge 10 ]
}

@test "WAIT_FOR_QUERY_TERMINATION exits cleanly on SIGTERM" {
  "$NES_REPL" --coordinator "$NES_SERVER_URL" -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION \
    <tests/sql-file-tests/good/non_infinite_query.sql >repl.out 2>repl.err 3>&- &
  REPL_PID=$!

  sleep 3

  start_time=$(date +%s)
  kill -TERM "$REPL_PID"
  wait "$REPL_PID" || true
  end_time=$(date +%s)

  duration=$((end_time - start_time))
  [ "$duration" -le 3 ]
  [ "$(jq -s '.[-1].CreatedQuery.query.id' repl.out)" = "1" ]
}

@test "STOP_QUERIES stops the query the server runs on exit" {
  start_time=$(date +%s)
  run repl --on-exit STOP_QUERIES <tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)
  [ "$status" -eq 0 ]

  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]
  run curl -fsS "$NES_SERVER_URL/v1/queries/1?wait=terminated&timeout_ms=5000"
  [ "$status" -eq 0 ]
  [ "$(printf '%s' "$output" | jq -r '.query.state')" = "Stopped" ]
}

@test "a failed statement fails the REPL with the server's error" {
  run repl <tests/sql-file-tests/bad/invalid_projection.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "the server's own flags are rejected together with --coordinator" {
  run repl --db catalog.db <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot be combined with --coordinator"* ]]

  run repl --optimizer join_strategy=HASH_JOIN <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 1 ]
  [[ "$output" == *"cannot be combined with --coordinator"* ]]
}

@test "a coordinator that cannot be reached fails the first statement" {
  run "$NES_REPL" --coordinator http://127.0.0.1:1 -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -ne 0 ]
  grep "cannot reach the coordinator at http://127.0.0.1:1/" nes-repl.log
}
