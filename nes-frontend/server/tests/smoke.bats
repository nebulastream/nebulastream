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
  nes_require_env NES_SERVER
  nes_require_env NES_TEST_TMP_DIR
  nes_require_executable "$NES_SERVER"
  echo "# Using NES_SERVER: $NES_SERVER" >&3
}

teardown_file() {
  nes_offline_teardown_file
}

setup() {
  nes_offline_setup
}

teardown() {
  if [ -n "${NES_SERVER_PID:-}" ]; then
    nes_server_stop 10 || true
  fi
}

http() {
  run nes_http "$@"
  assert_success
  http_status=${output##*$'\n'}
  body=${output%$'\n'*}
}

sql_body() {
  jq -n --arg sql "$1" '{sql: $sql}'
}

statement() {
  http POST /v1/statements "$(sql_body "$1")"
  [ "$http_status" -eq 200 ] || fail "statement failed ($http_status): $body"
}

@test "nes-server reports its version and its usage" {
  run "$NES_SERVER" --version
  assert_success
  assert_line --index 0 --regexp '^nes-server '

  run "$NES_SERVER" --help
  assert_success
  assert_output --partial '--listen'
}

@test "an embedded server registers its own worker" {
  nes_server_start --worker-mode embedded

  http GET /v1/health
  [ "$http_status" -eq 200 ]
  assert_json_equal '{"status":"ok"}' "$body"

  http GET /v1/workers
  [ "$http_status" -eq 200 ]
  assert_json_equal '1' "$(echo "$body" | jq 'length')"
  assert_json_equal '"localhost:8080"' "$(echo "$body" | jq '.[0].host_addr')"

  http GET /v1/workers/versions
  [ "$http_status" -eq 200 ]
  assert_json_equal '1' "$(echo "$body" | jq 'length')"
  [ -n "$(echo "$body" | jq -r '.[0].version // empty')" ]
  assert_json_equal 'null' "$(echo "$body" | jq '.[0].error')"
}

@test "a query runs on the embedded worker to completion" {
  nes_server_start --worker-mode embedded
  printf '1\n2\n3\n' > demo-input.csv

  statement 'CREATE LOGICAL SOURCE demo(value UINT64)'
  statement "CREATE PHYSICAL SOURCE FOR demo TYPE File SET('./demo-input.csv' AS \"SOURCE\".FILE_PATH, 'CSV' AS INPUT_FORMATTER.\"TYPE\")"
  statement "CREATE SINK result(value UINT64) TYPE File SET('./demo-output.csv' AS \"SINK\".FILE_PATH, 'CSV' AS \"SINK\".OUTPUT_FORMAT)"

  http POST '/v1/queries?wait=completed&timeout_ms=20000' "$(sql_body 'SELECT value FROM demo INTO result')"
  if [ "$http_status" -eq 202 ]; then
    id=$(echo "$body" | jq '.query.id')
    http GET "/v1/queries/$id?wait=completed&timeout_ms=20000"
    [ "$http_status" -eq 200 ] || fail "query did not complete ($http_status): $body"
  else
    [ "$http_status" -eq 201 ] || fail "query was not created ($http_status): $body"
  fi
  assert_json_equal '"Completed"' "$(echo "$body" | jq '.query.state')"

  assert_file_line_count demo-output.csv 4
  [ "$(tail -n +2 demo-output.csv | tr '\n' ' ')" = "1 2 3 " ]
}

@test "only a query reaches the query route" {
  nes_server_start --worker-mode embedded

  http POST /v1/queries "$(sql_body 'CREATE LOGICAL SOURCE x(v UINT64)')"
  [ "$http_status" -eq 400 ]
  assert_json_equal '2029' "$(echo "$body" | jq '.code')"

  http POST /v1/queries "$(sql_body 'SELECT value FROM nope INTO result')"
  [ "$http_status" -eq 404 ] || fail "expected the planner's unknown source, got $http_status: $body"
  assert_json_equal '2030' "$(echo "$body" | jq '.code')"
  assert_json_equal '"UnknownSourceName"' "$(echo "$body" | jq '.error')"
}

@test "an unknown optimizer configuration key fails the first planned statement" {
  nes_server_start --worker-mode embedded --optimizer-config '{"test_invalid_optimizer_config_name":"INVALID"}'
  http POST /v1/queries/explain "$(sql_body 'SELECT * FROM stream INTO sink')"
  [ "$http_status" -eq 500 ]
  assert_json_equal '"InvalidConfigParameter"' "$(echo "$body" | jq '.error')"
  [[ "$(echo "$body" | jq -r '.message')" == *"Unrecognized configuration key: 'test_invalid_optimizer_config_name'"* ]]
}

@test "an invalid optimizer configuration value fails the first planned statement" {
  nes_server_start --worker-mode embedded --optimizer-config '{"join_strategy":"INVALID"}'
  http POST /v1/queries/explain "$(sql_body 'SELECT * FROM stream INTO sink')"
  [ "$http_status" -eq 500 ]
  assert_json_equal '"InvalidConfigParameter"' "$(echo "$body" | jq '.error')"
  [[ "$(echo "$body" | jq -r '.message')" == *"Enum for INVALID was not found"* ]]
}

@test "a termination signal stops the server cleanly" {
  nes_server_start --worker-mode embedded
  http GET /v1/health
  [ "$http_status" -eq 200 ]

  code=0
  nes_server_stop || code=$?
  [ "$code" -eq 0 ]
  grep -q "shutting down" nes-server.err
  grep -q "terminated, shutting down" nes-server.log
}
