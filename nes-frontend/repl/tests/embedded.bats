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

NES_REPL="$NES_REPL_EMBEDDED"

setup_file()    { nes_offline_setup_file; }
teardown_file() { nes_offline_teardown_file; }
setup()         { nes_offline_setup; }

@test "nes-repl shows help" {
  run $NES_REPL --help
  [ "$status" -eq 0 ]
}

@test "basic test" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]

  # In JSON mode the REPL prints one pretty-printed StatementResult per statement;
  # slurp the whole stream into an array so we can address each result by index.
  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_equal '8' "$(echo "$results" | jq '. | length')"

  assert_json_contains '{"CreatedLogicalSource":{"name":"ENDLESS","schema":[{"name":"TS"}]}}' "$(echo "$results" | jq '.[0]')"
  assert_json_contains '{"CreatedPhysicalSource":{"logical_source":"ENDLESS","host_addr":"localhost:8080","source_type":"GENERATOR"}}' "$(echo "$results" | jq '.[1]')"
  assert_json_contains '{"CreatedSink":{"name":"SOMESINK","host_addr":"localhost:8080","sink_type":"FILE"}}' "$(echo "$results" | jq '.[2]')"
  assert_json_equal '{"Queries":[]}' "$(echo "$results" | jq '.[3]')"

  # SELECT ... INTO creates a query with an auto-assigned integer id. submit_sql blocks until it is
  # running, so the result reports the running query rather than the transient pending one.
  assert_json_contains '{"id":1,"state":"Running"}' "$(echo "$results" | jq '.[4].CreatedQuery[0]')"
  # SHOW QUERIES WHERE id = 1 lists the running query.
  assert_json_contains '{"id":1,"state":"Running"}' "$(echo "$results" | jq '.[5].Queries[0][0]')"
  # DROP QUERY WHERE id = 1 blocks until the query has stopped.
  assert_json_contains '{"id":1,"state":"Stopped"}' "$(echo "$results" | jq '.[6].DroppedQueries[0]')"
  # A stopped query stays in the catalog, so the final SHOW still lists it, now in the Stopped state.
  assert_json_contains '{"id":1,"state":"Stopped"}' "$(echo "$results" | jq '.[7].Queries[0][0]')"
}

@test "launch multiple queries distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/integer_literal_in_query_without_type_distributed.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "launch multiple queries" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/integer_literal_in_query_without_type.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "Fail on invalid optimizer config name" {
  run $NES_REPL --optimizer test_invalid_config_name=INVALID
  [ "$status" -ne 0 ]
  grep "invalid config parameter; Unrecognized configuration key: 'test_invalid_config_name'" nes-repl.log
}

@test "Fail on invalid optimizer config value" {
  run $NES_REPL --optimizer join_strategy=INVALID
  [ "$status" -ne 0 ]
  grep "invalid config parameter; Enum for INVALID was not found." nes-repl.log
}
