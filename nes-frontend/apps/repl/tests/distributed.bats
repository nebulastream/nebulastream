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

setup_file()    { nes_distributed_setup_file "$NES_REPL"; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

DOCKER_NES_REPL() {
  # In Docker-out-of-Docker environments, docker exec tears down the exec
  # session when its stdin pipe closes, even if the process is still running.
  # Work around this by: (1) piping from tail to keep stdin open until
  # nes-repl exits, and (2) reading the SQL file from the volume inside the
  # container instead of piping it via stdin.
  tail -f /dev/null | docker compose exec nes-repl bash -c "nes-repl -f JSON ${ADDITIONAL_NEBULI_FLAGS} </workdir/$1"
}

@test "launch query from topology" {
  setup_distributed tests/topologies/8-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/good/test_large_distributed.sql
  [ "$status" -eq 0 ]

  assert_json_equal '[{"worker":"sink-node:8080"}]' "${lines[0]}"
  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[1]}"
  assert_json_equal '[{"host":"sink-node:8080","parser_config":{"field_delimiter":",","tuple_delimiter":"\n","type":"CSV"},"physical_source_id":1,"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_config":[{"flush_interval_ms":10},{"generator_rate_config":"emit_rate 10"},{"generator_rate_type":"FIXED"},{"generator_schema":"SEQUENCE UINT64 0 10000000 1"},{"max_inflight_buffers":0},{"max_runtime_ms":10000000},{"seed":1},{"stop_generator_when_sequence_finishes":"ALL"}],"source_name":"ENDLESS","source_type":"Generator"}]' "${lines[2]}"
  assert_json_equal '[{"format_config":{},"host":"sink-node:8080","schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"sink_config":[{"add_timestamp":false},{"append":false},{"file_path":"out.csv"},{"output_format":"CSV"}],"sink_name":"SOMESINK","sink_type":"File"}]' "${lines[3]}"
  assert_json_equal '[]' "${lines[4]}"
  QUERY_ID=$(echo ${lines[5]} | jq -r '.[0].query_id')

  # One global and one local query
  echo "${lines[6]}" | jq -e '(. | length) == 2'
  echo "${lines[6]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[7]}"
  assert_json_contains "[]" "${lines[8]}"
}

@test "launch multiple queries" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "create model show and drop lifecycle" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/good/create_model.sql
  [ "$status" -eq 0 ]

  # lines[0]: CREATE MODEL result — eagerly loaded, returns full metadata
  assert_json_contains '[{"model_name":"TESTMODEL","input_schema":[[{"name":"F1","type":"VARSIZED"}]],"output_schema":[[{"name":"O1","type":"VARSIZED"}]]}]' "${lines[0]}"
  echo "${lines[0]}" | jq -e '.[0].path | test("tiny_identity.onnx")'
  # lines[1]: SHOW MODELS — should contain the same model metadata
  assert_json_contains '[{"model_name":"TESTMODEL","input_schema":[[{"name":"F1","type":"VARSIZED"}]],"output_schema":[[{"name":"O1","type":"VARSIZED"}]]}]' "${lines[1]}"
  echo "${lines[1]}" | jq -e '.[0].path | test("tiny_identity.onnx")'
  # lines[2]: DROP MODEL result
  assert_json_equal '[{"model_name":"TESTMODEL"}]' "${lines[2]}"
  # lines[3]: SHOW MODELS after drop — should be empty
  assert_json_equal '[]' "${lines[3]}"
}

@test "launch bad query should fail" {
  setup_distributed tests/topologies/1-node.yaml
  run DOCKER_NES_REPL tests/sql-file-tests/bad/integer_literal_in_query_without_type_distributed.sql
  [ "$status" -ne 0 ]

  sync_workdir
  grep "invalid query syntax" nes-repl.log
}

@test "launch query and wait for query termination on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit WAIT_FOR_QUERY_TERMINATION" run DOCKER_NES_REPL tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to not terminate while the query is still running due to the WAIT_FOR_QUERY_TERMINATION option
  duration=$((end_time - start_time))
  [ "$duration" -ge 10 ]
}

@test "launch query and terminate query on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit STOP_QUERIES" run DOCKER_NES_REPL tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to terminate within 5 seconds as it is configured to terminate all pending queries on exit
  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]

  # Verify that the implicit STOP QUERY statement was executed and its result matches the query that was created
  # lines[2] is the SELECT query result with query_id
  QUERY_ID=$(echo ${lines[3]} | jq -r '.[0].query_id')
  # The last line should contain the result of the implicit STOP QUERY command with the same query_id
  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[-1]}"
}

@test "default on-exit behavior should keep queries alive" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  run DOCKER_NES_REPL tests/sql-file-tests/good/multiple_queries_distributed.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]
  # The query source runs for 30s. We expect nes-repl to terminate well before that as it is configured to exit regardless of pending queries.
  # We allow up to 5 seconds to account for Docker overhead and date +%s granularity.
  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]

  # Check the log to ensure that the query has been started but not stopped.
  # The source may take a moment to start after the REPL exits, so retry
  # sync_workdir + grep for up to 10 seconds to avoid a race condition.
  local found=false
  for i in $(seq 1 10); do
    sleep 1
    sync_workdir
    if grep -q "Starting source with originId" worker-node/singleNodeWorker.log 2>/dev/null; then
      found=true
      break
    fi
  done
  [ "$found" = true ]
  ! grep "attempting to stop source" worker-node/singleNodeWorker.log
}
