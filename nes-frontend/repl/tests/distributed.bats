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

docker_nes_repl() {
  # In Docker-out-of-Docker environments, docker exec tears down the exec
  # session when its stdin pipe closes, even if the process is still running.
  # Work around this by: (1) piping from tail to keep stdin open until
  # nes-repl exits, and (2) reading the SQL file from the volume inside the
  # container instead of piping it via stdin.
  tail -f /dev/null | docker compose exec nes-repl bash -c "nes-repl -f JSON ${ADDITIONAL_NEBULI_FLAGS} </workdir/$1"
}

@test "launch query from topology" {
  setup_distributed tests/topologies/8-node.yaml
  run docker_nes_repl tests/sql-file-tests/good/test_large_distributed.sql
  [ "$status" -eq 0 ]

  # The REPL prints one pretty-printed StatementResult per statement; slurp the whole
  # stream into an array so each result can be addressed by index.
  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_equal '9' "$(echo "$results" | jq '. | length')"

  assert_json_contains '{"CreatedWorker":{"host_addr":"sink-node:8080"}}' "$(echo "$results" | jq '.[0]')"
  assert_json_contains '{"CreatedLogicalSource":{"name":"ENDLESS","schema":[{"name":"TS"}]}}' "$(echo "$results" | jq '.[1]')"
  assert_json_contains '{"CreatedPhysicalSource":{"logical_source":"ENDLESS","host_addr":"sink-node:8080","source_type":"GENERATOR"}}' "$(echo "$results" | jq '.[2]')"
  assert_json_contains '{"CreatedSink":{"name":"SOMESINK","host_addr":"sink-node:8080","sink_type":"FILE"}}' "$(echo "$results" | jq '.[3]')"
  # SHOW QUERIES before any query is installed.
  assert_json_equal '{"Queries":[]}' "$(echo "$results" | jq '.[4]')"
  # SELECT ... INTO creates a query with an auto-assigned integer id.
  # No statement waits for a query to start or stop, so how far the query has got by the time each
  # result is produced depends on machine speed. Only the identity of the query is asserted here.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[5].CreatedQuery[0]')"
  # SHOW QUERIES WHERE id = 1 lists it.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[6].Queries[0][0]')"
  # DROP QUERY WHERE id = 1 reports the query it applied to.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[7].DroppedQueries[0]')"
  # A dropped query stays in the catalog, so the final SHOW still lists it.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[8].Queries[0][0]')"
}

@test "launch multiple queries" {
  setup_distributed tests/topologies/1-node.yaml
  run docker_nes_repl tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]

  # One CREATE WORKER, one logical and one physical source, then four SELECTs, each of which
  # installs a query.
  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_equal '7' "$(echo "$results" | jq '. | length')"
  assert_json_equal '4' "$(echo "$results" | jq '[.[] | select(has("CreatedQuery"))] | length')"
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[3].CreatedQuery[0]')"
}

@test "quoted identifiers and uppercase compatibility survive distributed repl" {
  setup_distributed tests/topologies/1-node.yaml
  run docker_nes_repl tests/sql-file-tests/good/quoted_identifiers_distributed.sql
  [ "$status" -eq 0 ]

  assert_json_contains \
    '[{"source_name":"quotedSource","schema":[{"name":"mixedValue","type":"UINT64"},{"name":"A","type":"UINT64"}]}]' \
    "${lines[1]}"
  assert_json_contains \
    '[{"source_name":"quotedSource","schema":[{"name":"mixedValue","type":"UINT64"},{"name":"A","type":"UINT64"}]}]' \
    "${lines[2]}"
  assert_json_contains \
    '[{"sink_name":"quotedSink","schema":[{"name":"projectedValue","type":"UINT64"},{"name":"A","type":"UINT64"}]}]' \
    "${lines[3]}"
  echo "${lines[4]}" | jq -e '.[0].query_id | length > 0'
}

#bats test_tags=INFERENCE
@test "create model show and drop lifecycle" {
  setup_distributed tests/topologies/1-node.yaml
  run docker_nes_repl tests/sql-file-tests/good/create_model.sql
  [ "$status" -eq 0 ]

  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_equal '4' "$(echo "$results" | jq '. | length')"

  # CREATE MODEL is eagerly loaded and returns full metadata.
  assert_json_contains '{"CreatedMlModel":{"name":"TESTMODEL","input_schema":[{"name":"F1"}],"output_schema":[{"name":"O1"}]}}' "$(echo "$results" | jq '.[0]')"
  echo "$results" | jq -e '.[0].CreatedMlModel.path | test("tiny_identity.onnx")'
  # SHOW MODELS lists the same model.
  assert_json_contains '{"MlModels":[{"name":"TESTMODEL"}]}' "$(echo "$results" | jq '.[1]')"
  echo "$results" | jq -e '.[1].MlModels[0].path | test("tiny_identity.onnx")'
  # DROP MODEL reports the dropped model.
  assert_json_contains '{"DroppedMlModels":[{"name":"TESTMODEL"}]}' "$(echo "$results" | jq '.[2]')"
  # SHOW MODELS after the drop is empty.
  assert_json_equal '{"MlModels":[]}' "$(echo "$results" | jq '.[3]')"
}

# The worker is a process of its own, so the coordinator asks it over the network and the answer has
# to be the same build that worker reports about itself.
@test "show version matches the worker's own --version output" {
  setup_distributed tests/topologies/1-node.yaml

  expected=$(docker compose exec -T worker-node nes-single-node-worker --version)

  run docker_nes_repl tests/sql-file-tests/good/show_version_distributed.sql
  [ "$status" -eq 0 ]

  results=$(printf '%s' "$output" | jq -s '.')
  versions=$(echo "$results" | jq '.[1].WorkerVersions')
  [ "$(echo "$versions" | jq -r '.[0].worker')" = "worker-node:8080" ]
  [ "$(echo "$versions" | jq -r '.[0].version')" = "$expected" ]
  [ "$(echo "$versions" | jq -r '.[0].error')" = "null" ]
}

# One worker that cannot be reached reports why rather than failing the statement, so a fleet with a
# dead node still answers for the rest.
@test "show version reports an unreachable worker as an error" {
  setup_distributed tests/topologies/1-node.yaml
  docker compose stop worker-node

  run docker_nes_repl tests/sql-file-tests/good/show_version_distributed.sql
  [ "$status" -eq 0 ]

  results=$(printf '%s' "$output" | jq -s '.')
  versions=$(echo "$results" | jq '.[1].WorkerVersions')
  [ "$(echo "$versions" | jq -r '.[0].worker')" = "worker-node:8080" ]
  [ "$(echo "$versions" | jq -r '.[0].version')" = "null" ]
  [ "$(echo "$versions" | jq -r '.[0].error')" != "null" ]
}

@test "launch bad query should fail" {
  setup_distributed tests/topologies/1-node.yaml
  run docker_nes_repl tests/sql-file-tests/bad/invalid_projection_distributed.sql
  [ "$status" -ne 0 ]

  sync_workdir
  grep "invalid query syntax" nes-repl.log
}

@test "launch query and wait for query termination on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit WAIT_FOR_QUERY_TERMINATION" run docker_nes_repl tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to not terminate while the query is still running due to the WAIT_FOR_QUERY_TERMINATION option
  duration=$((end_time - start_time))
  [ "$duration" -ge 10 ]
}

@test "WAIT_FOR_QUERY_TERMINATION exits cleanly on SIGTERM" {
  setup_distributed tests/topologies/1-node.yaml

  # non_infinite_query.sql configures the source to produce data for 10000ms,
  # so WAIT_FOR_QUERY_TERMINATION would normally make nes-repl block ~10s.
  # Send SIGTERM to nes-repl mid-wait and verify the on-exit loop exits well
  # before the 10s mark and reports the warning.
  (
    tail -f /dev/null | docker compose exec -T nes-repl bash -c \
      "nes-repl -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION </workdir/tests/sql-file-tests/good/non_infinite_query.sql"
  ) &
  REPL_BG=$!

  # Give the REPL time to deploy the query and enter the on-exit wait loop.
  sleep 3

  start_time=$(date +%s)
  docker compose exec -T nes-repl pkill -TERM -f "^nes-repl"
  wait $REPL_BG
  end_time=$(date +%s)

  duration=$((end_time - start_time))
  # SIGTERM should break the wait loop within a couple of polling intervals,
  # well before the query's natural 10s end.
  [ "$duration" -le 3 ]
}

@test "launch query and terminate query on exit behavior" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  ADDITIONAL_NEBULI_FLAGS="--on-exit STOP_QUERIES" run docker_nes_repl tests/sql-file-tests/good/non_infinite_query.sql
  end_time=$(date +%s)

  [ "$status" -eq 0 ]

  # The query is configured to produce data for 10000ms. We expect nes-repl to terminate within 5 seconds as it is configured to terminate all pending queries on exit
  duration=$((end_time - start_time))
  [ "$duration" -le 5 ]

  # The SELECT installs a query. STOP_QUERIES issues an implicit DROP QUERY on exit, but that on-exit
  # result is a frontend action and is not emitted to stdout, so the fast exit above is what proves it
  # ran; here we only assert the query was created.
  results=$(printf '%s' "$output" | jq -s '.')
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[3].CreatedQuery[0]')"
}

@test "default on-exit behavior should keep queries alive" {
  setup_distributed tests/topologies/1-node.yaml

  start_time=$(date +%s)
  run docker_nes_repl tests/sql-file-tests/good/multiple_queries_distributed.sql
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
