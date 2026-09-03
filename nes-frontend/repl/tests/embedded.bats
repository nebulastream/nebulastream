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

  # SELECT ... INTO creates a query with an auto-assigned integer id.
  # No statement waits for a query to start or stop, so how far the query has got by the time each
  # result is produced depends on machine speed. Only the identity of the query is asserted here.
  # Whether it actually reaches a running and then a stopped state is covered by the on-exit tests.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[4].CreatedQuery[0]')"
  # SHOW QUERIES WHERE id = 1 lists it.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[5].Queries[0][0]')"
  # DROP QUERY WHERE id = 1 reports the query it applied to.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[6].DroppedQueries[0]')"
  # A dropped query stays in the catalog, so the final SHOW still lists it.
  assert_json_contains '{"id":1}' "$(echo "$results" | jq '.[7].Queries[0][0]')"
}

# The workers are this process, so every one of them reports the build the repl was linked with.
@test "show version reports the embedded worker build info" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/show_version.sql
  [ "$status" -eq 0 ]

  results=$(printf '%s' "$output" | jq -s '.')
  versions=$(echo "$results" | jq '.[0].WorkerVersions')
  assert_json_equal '1' "$(echo "$versions" | jq '. | length')"
  [ "$(echo "$versions" | jq -r '.[0].worker')" = "localhost:8080" ]
  [ "$(echo "$versions" | jq -r '.[0].error')" = "null" ]

  version=$(echo "$versions" | jq -r '.[0].version')
  [[ "$(echo "$version" | sed -n '1p')" == "nes-single-node-worker "* ]]
  echo "$version" | grep -q "commit:"
}

@test "launch multiple queries distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/invalid_projection_distributed.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "launch multiple queries" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/invalid_projection.sql
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

@test "EXPLAIN over 2-node topology places sources upstream and join downstream" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/explain_distributed.sql
  [ "$status" -eq 0 ]

  # Each statement produces one pretty-printed result spanning several lines, so an output line index
  # does not identify a statement. Collect the results into an array and index that instead. The six
  # EXPLAIN statements are the last six.
  results=$(printf '%s' "$output" | jq -s '.')
  local n
  n=$(echo "$results" | jq 'length')
  local i_logical_text=$((n - 6))
  local i_optimized_text=$((n - 5))
  local i_distributed_text=$((n - 4))
  local i_logical_visual=$((n - 3))
  local i_optimized_visual=$((n - 2))
  local i_distributed_visual=$((n - 1))

  # `sed` inside extract_explain right-trims each line so VISUAL padding does not need to
  # live as trailing whitespace in the .bats source.
  extract_explain() {
    echo "$results" | jq -j ".[$1].ExplainedQuery" | sed 's/[[:space:]]*$//'
  }

  assert_equal "$(extract_explain "$i_logical_text")" "$(cat <<'EOF'
== Initial Logical Plan ==
ANONYMOUS_SINK(AnonymousSink)
  PROJECTION(fields: [START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2])
    PROJECTION(fields: [*])
      Join(INNER_JOIN, ID = ID2)
        WATERMARK_ASSIGNER(Event time)
          PROJECTION(fields: [*])
            SOURCE(STREAM)
        WATERMARK_ASSIGNER(Event time)
          PROJECTION(fields: [*])
            SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "$i_optimized_text")" "$(cat <<'EOF'
== Optimized Global Plan ==
SINK(VOID)
  Join(INNER_JOIN, ID = ID2)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "$i_distributed_text")" "$(cat <<'EOF'
== Decomposed Plans ==
-- 1 plan(s) on sink-node:8080 --
0:
SINK(VOID)
  Join(INNER_JOIN, ID = ID2)
    SOURCE(NETWORK)
    SOURCE(NETWORK)


-- 2 plan(s) on source-node:8080 --
0:
SINK(NETWORK)
  WATERMARK_ASSIGNER(Event time)
    SOURCE(STREAM)


1:
SINK(NETWORK)
  WATERMARK_ASSIGNER(Event time)
    SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "$i_logical_visual")" "$(cat <<'EOF'
== Initial Logical Plan ==

                ANONYMOUS_SINK(AnonymousSink)
                              │
PROJECTION(fields: [START, END, ID, VALUE, TIMESTAMP, ID2...
                              │
                   PROJECTION(fields: [*])
                              │
                 Join(INNER_JOIN, ID = ID2)
               ┌──────────────┴───────────────┐
WATERMARK_ASSIGNER(Event time) WATERMARK_ASSIGNER(Event time)
               │                              │
   PROJECTION(fields: [*])        PROJECTION(fields: [*])
              ┌┘                             ┌┘
       SOURCE(STREAM)                SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "$i_optimized_visual")" "$(cat <<'EOF'
== Optimized Global Plan ==

                         SINK(VOID)
                              │
                 Join(INNER_JOIN, ID = ID2)
               ┌──────────────┴───────────────┐
WATERMARK_ASSIGNER(Event time) WATERMARK_ASSIGNER(Event time)
              ┌┘                             ┌┘
       SOURCE(STREAM)                SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "$i_distributed_visual")" "$(cat <<'EOF'
== Decomposed Plans ==
-- 1 plan(s) on sink-node:8080 --
0:

          SINK(VOID)
               │
  Join(INNER_JOIN, ID = ID2)
       ┌───────┴───────┐
SOURCE(NETWORK) SOURCE(NETWORK)


-- 2 plan(s) on source-node:8080 --
0:

        SINK(NETWORK)
               │
WATERMARK_ASSIGNER(Event time)
               │
        SOURCE(STREAM)


1:

        SINK(NETWORK)
               │
WATERMARK_ASSIGNER(Event time)
               │
       SOURCE(STREAM2)
EOF
)"
}


# The engine's own task events, read back through an EngineEvents source while a second query keeps the
# engine busy. The number of events an engine produces is not reproducible, so this asserts the invariants
# that hold rather than a fixed result: rows are produced, every started task also completes, and neither
# of the two bounded queues on the way dropped anything.
@test "query engine task events are queryable as a stream" {
  cat > engine_stats.sql <<'SQL'
-- A Generator query, purely to give the engine something to do.
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       4000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 500' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK loadSink(ts UINT64) TYPE File
       SET('load.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The engine's own task events. The source finds the feed of the worker it is placed on.
CREATE LOGICAL SOURCE engineStats(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id VARSIZED,
       pipeline_id UINT64, task_id UINT64, tuples UINT64);
CREATE PHYSICAL SOURCE FOR engineStats TYPE EngineEvents SET('CSV' as INPUT_FORMATTER."TYPE");
CREATE SINK statsSink(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id VARSIZED,
       pipeline_id UINT64, task_id UINT64, tuples UINT64) TYPE File
       SET('stats.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The observer goes first, so that it sees the load query start up.
SELECT event_type, ts_us, thread_id, query_id, pipeline_id, task_id, tuples
       FROM engineStats INTO statsSink;
SELECT ts FROM endless INTO loadSink;
SQL

  # The REPL runs its input and then waits, which is what keeps the queries alive. The statistics query
  # never ends on its own, so the wait is bounded from outside; SIGTERM aborts it through the normal
  # shutdown path, which flushes the sinks. GNU timeout reports 124 for exactly that.
  run timeout -s TERM 15 "$NES_REPL" -d --on-exit=WAIT_FOR_QUERY_TERMINATION --worker enable_task_statistics=true <engine_stats.sql
  [ "$status" -eq 124 ]

  [ -s stats.csv ]
  local starts dones
  starts=$(tail -n +2 stats.csv | cut -d, -f1 | grep -cx TASK_START || true)
  dones=$(tail -n +2 stats.csv | cut -d, -f1 | grep -cx TASK_DONE || true)
  echo "# TASK_START: $starts, TASK_DONE: $dones" >&3

  [ "$starts" -gt 0 ]
  [ "$dones" -ge "$starts" ]
  [ "$((dones - starts))" -le 2 ]

  # Both stages on the way drop rather than block, so a drop means one of them was too small.
  run grep -c "dropped so far" nes-repl.log
  [ "$output" -eq 0 ]
  grep -q "0 rows were dropped" nes-repl.log
}
