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
  ls >&3
  run $NES_REPL -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  assert_json_equal '[{"schema":[{"name":"TS","type":"UINT64"}],"source_name":"ENDLESS"}]' "${lines[0]}"
  assert_json_equal '[{"host":"localhost:8080","input_formatter_config":{"ALLOW_COMMAS_IN_STRINGS":true,"FIELD_DELIMITER":",","TUPLE_DELIMITER":"\n","type":"CSV"},"physical_source_id":1,"schema":[{"name":"TS","type":"UINT64"}],"source_config":[{"FLUSH_INTERVAL_MS":10},{"GENERATOR_RATE_CONFIG":"emit_rate 10"},{"GENERATOR_RATE_TYPE":"FIXED"},{"GENERATOR_SCHEMA":"SEQUENCE UINT64 0 10000000 1"},{"MAX_INFLIGHT_BUFFERS":0},{"MAX_RUNTIME_MS":10000000},{"SEED":1},{"STOP_GENERATOR_WHEN_SEQUENCE_FINISHES":"ALL"}],"source_name":"ENDLESS","source_type":"GENERATOR"}]' "${lines[1]}"
  assert_json_equal '[{"format_config":[],"host":"localhost:8080","schema":[{"name":"TS","type":"UINT64"}],"sink_config":[{"ADD_TIMESTAMP":false},{"APPEND":false},{"BACKPRESSURE_LOWER_THRESHOLD":200},{"BACKPRESSURE_UPPER_THRESHOLD":1000},{"FILE_PATH":"out.csv"},{"OUTPUT_FORMAT":"CSV"}],"sink_name":"SOMESINK","sink_type":"FILE"}]' "${lines[2]}"
  assert_json_equal '[]' "${lines[3]}"
  QUERY_ID=$(echo ${lines[4]} | jq -r '.[0].query_id')

  # One global and one local query
  echo "${lines[5]}" | jq -e '(. | length) == 2'
  echo "${lines[5]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[6]}"
  assert_json_contains "[]" "${lines[7]}"
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

  # The six EXPLAIN statements sit at the tail of the JSON output stream.
  local n=${#lines[@]}
  local i_logical_text=$((n - 6))
  local i_optimized_text=$((n - 5))
  local i_distributed_text=$((n - 4))
  local i_logical_visual=$((n - 3))
  local i_optimized_visual=$((n - 2))
  local i_distributed_visual=$((n - 1))

  # `sed` inside extract_explain right-trims each line so VISUAL padding does not need to
  # live as trailing whitespace in the .bats source.
  extract_explain() {
    echo "$1" | jq -j '.[0].explain' | sed 's/[[:space:]]*$//'
  }

  assert_equal "$(extract_explain "${lines[$i_logical_text]}")" "$(cat <<'EOF'
== Initial Logical Plan ==
INLINE_SINK(InlineSink)
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

  assert_equal "$(extract_explain "${lines[$i_optimized_text]}")" "$(cat <<'EOF'
== Optimized Global Plan ==
SINK(VOID)
  Join(INNER_JOIN, ID = ID2)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_distributed_text]}")" "$(cat <<'EOF'
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

  assert_equal "$(extract_explain "${lines[$i_logical_visual]}")" "$(cat <<'EOF'
== Initial Logical Plan ==

                   INLINE_SINK(InlineSink)
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

  assert_equal "$(extract_explain "${lines[$i_optimized_visual]}")" "$(cat <<'EOF'
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

  assert_equal "$(extract_explain "${lines[$i_distributed_visual]}")" "$(cat <<'EOF'
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

