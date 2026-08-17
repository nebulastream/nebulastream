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

@test "show version reports the embedded worker build info" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/show_version.sql
  [ "$status" -eq 0 ]

  [ "$(echo "${lines[0]}" | jq -r '.[0].worker')" = "localhost:8080" ]
  version=$(echo "${lines[0]}" | jq -r '.[0].version')
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
  run timeout -s TERM 15 "$NES_REPL" -d --on-exit=WAIT_FOR_QUERY_TERMINATION -- --enable_task_statistics=true <engine_stats.sql
  [ "$status" -eq 124 ]

  [ -s stats.csv ]
  local starts dones
  starts=$(tail -n +2 stats.csv | cut -d, -f1 | grep -cx TASK_START || true)
  dones=$(tail -n +2 stats.csv | cut -d, -f1 | grep -cx TASK_DONE || true)
  echo "# TASK_START: $starts, TASK_DONE: $dones" >&3

  [ "$starts" -gt 0 ]
  [ "$starts" -eq "$dones" ]

  # Both stages on the way drop rather than block, so a drop means one of them was too small.
  run grep -c "dropped so far" nes-repl.log
  [ "$output" -eq 0 ]
  grep -q "0 rows were dropped" nes-repl.log
}


# The buffer pool's own statistics, read back through a BufferEvents source while a second query puts the
# pool under load. How many buffers a query churns through is not reproducible, so this asserts the
# invariants that hold rather than a fixed result: rows are produced, the pool describes itself, the load
# is visible in both the pooled and the unpooled columns, and no row contradicts the pool's size.
@test "buffer manager events are queryable as a stream" {
  cat > buffer_stats.sql <<'SQL'
-- A Generator query, purely to put the buffer pool under load. The VARSIZED payload is what drives the
-- unpooled columns; a fixed-size schema alone would leave them at zero. It finishes well before the REPL
-- is torn down, so the statistics stream shows the pool under load and afterwards at rest.
CREATE LOGICAL SOURCE endless(ts UINT64, payload VARSIZED);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       8000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 500' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 10000000 1, RANDOMSTR 512 8192' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK loadSink(ts UINT64, length UINT64) TYPE File
       SET('load.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The pool's own statistics. The source finds the feed of the worker it is placed on.
CREATE LOGICAL SOURCE bufferStats(
       ts_us UINT64, interval_ms UINT64, pooled_total UINT64, pooled_available UINT64,
       pooled_available_min UINT64, pooled_acquired UINT64, pooled_recycled UINT64,
       pooled_request_failures UINT64, unpooled_allocated UINT64, unpooled_bytes_requested UINT64,
       unpooled_bytes_in_use UINT64, unpooled_chunks_allocated UINT64, unpooled_chunks_released UINT64,
       unpooled_request_failures UINT64, rows_dropped UINT64);
CREATE PHYSICAL SOURCE FOR bufferStats TYPE BufferEvents SET('CSV' as INPUT_FORMATTER."TYPE");
CREATE SINK statsSink(
       ts_us UINT64, interval_ms UINT64, pooled_total UINT64, pooled_available UINT64,
       pooled_available_min UINT64, pooled_acquired UINT64, pooled_recycled UINT64,
       pooled_request_failures UINT64, unpooled_allocated UINT64, unpooled_bytes_requested UINT64,
       unpooled_bytes_in_use UINT64, unpooled_chunks_allocated UINT64, unpooled_chunks_released UINT64,
       unpooled_request_failures UINT64, rows_dropped UINT64) TYPE File
       SET('buffer_stats.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);

-- The observer goes first, so that it sees the pool before the load query touches it.
SELECT ts_us, interval_ms, pooled_total, pooled_available, pooled_available_min, pooled_acquired,
       pooled_recycled, pooled_request_failures, unpooled_allocated, unpooled_bytes_requested,
       unpooled_bytes_in_use, unpooled_chunks_allocated, unpooled_chunks_released,
       unpooled_request_failures, rows_dropped
       FROM bufferStats INTO statsSink;
SELECT ts, OCTET_LENGTH(payload) as length FROM endless INTO loadSink;
SQL

  # As above: the statistics query never ends on its own, so the wait is bounded from outside and GNU
  # timeout reports 124 for the SIGTERM it sends.
  run timeout -s TERM 15 "$NES_REPL" -d --on-exit=WAIT_FOR_QUERY_TERMINATION \
      -- --enable_buffer_statistics=true --buffer_statistics_interval_ms=50 <buffer_stats.sql
  [ "$status" -eq 124 ]

  [ -s buffer_stats.csv ]
  # Sum and maximum of a 1-based column over the data rows, i.e. everything below the CSV header.
  sum_col() { tail -n +2 buffer_stats.csv | awk -F, -v c="$1" '{ total += $c } END { printf "%d", total + 0 }'; }
  max_col() { tail -n +2 buffer_stats.csv | awk -F, -v c="$1" '{ if ($c > m) m = $c } END { printf "%d", m + 0 }'; }

  local rows pool acquired recycled unpooled chunks_allocated chunks_released overshoot
  rows=$(tail -n +2 buffer_stats.csv | wc -l)
  pool=$(max_col 3)
  acquired=$(sum_col 6)
  recycled=$(sum_col 7)
  unpooled=$(sum_col 9)
  chunks_allocated=$(sum_col 12)
  chunks_released=$(sum_col 13)
  # A row whose reported fill level exceeds the pool size would mean the gauges are wrong.
  overshoot=$(tail -n +2 buffer_stats.csv | awk -F, -v t="$pool" '($4 > t) || ($5 > t) { n++ } END { printf "%d", n + 0 }')
  echo "# rows: $rows, pool: $pool, acquired: $acquired, recycled: $recycled, unpooled: $unpooled" >&3

  [ "$rows" -gt 0 ]
  [ "$pool" -gt 0 ]
  [ "$acquired" -gt 0 ]
  # The VARSIZED payload does not fit a pooled buffer, so it has to show up in the unpooled columns.
  [ "$unpooled" -gt 0 ]
  [ "$chunks_allocated" -gt 0 ]
  [ "$overshoot" -eq 0 ]
  # Every acquired buffer is eventually recycled, but one acquired in the last interval before shutdown is
  # recycled after the final row, so the two totals only have to agree within the pool size.
  [ "$((acquired - recycled))" -le "$pool" ]
  [ "$((recycled - acquired))" -le "$pool" ]
  # That a chunk release is reported for every allocation is asserted deterministically in
  # BufferManagerEventTest; here it can only be bounded, for the same reason as the acquire/recycle pair.
  [ "$chunks_released" -le "$chunks_allocated" ]

  # The feed drops rather than blocks, so a drop means it was too small for the row rate.
  grep -q "Closing the BufferEvents source .* 0 rows were dropped" nes-repl.log
}
