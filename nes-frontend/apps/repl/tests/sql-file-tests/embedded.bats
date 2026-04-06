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
  if [ -z "$NES_REPL" ]; then
    echo "ERROR: NES_REPL environment variable must be set" >&2
    echo "Usage: NES_REPL=/path/to/nes-repl bats nes-repl.bats" >&2
    exit 1
  fi

  if [ -z "$NES_REPL_TESTDATA" ]; then
    echo "ERROR: NES_REPL_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_REPL_TESTDATA=/path/to/nes-repl/testdata" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    echo "Usage: NES_TEST_TMP_DIR=/path/to/tmp" >&2
    exit 1
  fi

  if [ -z "$SYSTEST_TESTDATA" ]; then
    echo "ERROR: SYSTEST_TESTDATA environment variable must be set" >&2
    echo "Usage: SYSTEST_TESTDATA=/path/to/nes-systest/testdata" >&2
    exit 1
  fi

  if [ ! -f "$NES_REPL" ]; then
    echo "ERROR: NES_REPL file does not exist: $NES_REPL" >&2
    exit 1
  fi

  if [ ! -x "$NES_REPL" ]; then
    echo "ERROR: NES_REPL file is not executable: $NES_REPL" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NES_REPL: $NES_REPL" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")

  ln -s "$NES_REPL_TESTDATA" "$TMP_DIR"
  ln -s "$SYSTEST_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

@test "nes-repl shows help" {
  run $NES_REPL --help
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
  run $NES_REPL -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[0]}"
  assert_json_equal '[{"host":"localhost:8080","parser_config":{"field_delimiter":",","tuple_delimiter":"\n","type":"CSV"},"physical_source_id":1,"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_config":[{"flush_interval_ms":10},{"generator_rate_config":"emit_rate 10"},{"generator_rate_type":"FIXED"},{"generator_schema":"SEQUENCE UINT64 0 10000000 1"},{"max_inflight_buffers":0},{"max_runtime_ms":10000000},{"seed":1},{"stop_generator_when_sequence_finishes":"ALL"}],"source_name":"ENDLESS","source_type":"Generator"}]' "${lines[1]}"
  assert_json_equal '[{"format_config":{},"host":"localhost:8080","schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"sink_config":[{"add_timestamp":false},{"append":false},{"file_path":"out.csv"},{"output_format":"CSV"}],"sink_name":"SOMESINK","sink_type":"File"}]' "${lines[2]}"
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

@test "unix socket sink lifecycle" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/unix_socket_sink.sql
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  # Line 0: CREATE LOGICAL SOURCE
  assert_json_equal '[{"schema":[[{"name":"ENDLESS$TS","type":"UINT64"}]],"source_name":"ENDLESS"}]' "${lines[0]}"

  # Line 2: CREATE SINK — verify it's a UnixSocket sink with the right config
  echo "${lines[2]}" | jq -e '.[0].sink_type == "UnixSocket"'
  echo "${lines[2]}" | jq -e '.[0].sink_name == "SOCKETSINK"'

  # Line 3: SHOW QUERIES — initially empty
  assert_json_equal '[]' "${lines[3]}"

  # Line 4: SELECT — query submitted
  QUERY_ID=$(echo ${lines[4]} | jq -r '.[0].query_id')
  [ "$QUERY_ID" = "a1041672-d4c9-40ac-a2b7-6b11342764b4" ]

  # Line 5: SHOW QUERIES WHERE — query is running
  echo "${lines[5]}" | jq -e '(. | length) == 2'
  echo "${lines[5]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  # Line 6: DROP QUERY
  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[6]}"

  # Line 7: SHOW QUERIES — empty after drop
  assert_json_contains "[]" "${lines[7]}"
}

@test "unix socket sink data flow" {
  SOCK_PATH="$TMP_DIR/nes-data-flow.sock"
  SOCKET_OUTPUT="$TMP_DIR/socket_output.txt"

  # Generate SQL with the test-specific socket path
  cat > "$TMP_DIR/socket_e2e.sql" <<EOSQL
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as \`SOURCE\`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as PARSER.\`TYPE\`,
       10000000 AS \`SOURCE\`.MAX_RUNTIME_MS,
       'emit_rate 10' AS \`SOURCE\`.GENERATOR_RATE_CONFIG,
       1 AS \`SOURCE\`.SEED,
       'SEQUENCE UINT64 0 10000000 1' AS \`SOURCE\`.GENERATOR_SCHEMA);
CREATE SINK socketSink(ENDLESS.TS UINT64) TYPE UnixSocket SET('$SOCK_PATH' as \`SINK\`.SOCKET_PATH, 'CSV' as \`SINK\`.OUTPUT_FORMAT);
SELECT TS FROM ENDLESS INTO SOCKETSINK SET ('b2041672-d4c9-40ac-a2b7-6b11342764b4' AS \`QUERY\`.\`ID\`);
SHOW QUERIES WHERE id = 'b2041672-d4c9-40ac-a2b7-6b11342764b4';
EOSQL

  # Start a background Python reader that waits for the socket and reads data
  python3 -c "
import socket, time, os, sys
sock_path = '$SOCK_PATH'
out_path = '$SOCKET_OUTPUT'
for _ in range(80):
    if os.path.exists(sock_path):
        break
    time.sleep(0.1)
else:
    sys.exit(1)
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(sock_path)
data = b''
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
except socket.timeout:
    pass
s.close()
with open(out_path, 'wb') as f:
    f.write(data)
" &
  READER_PID=$!

  # Feed the SQL but keep stdin open long enough for the reader to get data.
  # After the SQL, sleep a bit then send DROP to terminate the query cleanly.
  (
    cat "$TMP_DIR/socket_e2e.sql"
    sleep 4
    echo "DROP QUERY WHERE id = 'b2041672-d4c9-40ac-a2b7-6b11342764b4';"
  ) | $NES_REPL -f JSON > /dev/null 2>&1
  REPL_STATUS=$?

  wait $READER_PID 2>/dev/null || true

  [ "$REPL_STATUS" -eq 0 ]
  # Verify the socket output file has data (schema header + tuples)
  [ -s "$SOCKET_OUTPUT" ]
}

@test "unix socket sink no reader attached" {
  # Verify the query runs and drops cleanly even when nobody connects to the socket.
  SOCK_PATH="$TMP_DIR/nes-no-reader.sock"

  cat > "$TMP_DIR/socket_no_reader.sql" <<EOSQL
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as \`SOURCE\`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as PARSER.\`TYPE\`,
       10000000 AS \`SOURCE\`.MAX_RUNTIME_MS,
       'emit_rate 10' AS \`SOURCE\`.GENERATOR_RATE_CONFIG,
       1 AS \`SOURCE\`.SEED,
       'SEQUENCE UINT64 0 10000000 1' AS \`SOURCE\`.GENERATOR_SCHEMA);
CREATE SINK socketSink(ENDLESS.TS UINT64) TYPE UnixSocket SET('$SOCK_PATH' as \`SINK\`.SOCKET_PATH, 'CSV' as \`SINK\`.OUTPUT_FORMAT);
SELECT TS FROM ENDLESS INTO SOCKETSINK SET ('c1041672-d4c9-40ac-a2b7-6b11342764b4' AS \`QUERY\`.\`ID\`);
SHOW QUERIES WHERE id = 'c1041672-d4c9-40ac-a2b7-6b11342764b4';
EOSQL

  # Run query for 3 seconds with no reader, then drop it
  (
    cat "$TMP_DIR/socket_no_reader.sql"
    sleep 3
    echo "DROP QUERY WHERE id = 'c1041672-d4c9-40ac-a2b7-6b11342764b4';"
  ) | $NES_REPL -f JSON > "$TMP_DIR/no_reader_out.txt" 2>&1
  REPL_STATUS=$?

  [ "$REPL_STATUS" -eq 0 ]
  # The socket should have been cleaned up after the query dropped
  [ ! -S "$SOCK_PATH" ]
}

@test "unix socket sink reader disconnects midway" {
  # Verify the query keeps running after a reader connects and then disconnects.
  SOCK_PATH="$TMP_DIR/nes-reader-disconnect.sock"
  SOCKET_OUTPUT="$TMP_DIR/socket_disconnect_output.txt"

  cat > "$TMP_DIR/socket_disconnect.sql" <<EOSQL
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as \`SOURCE\`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as PARSER.\`TYPE\`,
       10000000 AS \`SOURCE\`.MAX_RUNTIME_MS,
       'emit_rate 10' AS \`SOURCE\`.GENERATOR_RATE_CONFIG,
       1 AS \`SOURCE\`.SEED,
       'SEQUENCE UINT64 0 10000000 1' AS \`SOURCE\`.GENERATOR_SCHEMA);
CREATE SINK socketSink(ENDLESS.TS UINT64) TYPE UnixSocket SET('$SOCK_PATH' as \`SINK\`.SOCKET_PATH, 'CSV' as \`SINK\`.OUTPUT_FORMAT);
SELECT TS FROM ENDLESS INTO SOCKETSINK SET ('d1141672-d4c9-40ac-a2b7-6b11342764b4' AS \`QUERY\`.\`ID\`);
SHOW QUERIES WHERE id = 'd1141672-d4c9-40ac-a2b7-6b11342764b4';
EOSQL

  # Reader that connects, reads briefly, then disconnects
  python3 -c "
import socket, time, os, sys
sock_path = '$SOCK_PATH'
out_path = '$SOCKET_OUTPUT'
for _ in range(80):
    if os.path.exists(sock_path):
        break
    time.sleep(0.1)
else:
    sys.exit(1)
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(2)
s.connect(sock_path)
data = b''
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(data) > 100:
            break  # Got some data, disconnect early
except socket.timeout:
    pass
s.close()
with open(out_path, 'wb') as f:
    f.write(data)
" &
  READER_PID=$!

  # Feed SQL, let query run past the reader disconnect, then verify query is still running before drop
  (
    cat "$TMP_DIR/socket_disconnect.sql"
    sleep 5
    echo "SHOW QUERIES WHERE id = 'd1141672-d4c9-40ac-a2b7-6b11342764b4';"
    sleep 1
    echo "DROP QUERY WHERE id = 'd1141672-d4c9-40ac-a2b7-6b11342764b4';"
  ) | $NES_REPL -f JSON > "$TMP_DIR/disconnect_out.txt" 2>&1
  REPL_STATUS=$?

  wait $READER_PID 2>/dev/null || true

  [ "$REPL_STATUS" -eq 0 ]
  # Reader got some data before disconnecting
  [ -s "$SOCKET_OUTPUT" ]
  # Query was still running after the reader disconnected (second SHOW QUERIES output)
  # The output has: source, phys_source, sink, query_id, show_running, show_still_running, drop
  LAST_SHOW=$(tail -2 "$TMP_DIR/disconnect_out.txt" | head -1)
  echo "$LAST_SHOW" | jq -e '.[].query_status | test("^Running|Registered|Started$")'
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
