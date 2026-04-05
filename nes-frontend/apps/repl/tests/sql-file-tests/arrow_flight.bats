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

FLIGHT_SERVER_SCRIPT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/util/flight_server.py"

setup_file() {
  if [ -z "$NES_REPL" ]; then
    echo "ERROR: NES_REPL environment variable must be set" >&2
    exit 1
  fi

  if [ ! -x "$NES_REPL" ]; then
    echo "ERROR: NES_REPL is not executable: $NES_REPL" >&2
    exit 1
  fi

  echo "# Using NES_REPL: $NES_REPL" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)
  cd "$TMP_DIR" || exit
}

teardown() {
  # Kill the flight server if still running
  if [ -n "$FLIGHT_PID" ] && kill -0 "$FLIGHT_PID" 2>/dev/null; then
    kill "$FLIGHT_PID" 2>/dev/null || true
    wait "$FLIGHT_PID" 2>/dev/null || true
  fi
}

@test "Arrow Flight sink receives data from generator source" {
  # Start the Flight server in the background.
  # It prints FLIGHT_PORT=<port> on the first line, then JSON summary on shutdown.
  FLIGHT_OUTPUT="$TMP_DIR/flight_output.txt"
  "$FLIGHT_SERVER_SCRIPT" --timeout 30 > "$FLIGHT_OUTPUT" 2>"$TMP_DIR/flight_stderr.txt" &
  FLIGHT_PID=$!

  # Wait for the server to print its port
  for i in $(seq 1 30); do
    if head -1 "$FLIGHT_OUTPUT" 2>/dev/null | grep -q "FLIGHT_PORT="; then
      break
    fi
    sleep 0.2
  done

  FLIGHT_PORT=$(head -1 "$FLIGHT_OUTPUT" | sed 's/FLIGHT_PORT=//')
  echo "# Flight server on port $FLIGHT_PORT (pid $FLIGHT_PID)" >&3
  [ -n "$FLIGHT_PORT" ] || { echo "Flight server did not start"; cat "$TMP_DIR/flight_stderr.txt" >&3; false; }

  # Create the SQL file with the actual Flight endpoint substituted
  FLIGHT_ENDPOINT="grpc://localhost:$FLIGHT_PORT"
  sed "s|FLIGHT_ENDPOINT|$FLIGHT_ENDPOINT|g" \
    "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/good/arrow_flight_sink.sql" \
    > "$TMP_DIR/query.sql"

  echo "# Running query with endpoint $FLIGHT_ENDPOINT" >&3
  cat "$TMP_DIR/query.sql" >&3

  # Run the embedded REPL — wait for the generator to finish (10s max_runtime)
  run timeout 30 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/query.sql"
  echo "# REPL exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# REPL: $line" >&3
  done
  [ "$status" -eq 0 ] || [ "$status" -eq 232 ]

  # Give the Flight server a moment to process any in-flight RPCs, then shut it down
  sleep 1
  kill "$FLIGHT_PID" 2>/dev/null || true
  wait "$FLIGHT_PID" 2>/dev/null || true

  # The last line of flight_output is the JSON summary (first line is FLIGHT_PORT=...)
  SUMMARY=$(tail -1 "$FLIGHT_OUTPUT")
  echo "# Raw flight output:" >&3
  cat "$FLIGHT_OUTPUT" >&3
  echo "# Flight server summary: $SUMMARY" >&3

  # Verify we received data
  TOTAL_ROWS=$(echo "$SUMMARY" | jq -r '.total_rows')
  TOTAL_BATCHES=$(echo "$SUMMARY" | jq -r '.total_batches')
  ALL_VALID=$(echo "$SUMMARY" | jq -r '.all_valid')

  echo "# Received $TOTAL_ROWS rows in $TOTAL_BATCHES batches" >&3

  [ "$TOTAL_ROWS" -gt 0 ]
  [ "$TOTAL_BATCHES" -gt 0 ]

  # Print per-stream validation details
  VALIDATIONS=$(echo "$SUMMARY" | jq '.streams[0].validations')
  echo "# Validations: $VALIDATIONS" >&3

  # Check non-nullable columns are correct
  NON_NULL_VALID=$(echo "$VALIDATIONS" | jq -r '.non_nullable_valid')
  if [ "$NON_NULL_VALID" != "true" ]; then
    echo "# FAIL: non-nullable column data mismatch" >&3
    echo "$VALIDATIONS" | jq '.non_nullable' >&3
    false
  fi

  # Check id column has no duplicates and filter is correct
  ID_OK=$(echo "$VALIDATIONS" | jq -r '.id_no_duplicates and .id_filter_valid')
  if [ "$ID_OK" != "true" ]; then
    echo "# FAIL: id column validation failed" >&3
    false
  fi

  # Nullable columns — must also be valid now that Arrow bitmap layout is implemented.
  NULLABLE_PRESENT=$(echo "$VALIDATIONS" | jq -r '.nullable_columns_present')
  echo "# Nullable columns present: $NULLABLE_PRESENT" >&3
  [ "$NULLABLE_PRESENT" -gt 0 ]

  NULLABLE_VALID=$(echo "$VALIDATIONS" | jq -r '.nullable_valid')
  if [ "$NULLABLE_VALID" != "true" ]; then
    echo "# FAIL: nullable column data mismatch" >&3
    echo "$VALIDATIONS" | jq '.nullable' >&3
    false
  fi
}
