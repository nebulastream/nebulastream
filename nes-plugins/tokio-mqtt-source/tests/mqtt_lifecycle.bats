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

# Lifecycle and stress tests for the TokioMqtt embedded broker source.
#
# Lifecycle tests verify broker start/stop behavior:
#   - Broker does not start until a query needs it
#   - Producers cannot connect when no query is running
#   - Broker accepts connections once a query starts
#   - Queries can be added incrementally while the broker is running
#   - Process termination closes the broker port
#
# Stress test verifies many concurrent sources on random topics:
#   - Up to 100 concurrent queries, each on a different topic
#   - Producers publish low-volume data to all topics simultaneously
#   - Each sink receives exactly the rows for its topic
#
# Required env vars: NES_REPL, NES_TEST_TMP_DIR
# Required tools:    mosquitto_pub, python3 (with paho-mqtt)

MQTT_PORT=1883

setup_file() {
  if [ -z "$NES_REPL" ]; then
    echo "ERROR: NES_REPL environment variable must be set" >&2; exit 1
  fi
  if [ ! -x "$NES_REPL" ]; then
    echo "ERROR: NES_REPL is not executable: $NES_REPL" >&2; exit 1
  fi
  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2; exit 1
  fi
  if ! command -v mosquitto_pub &>/dev/null; then
    echo "ERROR: mosquitto_pub not found" >&2; exit 1
  fi
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cd "$TMP_DIR" || exit
}

teardown() {
  # Close FIFO fd if open.
  exec 7>&- 2>/dev/null || true
  if [ -n "$REPL_PID" ] && kill -0 "$REPL_PID" 2>/dev/null; then
    kill -9 "$REPL_PID" 2>/dev/null || true
    wait "$REPL_PID" 2>/dev/null || true
  fi
  if [ -n "$BLOCKER_PID" ] && kill -0 "$BLOCKER_PID" 2>/dev/null; then
    kill -9 "$BLOCKER_PID" 2>/dev/null || true
    wait "$BLOCKER_PID" 2>/dev/null || true
  fi
}


# ---- Helpers ----

# Wait for port to be listening (up to $1 seconds, default 30).
wait_for_port() {
  local timeout=${1:-30}
  for attempt in $(seq 1 "$timeout"); do
    if ss -tlnp 2>/dev/null | grep -q ":${MQTT_PORT} "; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Verify port is NOT listening.
assert_port_closed() {
  ! ss -tlnp 2>/dev/null | grep -q ":${MQTT_PORT} "
}

# Start REPL with FIFO stdin for incremental query submission.
# Sets REPL_PID and FIFO. Use "echo ... >&3" to send SQL.
start_repl_fifo() {
  FIFO="$TMP_DIR/repl_input"
  mkfifo "$FIFO"
  $NES_REPL --debug --on-exit WAIT_FOR_QUERY_TERMINATION < "$FIFO" > "$TMP_DIR/repl_stdout.txt" 2>&1 &
  REPL_PID=$!
  exec 7>"$FIFO"
}

# Send a complete query (source + sink + select) to the REPL via FIFO.
# Args: $1=source_name $2=topic $3=sink_file [$4=query_id]
send_query() {
  local name=$1 topic=$2 sink_file=$3 query_id=${4:-}
  local query_opts=""
  if [ -n "$query_id" ]; then
    query_opts=" SET ('${query_id}' AS \`QUERY\`.\`ID\`)"
  fi
  cat >&7 <<SQLEOF
CREATE LOGICAL SOURCE ${name}(id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR ${name} TYPE TokioMqtt SET(
    '${topic}' AS \`SOURCE\`.MQTT_TOPIC,
    'CSV' AS PARSER.\`TYPE\`
);
CREATE SINK sink_${name}(${name}.id UINT64 NOT NULL, ${name}.value UINT64 NOT NULL, ${name}.ts UINT64 NOT NULL) TYPE File SET(
    '${sink_file}' AS \`SINK\`.FILE_PATH,
    'CSV' AS \`SINK\`.OUTPUT_FORMAT
);
SELECT * FROM ${name} INTO sink_${name}${query_opts};
SQLEOF
}

# ---- Lifecycle tests ----

@test "lifecycle: broker not started without MQTT query" {
  # Start REPL with a plain logical source (no physical MQTT source, no query).
  cat > "$TMP_DIR/no_mqtt.sql" <<'SQLEOF'
CREATE LOGICAL SOURCE dummy(id UINT64 NOT NULL);
SQLEOF
  $NES_REPL < "$TMP_DIR/no_mqtt.sql" > "$TMP_DIR/repl.txt" 2>&1 &
  REPL_PID=$!
  sleep 3

  # Port should NOT be listening.
  assert_port_closed
  echo "# Confirmed: MQTT broker not started without a query" >&3
}

@test "lifecycle: producer cannot connect before query starts" {
  # No REPL running — port 1883 should be closed.
  assert_port_closed

  # mosquitto_pub should fail with connection refused.
  run mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "early-pub" -t "test" -m "x"
  [ "$status" -ne 0 ]
  echo "# Confirmed: producer rejected (exit=$status)" >&3
}

@test "lifecycle: broker starts with first query, data flows" {
  start_repl_fifo
  assert_port_closed

  # Submit first MQTT query.
  send_query "s0" "nes/lifecycle" "$TMP_DIR/out.csv"
  wait_for_port 30
  sleep 2
  echo "# Broker listening after first query" >&3

  # Publish and verify.
  printf '1,10,1000\n2,20,2000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "lc-pub" -t "nes/lifecycle" -s
  sleep 3

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  [ -f "$TMP_DIR/out.csv" ]
  grep -q "1,10,1000" "$TMP_DIR/out.csv"
  grep -q "2,20,2000" "$TMP_DIR/out.csv"
}

@test "lifecycle: second query joins running broker" {
  start_repl_fifo

  # First query on topic/a.
  send_query "s0" "topic/a" "$TMP_DIR/a.csv"
  wait_for_port 30
  sleep 2

  # Publish to topic/a before second query exists.
  printf '1,10,1000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "pub-a1" -t "topic/a" -s
  sleep 1

  # Second query on topic/b.
  send_query "s1" "topic/b" "$TMP_DIR/b.csv"
  sleep 2

  # Publish to both topics.
  printf '2,20,2000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "pub-a2" -t "topic/a" -s
  printf '100,1000,100000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "pub-b1" -t "topic/b" -s
  sleep 3

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  # Sink A should have rows from topic/a (both before and after query B started).
  echo "# Sink A:" >&3; cat "$TMP_DIR/a.csv" >&3
  [ -f "$TMP_DIR/a.csv" ]
  grep -q "1,10,1000" "$TMP_DIR/a.csv"
  grep -q "2,20,2000" "$TMP_DIR/a.csv"

  # Sink B should have only topic/b data.
  echo "# Sink B:" >&3; cat "$TMP_DIR/b.csv" >&3
  [ -f "$TMP_DIR/b.csv" ]
  grep -q "100,1000,100000" "$TMP_DIR/b.csv"

  # Cross-check: topic/a rows should NOT appear in sink B.
  ! grep -q "1,10,1000" "$TMP_DIR/b.csv"
}

@test "lifecycle: process termination closes broker port" {
  start_repl_fifo
  send_query "s0" "nes/data" "$TMP_DIR/out.csv"
  wait_for_port 30
  sleep 2

  # Kill process.
  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""
  sleep 1

  # Port must be closed.
  assert_port_closed
  echo "# Confirmed: port closed after process kill" >&3

  # Producer must fail.
  run mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "post-kill" -t "nes/data" -m "x"
  [ "$status" -ne 0 ]
  echo "# Confirmed: producer rejected after kill (exit=$status)" >&3
}

@test "lifecycle: port conflict — source reports address-in-use error" {
  # Occupy port 1883 with a dummy TCP listener.
  python3 -c "
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', $MQTT_PORT))
s.listen(1)
time.sleep(120)
" &
  BLOCKER_PID=$!
  sleep 1

  # Verify the blocker is holding the port.
  ss -tlnp 2>/dev/null | grep -q ":${MQTT_PORT} "
  echo "# Port $MQTT_PORT occupied by blocker (PID $BLOCKER_PID)" >&3

  # Start REPL with an MQTT query — the broker cannot bind.
  # The broker thread exits (start() returns with the bind error),
  # which triggers the broker_stopped watch. The source detects this
  # and reports the error instead of hanging.
  start_repl_fifo
  send_query "s0" "nes/data" "$TMP_DIR/out.csv"

  # Give the source time to detect the broker failure.
  sleep 8

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  # Check the NES log for the expected error chain.
  local LOG="$TMP_DIR/nes-repl.log"
  echo "# NES log:" >&3; cat "$LOG" >&3

  # The log must contain the server bind error from rumqttd.
  grep -q "Server error" "$LOG"

  # The log must contain our broker error signal.
  grep -q "MQTT broker stopped with error" "$LOG"

  # Sink should not exist or be empty — no data made it through.
  if [ -f "$TMP_DIR/out.csv" ]; then
    local data_lines
    data_lines=$(grep -c "^[0-9]" "$TMP_DIR/out.csv" || true)
    [ -z "$data_lines" ] && data_lines=0
    echo "# Sink has $data_lines data rows (expected 0)" >&3
    [ "$data_lines" -eq 0 ]
  else
    echo "# Sink file not created (expected)" >&3
  fi
}

# ---- Broker restart test ----

@test "lifecycle: drop query and restart — broker restarts cleanly" {

  start_repl_fifo

  # First query with explicit ID.
  send_query "s0" "restart/topic" "$TMP_DIR/out1.csv" "restart-q1"
  wait_for_port 30
  sleep 2
  echo "# Query 1 running" >&3

  # Publish batch 1.
  printf '1,10,1000\n2,20,2000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "restart-pub-1" -t "restart/topic" -s
  sleep 2

  # Drop query — broker shuts down.
  echo "DROP QUERY WHERE id = 'restart-q1';" >&7
  sleep 3

  # Start second query — broker should restart.
  send_query "s1" "restart/topic" "$TMP_DIR/out2.csv" "restart-q2"
  wait_for_port 30
  sleep 2
  echo "# Query 2 running" >&3

  # Publish batch 2.
  printf '3,30,3000\n4,40,4000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "restart-pub-2" -t "restart/topic" -s
  sleep 3

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  local LOG="$TMP_DIR/nes-repl.log"
  echo "# NES log (RUST):" >&3
  grep "RUST" "$LOG" >&3 || true

  # Assert broker lifecycle: started → shutdown → restarted.
  local start_count
  start_count=$(grep -c "Shared MQTT broker started" "$LOG")
  echo "# Broker start count: $start_count (expected 2)" >&3
  [ "$start_count" -eq 2 ]

  grep -q "Last MQTT registration dropped, shutting down broker" "$LOG"
  grep -q "Broker thread joined, port released" "$LOG"
  grep -q "Broker shutdown complete, port released" "$LOG"

  # First query sink should have batch 1.
  echo "# Sink 1:" >&3; cat "$TMP_DIR/out1.csv" >&3
  [ -f "$TMP_DIR/out1.csv" ]
  grep -q "1,10,1000" "$TMP_DIR/out1.csv"

  # Second query sink should have batch 2.
  echo "# Sink 2:" >&3; cat "$TMP_DIR/out2.csv" >&3
  [ -f "$TMP_DIR/out2.csv" ]
  grep -q "3,30,3000" "$TMP_DIR/out2.csv"
}

# ---- Stress test ----

@test "stress: 50 concurrent sources on different topics" {
  local NUM_SOURCES=50
  local ROWS_PER_TOPIC=20

  start_repl_fifo

  # Submit all queries.
  local TOPICS=""
  for i in $(seq 0 $((NUM_SOURCES - 1))); do
    send_query "src_${i}" "stress/topic${i}" "$TMP_DIR/sink_${i}.csv"
    if [ -n "$TOPICS" ]; then TOPICS="${TOPICS},"; fi
    TOPICS="${TOPICS}stress/topic${i}"
  done

  # Wait for broker.
  wait_for_port 30
  echo "# Broker up, $NUM_SOURCES queries submitted" >&3

  # Give all sources time to register their links.
  sleep 5

  # Publish to all topics concurrently.
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"
  python3 "$SCRIPT_DIR/mqtt_multi_topic_publish.py" \
    --port $MQTT_PORT \
    --topics "$TOPICS" \
    --clients-per-topic 1 \
    --rows-per-client $ROWS_PER_TOPIC \
    --batch-size 10 2>&3
  echo "# Published $((NUM_SOURCES * ROWS_PER_TOPIC)) rows across $NUM_SOURCES topics" >&3
  sleep 10

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  # Verify each sink got its rows.
  local pass=0 fail=0
  for i in $(seq 0 $((NUM_SOURCES - 1))); do
    local sink="$TMP_DIR/sink_${i}.csv"
    if [ ! -f "$sink" ]; then
      echo "# FAIL: sink_${i}.csv missing" >&3
      fail=$((fail + 1))
      continue
    fi
    local count
    count=$(grep -c "^[0-9]" "$sink" 2>/dev/null || echo 0)
    if [ "$count" -ge "$ROWS_PER_TOPIC" ]; then
      pass=$((pass + 1))
    else
      echo "# FAIL: topic ${i}: got $count rows, expected $ROWS_PER_TOPIC" >&3
      fail=$((fail + 1))
    fi
  done

  echo "# Results: $pass passed, $fail failed out of $NUM_SOURCES sources" >&3
  [ "$fail" -eq 0 ]
}
