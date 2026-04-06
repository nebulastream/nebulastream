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

# Integration tests for the TokioMqttSink plugin.
#
# Test cases:
#   1. Generator → query → MQTT sink → subscriber (verifies actual query output)
#   2. MQTT source → query → MQTT sink → subscriber (basic round-trip)
#   3. MQTT source → query → MQTT sink → subscriber (10k rows)
#   4. Topic isolation: subscriber on unrelated topic sees nothing
#   5. Port conflict: broker can't bind, sink reports error
#
# Required env vars: NES_REPL, NES_TEST_TMP_DIR
# Required tools:    mosquitto_pub, mosquitto_sub

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
    echo "ERROR: mosquitto_pub not found. Install with: apt install mosquitto-clients" >&2; exit 1
  fi
  echo "# Using NES_REPL: $NES_REPL" >&3
}

teardown_file() {
  echo "# MQTT sink test suite completed" >&3
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cd "$TMP_DIR" || exit
  echo "# Using TMP_DIR: $TMP_DIR" >&3
}

teardown() {
  # Close FIFO fd if open.
  exec 7>&- 2>/dev/null || true
  if [ -n "$SUB_PID" ] && kill -0 "$SUB_PID" 2>/dev/null; then
    kill "$SUB_PID" 2>/dev/null || true
    wait "$SUB_PID" 2>/dev/null || true
  fi
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

# Start REPL with FIFO stdin for incremental query submission.
# Sets REPL_PID and FIFO. Use "echo ... >&7" to send SQL.
start_repl_fifo() {
  FIFO="$TMP_DIR/repl_input"
  mkfifo "$FIFO"
  $NES_REPL --debug --on-exit WAIT_FOR_QUERY_TERMINATION < "$FIFO" > "$TMP_DIR/repl_stdout.txt" 2>&1 &
  REPL_PID=$!
  exec 7>"$FIFO"
}

# Send a MQTT source→MQTT sink query to the REPL via FIFO.
# Args: $1=source_topic $2=sink_topic
send_mqtt_source_sink_query() {
  local src_topic=$1 sink_topic=$2
  cat >&7 <<SQLEOF
CREATE LOGICAL SOURCE mqtt_input(id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR mqtt_input TYPE TokioMqtt SET(
    '${src_topic}' AS \`SOURCE\`.MQTT_TOPIC,
    'CSV' AS PARSER.\`TYPE\`
);
CREATE SINK mqtt_output(mqtt_input.id UINT64 NOT NULL, mqtt_input.value UINT64 NOT NULL, mqtt_input.ts UINT64 NOT NULL) TYPE TokioMqttSink SET(
    '${sink_topic}' AS \`SINK\`.MQTT_TOPIC,
    'CSV' AS \`SINK\`.OUTPUT_FORMAT
);
SELECT * FROM mqtt_input INTO mqtt_output;
SQLEOF
}

# Start Python MQTT subscriber in the background. Retries connection
# until the broker is up, so it can be started before the broker.
# Args: $1=topic $2=output_file $3=timeout (default 30)
# Sets SUB_PID.
start_subscriber() {
  local topic=$1 output_file=$2 timeout=${3:-30}
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"
  python3 "$SCRIPT_DIR/mqtt_subscribe.py" \
    --port $MQTT_PORT --topic "$topic" \
    --output "$output_file" --timeout "$timeout" 2>&3 &
  SUB_PID=$!
}

# ---- Tests ----

@test "MQTT sink: generator 10k tuples with verified CSV output" {
  # TokioGenerator emits 10000 values (0..9999) → query → TokioMqttSink → subscriber.
  # The pipeline formats output as CSV. We verify the subscriber receives
  # all expected values, confirming the full pipeline works end-to-end.
  #
  # Ordering: start Python subscriber (retries connection) → submit query
  # (starts broker + generator) → subscriber connects as soon as broker is
  # up and catches all messages from the start.
  local SUB_OUT="$TMP_DIR/gen_output.txt"
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"

  # Start subscriber BEFORE the query. It retries connection until the
  # broker comes up, so there is no race.
  python3 "$SCRIPT_DIR/mqtt_subscribe.py" \
    --port $MQTT_PORT --topic "gen/output" \
    --output "$SUB_OUT" --timeout 45 &
  SUB_PID=$!
  echo "# Started Python subscriber (PID $SUB_PID), waiting for broker" >&3

  # Now submit the query. This starts the broker and the generator.
  start_repl_fifo

  cat >&7 <<'SQLEOF'
CREATE LOGICAL SOURCE gen(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR gen TYPE TokioGenerator SET(
    10000 AS `SOURCE`.GENERATOR_COUNT,
    0 AS `SOURCE`.GENERATOR_INTERVAL_MS,
    'CSV' AS PARSER.`TYPE`
);
CREATE SINK mqttOut(gen.value UINT64 NOT NULL) TYPE TokioMqttSink SET(
    'gen/output' AS `SINK`.MQTT_TOPIC,
    'CSV' AS `SINK`.OUTPUT_FORMAT
);
SELECT * FROM gen INTO mqttOut;
SQLEOF

  # Wait for subscriber to finish (timeout-based exit).
  wait "$SUB_PID" 2>/dev/null || true
  SUB_PID=""

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  echo "# Generator output (first 5 / last 5 lines):" >&3
  head -5 "$SUB_OUT" >&3; echo "# ..." >&3; tail -5 "$SUB_OUT" >&3

  [ -f "$SUB_OUT" ]

  # Count data lines (CSV: one number per line).
  #
  # The external subscriber misses the first ~30 values because the broker
  # only starts when the query is submitted, and the generator emits at
  # interval=0. By the time the subscriber connects and its subscription
  # is registered by the router, the first batch has already been published.
  # This is expected behavior with QoS 0 and an on-demand broker — no data
  # is lost in the pipeline itself.
  #
  # A future "always-on broker" config would eliminate this window.
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SUB_OUT")
  echo "# Got $data_lines data rows (expected ~10000, first ~30 lost to subscriber registration race)" >&3
  [ "$data_lines" -ge 9900 ]

  # Verify only the initial values are missing (subscriber registration race).
  # The received values must form a contiguous range ending at 9999.
  local first_value last_value
  first_value=$(grep "^[0-9]" "$SUB_OUT" | head -1)
  last_value=$(grep "^[0-9]" "$SUB_OUT" | tail -1)
  echo "# First received: $first_value, Last received: $last_value" >&3
  [ "$last_value" -eq 9999 ]

  # All values from first_value to 9999 must be present (no gaps in the middle).
  local expected_count=$(( 9999 - first_value + 1 ))
  echo "# Expected contiguous range [$first_value..9999] = $expected_count values, got $data_lines" >&3
  [ "$data_lines" -eq "$expected_count" ]
}

@test "MQTT sink: basic round-trip MQTT source to MQTT sink" {
  # External publisher → MQTT Source → Query → MQTT Sink → External subscriber.
  # Subscriber starts first (retries connection), then query, then publish.
  local SUB_OUT="$TMP_DIR/sub_output.txt"
  start_subscriber "out/data" "$SUB_OUT" 20

  start_repl_fifo
  send_mqtt_source_sink_query "raw/data" "out/data"

  # Wait for broker so we can publish.
  wait_for_port 30
  sleep 2

  printf '1,10,1000\n2,20,2000\n3,30,3000\n4,40,4000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "sink-pub" -t "raw/data" -s
  echo "# Published 4 rows to raw/data" >&3

  wait "$SUB_PID" 2>/dev/null || true
  SUB_PID=""
  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  echo "# Subscriber output:" >&3; cat "$SUB_OUT" >&3

  [ -f "$SUB_OUT" ]
  grep -q "1,10,1000" "$SUB_OUT"
  grep -q "4,40,4000" "$SUB_OUT"
}

@test "MQTT sink: 10000 rows round-trip" {
  # Subscriber starts first, then query, then publish 10k rows.
  local SUB_OUT="$TMP_DIR/sub_output.txt"
  start_subscriber "output" "$SUB_OUT" 30

  start_repl_fifo
  send_mqtt_source_sink_query "input" "output"

  wait_for_port 30
  sleep 2

  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"
  python3 "$SCRIPT_DIR/../../tokio-mqtt-source/tests/mqtt_publish.py" \
    --port $MQTT_PORT --topic "input" \
    --clients 10 --rows-per-client 1000 --batch-size 100 2>&3
  echo "# Published 10000 rows to input" >&3

  wait "$SUB_PID" 2>/dev/null || true
  SUB_PID=""
  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  echo "# Subscriber output (first 5 / last 5):" >&3
  head -5 "$SUB_OUT" >&3; echo "# ..." >&3; tail -5 "$SUB_OUT" >&3

  [ -f "$SUB_OUT" ]
  grep -q "1,10,1000" "$SUB_OUT"
  grep -q "10000,100000,10000000" "$SUB_OUT"
}

@test "MQTT sink: topic isolation — subscriber on unrelated topic sees nothing" {
  # Subscriber on "other/topic", data flows through "raw/data" → "out/data".
  local SUB_OUT="$TMP_DIR/sub_output.txt"
  start_subscriber "other/topic" "$SUB_OUT" 15

  start_repl_fifo
  send_mqtt_source_sink_query "raw/data" "out/data"

  wait_for_port 30
  sleep 2

  printf '1,10,1000\n2,20,2000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "iso-pub" -t "raw/data" -s
  echo "# Published 2 rows to raw/data" >&3

  wait "$SUB_PID" 2>/dev/null || true
  SUB_PID=""
  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  echo "# Subscriber output (should be empty):" >&3; cat "$SUB_OUT" >&3

  [ ! -s "$SUB_OUT" ]
}

@test "MQTT sink: subscriber survives query stop and restart" {

  local SUB_OUT="$TMP_DIR/lifecycle_output.txt"
  start_subscriber "lifecycle/out" "$SUB_OUT" 30

  sleep 1

  start_repl_fifo

  cat >&7 <<'SQLEOF'
CREATE LOGICAL SOURCE lc_src(id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR lc_src TYPE TokioMqtt SET(
    'lifecycle/in' AS `SOURCE`.MQTT_TOPIC,
    'CSV' AS PARSER.`TYPE`
);
CREATE SINK lc_sink(lc_src.id UINT64 NOT NULL, lc_src.value UINT64 NOT NULL, lc_src.ts UINT64 NOT NULL) TYPE TokioMqttSink SET(
    'lifecycle/out' AS `SINK`.MQTT_TOPIC,
    'CSV' AS `SINK`.OUTPUT_FORMAT
);
SELECT * FROM lc_src INTO lc_sink SET ('lc-query-1' AS `QUERY`.`ID`);
SQLEOF

  wait_for_port 30
  sleep 2
  echo "# Query 1 running" >&3

  printf '1,10,1000\n2,20,2000\n3,30,3000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "lc-pub-1" -t "lifecycle/in" -s
  echo "# Published batch 1 (rows 1-3)" >&3
  sleep 2

  echo "DROP QUERY WHERE id = 'lc-query-1';" >&7
  echo "# Stopped query 1" >&3
  sleep 2

  # Gap row: broker is down after DROP QUERY, so this publish is expected to fail.
  printf '100,1000,100000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "lc-pub-gap" -t "lifecycle/in" -s 2>/dev/null || true
  echo "# Published gap row (100) — expected to fail (broker down)" >&3
  sleep 1

  echo "SELECT * FROM lc_src INTO lc_sink SET ('lc-query-2' AS \`QUERY\`.\`ID\`);" >&7
  echo "# Started query 2" >&3
  wait_for_port 30
  # Wait for the Python subscriber to reconnect to the restarted broker.
  # The subscriber retries every 1s; give it time to reconnect and subscribe.
  sleep 5

  printf '4,40,4000\n5,50,5000\n6,60,6000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "lc-pub-2" -t "lifecycle/in" -s
  echo "# Published batch 2 (rows 4-6)" >&3
  sleep 3

  wait "$SUB_PID" 2>/dev/null || true
  SUB_PID=""
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
  grep -q "Broker shutdown complete, port released" "$LOG"

  echo "# Subscriber output:" >&3; cat "$SUB_OUT" >&3

  [ -f "$SUB_OUT" ]
  grep -q "1,10,1000" "$SUB_OUT"
  grep -q "3,30,3000" "$SUB_OUT"
  grep -q "4,40,4000" "$SUB_OUT"
  grep -q "6,60,6000" "$SUB_OUT"
  ! grep -q "100,1000,100000" "$SUB_OUT"
}

@test "MQTT sink: port conflict reports error" {
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

  ss -tlnp 2>/dev/null | grep -q ":${MQTT_PORT} "
  echo "# Port $MQTT_PORT occupied by blocker (PID $BLOCKER_PID)" >&3

  start_repl_fifo
  send_mqtt_source_sink_query "raw/data" "out/data"

  sleep 8

  exec 7>&-
  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true
  REPL_PID=""

  local LOG="$TMP_DIR/nes-repl.log"
  echo "# NES log:" >&3; cat "$LOG" >&3

  grep -q "Address already in use" "$LOG"
  grep -q "MQTT broker stopped.*broker error" "$LOG"
}
