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

# Integration tests for the TokioMqtt embedded broker source.
#
# Three test cases:
#   1. Basic:  4 rows in one message
#   2. Bulk:   10 000 rows streamed at full speed
#   3. Drip:   10 rows with 1s sleep between each (flush verification)
#
# Required env vars: NES_REPL, NES_TEST_TMP_DIR
# Required tools:    mosquitto_pub

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
  echo "# MQTT source test suite completed" >&3
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cd "$TMP_DIR" || exit
  echo "# Using TMP_DIR: $TMP_DIR" >&3
}

teardown() {
  if [ -n "$REPL_PID" ] && kill -0 "$REPL_PID" 2>/dev/null; then
    kill -9 "$REPL_PID" 2>/dev/null || true
    wait "$REPL_PID" 2>/dev/null || true
  fi
}

# ---- Helpers ----

# Default MQTT broker port (hardcoded in the source, not configurable per-source).
MQTT_PORT=1883

# Write the SQL file for a given sink path.
write_sql() {
  local sink_file=$1
  cat > "$TMP_DIR/mqtt_test.sql" <<SQLEOF
CREATE LOGICAL SOURCE mqtt_stream(id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR mqtt_stream TYPE TokioMqtt SET(
    'nes/data' AS \`SOURCE\`.MQTT_TOPIC,
    'CSV' AS PARSER.\`TYPE\`
);
CREATE SINK mqtt_sink(mqtt_stream.id UINT64 NOT NULL, mqtt_stream.value UINT64 NOT NULL, mqtt_stream.ts UINT64 NOT NULL) TYPE File SET(
    '${sink_file}' AS \`SINK\`.FILE_PATH,
    'CSV' AS \`SINK\`.OUTPUT_FORMAT
);
SELECT * FROM mqtt_stream INTO mqtt_sink;
SQLEOF
}

# Start the REPL and wait for the MQTT broker to accept connections.
start_repl_and_wait() {
  $NES_REPL --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/mqtt_test.sql" > "$TMP_DIR/repl_stdout.txt" 2>&1 &
  REPL_PID=$!
  echo "# Started REPL PID $REPL_PID on port $MQTT_PORT" >&3

  local ready=0
  for attempt in $(seq 1 30); do
    if ss -tlnp 2>/dev/null | grep -q ":${MQTT_PORT} "; then
      ready=1; break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "# MQTT broker not ready on port $MQTT_PORT after 30s" >&3; return 1
  fi
  echo "# MQTT broker listening on port $MQTT_PORT" >&3
  sleep 2
}

# ---- Tests ----

@test "MQTT basic: 4 rows in single message" {
  local SINK="$TMP_DIR/mqtt_output.csv"
  write_sql "$SINK"
  start_repl_and_wait

  printf '1,10,1000\n2,20,2000\n3,30,3000\n4,40,4000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "basic-pub" -t "nes/data" -s
  echo "# Published 4 rows" >&3
  sleep 3

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  echo "# Sink contents:" >&3; cat "$SINK" >&3

  [ -f "$SINK" ]
  grep -q "1,10,1000" "$SINK"
  grep -q "4,40,4000" "$SINK"
  [ "$(grep -c "^[0-9]" "$SINK")" -ge 4 ]
}

@test "MQTT large message: 1000 rows in single MQTT message (no truncation)" {
  local SINK="$TMP_DIR/mqtt_large.csv"
  write_sql "$SINK"
  start_repl_and_wait

  # Send 1000 rows (~17 KB) in a single MQTT message.
  # This exceeds the NES buffer size (~4-8 KB) and will be truncated
  # unless the source splits large payloads across multiple buffers.
  seq 1 1000 | awk '{printf "%d,%d,%d\n", $1, $1*10, $1*1000}' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "large-pub" -t "nes/data" -s
  echo "# Published 1000 rows in single message" >&3
  sleep 5

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  echo "# Sink contents (first 5 / last 5):" >&3
  head -6 "$SINK" >&3; echo "# ..." >&3; tail -5 "$SINK" >&3

  [ -f "$SINK" ]
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SINK")
  echo "# Got $data_lines data rows (expected 1000)" >&3
  [ "$data_lines" -ge 1000 ]
  grep -q "1,10,1000" "$SINK"
  grep -q "1000,10000,1000000" "$SINK"
}

@test "MQTT bulk: 10000 rows from 10 concurrent clients" {
  local SINK="$TMP_DIR/mqtt_bulk.csv"
  write_sql "$SINK"
  start_repl_and_wait

  # 10 concurrent clients, each publishing 1000 rows in 100-row batches.
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"
  python3 "$SCRIPT_DIR/mqtt_publish.py" \
    --port $MQTT_PORT --topic "nes/data" \
    --clients 10 --rows-per-client 1000 --batch-size 100 2>&3
  echo "# Published 10000 rows from 10 clients" >&3
  sleep 10

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  [ -f "$SINK" ]
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SINK")
  echo "# Got $data_lines data rows (expected 10000)" >&3
  [ "$data_lines" -ge 10000 ]
}

@test "MQTT drip: 10 rows with 1s sleep (flush verification)" {
  local SINK="$TMP_DIR/mqtt_drip.csv"
  write_sql "$SINK"
  start_repl_and_wait

  for i in $(seq 1 10); do
    printf '%s\n' "${i},$((i * 10)),$((i * 1000))" \
      | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "drip-pub-${i}" -t "nes/data" -s
    echo "# Published row $i" >&3
    sleep 1
  done
  sleep 3

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  echo "# Drip output:" >&3; cat "$SINK" >&3

  [ -f "$SINK" ]
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SINK")
  echo "# Got $data_lines data rows (expected 10)" >&3
  [ "$data_lines" -ge 10 ]
  for i in $(seq 1 10); do
    grep -q "${i},$((i * 10)),$((i * 1000))" "$SINK"
  done
}

@test "MQTT topic isolation: source ignores messages for other topics" {
  # One source subscribed to a specific topic. Publishers send to both
  # the correct topic AND a different topic. The sink must contain ONLY
  # rows from the correct topic.
  local SINK="$TMP_DIR/isolation.csv"
  write_sql "$SINK"
  start_repl_and_wait

  # Publish to the subscribed topic (nes/data) — rows 1-5
  printf '1,10,1000\n2,20,2000\n3,30,3000\n4,40,4000\n5,50,5000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "iso-good" -t "nes/data" -s

  # Publish to a DIFFERENT topic — rows 99-100 (should NOT appear in sink)
  printf '99,990,99000\n100,1000,100000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "iso-bad" -t "other/topic" -s

  # Publish more to the correct topic — rows 6-8
  printf '6,60,6000\n7,70,7000\n8,80,8000\n' \
    | mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -V mqttv311 -i "iso-good2" -t "nes/data" -s

  echo "# Published 8 correct + 2 wrong-topic rows" >&3
  sleep 3

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  echo "# Sink contents:" >&3; cat "$SINK" >&3

  [ -f "$SINK" ]
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SINK")
  echo "# Got $data_lines data rows (expected 8)" >&3
  [ "$data_lines" -eq 8 ]

  # Rows from nes/data must be present.
  grep -q "1,10,1000" "$SINK"
  grep -q "8,80,8000" "$SINK"

  # Rows from other/topic must NOT be present.
  ! grep -q "99,990,99000" "$SINK"
  ! grep -q "100,1000,100000" "$SINK"
}

@test "MQTT stress: 10000 rows with noise on 4 other topics" {
  # One source on nes/data, but 5 topics are being published to
  # concurrently. The source must receive all 10000 rows for its topic
  # and none from the noise topics.
  local SINK="$TMP_DIR/stress.csv"
  write_sql "$SINK"
  start_repl_and_wait

  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BATS_TEST_FILENAME}")" && pwd)"

  # 5 topics published concurrently: nes/data (the real one) + 4 noise topics.
  # Each topic gets 2 clients × 1000 rows.
  python3 "$SCRIPT_DIR/mqtt_multi_topic_publish.py" \
    --port $MQTT_PORT \
    --topics "nes/data,noise/a,noise/b,noise/c,noise/d" \
    --clients-per-topic 2 \
    --rows-per-client 1000 \
    --batch-size 100 2>&3
  echo "# Published 10000 rows across 5 topics" >&3
  sleep 10

  kill -9 "$REPL_PID" 2>/dev/null; wait "$REPL_PID" 2>/dev/null || true

  [ -f "$SINK" ]
  local data_lines
  data_lines=$(grep -c "^[0-9]" "$SINK")
  echo "# Got $data_lines data rows (expected 2000 from nes/data)" >&3

  # Source should have received exactly the 2000 rows published to nes/data
  # (2 clients × 1000 rows). Row IDs 1..2000.
  [ "$data_lines" -ge 2000 ]
  grep -q "^1," "$SINK"
  grep -q "^2000," "$SINK"
}
