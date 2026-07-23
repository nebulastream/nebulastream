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

setup_file()    { nes_distributed_setup_file "$NES_CLI"; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

docker_mqtt_subscribe() {
  local topic=$1
  shift
  docker compose exec mqtt-client mosquitto_sub -h mqtt-broker -t "$topic" > "$1"
}

@test "example test with small data" {
  setup_distributed tests/good/example.yaml

  docker_mqtt_subscribe "mqtt-sink-test" results.csv &
  run docker_nes_cli -t tests/good/example.yaml start
  assert_success

  # wait until the query stops
  wait_until_status tests/good/example.yaml "Stopped" $output

  sleep 1
  sync_workdir
  # assert that all generates tuples (1, 1000] where generated
  assert_file_line_count results.csv 9 --ignore-empty-lines
}

@test "test child buffer" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker_mqtt_subscribe "mqtt-sink-test" results.csv &
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM
    GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 1000 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO MQTT(
        'worker-1:8080' AS "SINK"."HOST",
        'mqtt-sink-test' AS "SINK"."TOPIC",
        'mqtt-broker' AS "SINK"."SERVER_URI",
        2 AS "SINK"."QOS",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        -- provoke backpressure
        4 AS "SINK"."MAX_OUTSTANDING_MESSAGES",
        '2' AS "SINK"."BACKPRESSURE_LOWER_THRESHOLD",
        '4' AS "SINK"."BACKPRESSURE_UPPER_THRESHOLD"
    )
EOF
)"
  assert_success
  wait_until_status tests/good/example.yaml "Stopped" $output --require-healthy "worker-1"

  sync_workdir
  assert_file_line_count results.csv 1000 --ignore-empty-lines
}


@test "more data test" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker_mqtt_subscribe "mqtt-sink-test" results.csv &
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM
    GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 400000 1' AS "SOURCE"."GENERATOR_SCHEMA",
        2 AS "SOURCE"."FLUSH_INTERVAL_MS",
        'EMIT_RATE 100000' AS "SOURCE"."GENERATOR_RATE_CONFIG",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO MQTT(
        'worker-1:8080' AS "SINK"."HOST",
        'mqtt-sink-test' AS "SINK"."TOPIC",
        'mqtt-broker' AS "SINK"."SERVER_URI",
        2 AS "SINK"."QOS",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        -- provoke backpressure
        4 AS "SINK"."MAX_OUTSTANDING_MESSAGES",
        '2' AS "SINK"."BACKPRESSURE_LOWER_THRESHOLD",
        '4' AS "SINK"."BACKPRESSURE_UPPER_THRESHOLD"
    )
EOF
)"
  assert_success
  wait_until_status tests/good/example.yaml "Stopped" $output

  sync_workdir
  assert_file_line_count results.csv 400000 --ignore-empty-lines
  grep -q "Backpressure acquired:" worker-1/singleNodeWorker.log
  grep -q "Backpressure released:" worker-1/singleNodeWorker.log
}

@test "fails query when broker stops during processing" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR(
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 10000000 1' AS "SOURCE"."GENERATOR_SCHEMA",
        'EMIT_RATE 10' AS "SOURCE"."GENERATOR_RATE_CONFIG",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO MQTT(
        'worker-1:8080' AS "SINK"."HOST",
        'mqtt-sink-test' AS "SINK"."TOPIC",
        'mqtt-broker' AS "SINK"."SERVER_URI",
        2 AS "SINK"."QOS",
        'CSV' AS "SINK"."OUTPUT_FORMAT"
    )
EOF
)"
  assert_success
  query_id=$output

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Running" "$query_id" --require-healthy "worker-1"
  sleep 1
  docker compose stop mqtt-broker
  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" "$query_id" --require-healthy "worker-1"
}

@test "fails query when broker is unavailable at startup" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker compose kill mqtt-broker
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR(
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 10000000 1' AS "SOURCE"."GENERATOR_SCHEMA",
        'EMIT_RATE 10' AS "SOURCE"."GENERATOR_RATE_CONFIG",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO MQTT(
        'worker-1:8080' AS "SINK"."HOST",
        'mqtt-sink-test' AS "SINK"."TOPIC",
        'mqtt-broker' AS "SINK"."SERVER_URI",
        2 AS "SINK"."QOS",
        'CSV' AS "SINK"."OUTPUT_FORMAT"
    )
EOF
)"
  assert_success
  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" "$output" --require-healthy "worker-1"
}
