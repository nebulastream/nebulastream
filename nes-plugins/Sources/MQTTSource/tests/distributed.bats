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

docker_mqtt_produce() {
  local topic=$1
  shift
  docker compose exec mqtt-client mosquitto_pub -h mqtt-broker -t "$topic" -m "$@"
}

docker_mqtt_subscribe() {
  local topic=$1
  shift
  docker compose exec mqtt-client mosquitto_sub -h mqtt-broker -t "$topic" >&3
}

@test "launch query from topology" {
  setup_distributed tests/good/example.yaml
  run docker_nes_cli -t tests/good/example.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  docker_mqtt_produce mqtt-source-test $'32.0\n'
  docker_mqtt_produce mqtt-source-test $'32.0\n'
  sleep 1

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  sleep 1

  sync_workdir
  grep "2,410" worker-1/results.csv
}

@test "launch query from commandline" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        100 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO CHECKSUM('worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  docker_mqtt_produce mqtt-source-test $'32\n'
  docker_mqtt_produce mqtt-source-test $'32\n'
  sleep 1

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  [ "$status" -eq 0 ]
  sleep 1

  sync_workdir
  grep "2,222" worker-1/results.csv
}

@test "long flushing interval" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        5000 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"

  [ "$status" -eq 0 ]

  sleep 1
  docker_mqtt_produce mqtt-source-test $'32\n'
  docker_mqtt_produce mqtt-source-test $'32\n'

  sleep 6
  sync_workdir
  # header + 2 rows
  [ "$(cat worker-1/results.csv | wc -l)" -eq 3 ]
}

@test "long flushing interval does not flush early" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        120000 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"

  [ "$status" -eq 0 ]

  sleep 1
  docker_mqtt_produce mqtt-source-test $'32\n'
  docker_mqtt_produce mqtt-source-test $'32\n'
  sleep 5

  sync_workdir
  assert_file_line_count worker-1/results.csv 0
}


@test "long flushing interval flush on buffer full" {

  # configures the buffer to use 4096 bytes.
  # the source receives raw ascii csv. the data is always ['3', '2', '\n'] (3 bytes)

  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        1000000 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  #
  for i in {1..68}; do (docker_mqtt_produce mqtt-source-test $'32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n'&); done
  wait
  sleep 1

  sync_workdir
  [ "$(cat worker-1/results.csv | wc -l)" -eq 0 ]

  docker_mqtt_produce mqtt-source-test $'32\n32\n32\n32\n32\n32\n'

  sleep 1
  sync_workdir

  # header + 1365 rows (4096 / 3)
  [ "$(cat worker-1/results.csv | wc -l)" -eq 1366 ]
}

@test "more data" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        100 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO CHECKSUM('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1

  # 30000 tuples
  # 2 producers
  for i in {1..2}; do
    (
        # 250 messages
        for j in {1..250}; do
            # 60 per message
            docker_mqtt_produce mqtt-source-test $'32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n'
        done
    )&
  done

  # 500 tuples
  # 2 producers
  for i in {1..2}; do
    (
        # 250 messages
        for j in {1..250}; do
            # 1 per message
            docker_mqtt_produce mqtt-source-test $'32\n'
        done
    )&
  done
  wait

  sleep 1
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id

  sync_workdir
  grep "30500" worker-1/results.csv
}

@test "fails query when broker stops during processing" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        100 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO CHECKSUM('worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"
  assert_success
  query_id=$output

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Running" "$query_id" --require-healthy 'worker-1'
  run docker_mqtt_produce mqtt-source-test $'32\n'
  assert_success
  sleep 1
  docker compose stop mqtt-broker
  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" "$query_id" --require-healthy 'worker-1'
}

@test "fails query when broker is unavailable at startup" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker compose kill mqtt-broker
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        'worker-1:8080' AS "SOURCE"."HOST",
        'mqtt-source-test' AS "SOURCE"."TOPIC",
        'mqtt-broker' AS "SOURCE"."SERVER_URI",
        100 AS "SOURCE"."FLUSH_INTERVAL_MS",
        2 AS "SOURCE"."QOS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO CHECKSUM('worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"
  assert_success
  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" "$output" --require-healthy '^worker-.*$'
}
