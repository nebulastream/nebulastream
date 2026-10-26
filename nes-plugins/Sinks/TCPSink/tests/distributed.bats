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

setup_file() {
  nes_distributed_setup_file "$NES_CLI" tcp-sink
}

teardown_file() {
  nes_distributed_teardown_file
}

setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

@test "streams formatted output to nc" {
  setup_distributed tests/good/example.yaml

  run docker_nes_cli -t tests/good/example.yaml start
  assert_success
  wait_until_status tests/good/example.yaml "Stopped" "$output" --require-healthy "worker-1"

  wait_until test -s results.csv
  assert_file_line_count results.csv 10 --ignore-empty-lines
  run diff -u <(seq 0 9) <(sort -n results.csv)
  assert_success
}

@test "applies backpressure while nc is not reading" {
  setup_distributed tests/good/example.yaml
  docker compose pause tcp-client

  run docker_nes_cli -t tests/good/example.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 2000 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO TCP (
        'worker-1:8080' AS "SINK"."HOST",
        'tcp-client' AS "SINK"."SOCKET_HOST",
        9000 AS "SINK"."SOCKET_PORT",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        4 AS "SINK"."MAX_QUEUED_BUFFERS",
        2 AS "SINK"."BACKPRESSURE_LOWER_THRESHOLD",
        4 AS "SINK"."BACKPRESSURE_UPPER_THRESHOLD"
    )
EOF
)"
  assert_success
  query_id=$output

  wait_until grep -q "Backpressure acquired:" worker-1/singleNodeWorker.log
  docker compose unpause tcp-client
  wait_until_status tests/good/example.yaml "Stopped" "$query_id" --require-healthy "worker-1"

  wait_until awk 'NF { count++ } END { exit(count != 2000) }' results.csv
  assert_file_line_count results.csv 2000 --ignore-empty-lines
}

@test "reconnects after the TCP peer restarts" {
  setup_distributed tests/good/example.yaml

  run docker_nes_cli -t tests/good/example.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 1000 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        'EMIT_RATE 100' AS "SOURCE"."GENERATOR_RATE_CONFIG",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO TCP (
        'worker-1:8080' AS "SINK"."HOST",
        'tcp-client' AS "SINK"."SOCKET_HOST",
        9000 AS "SINK"."SOCKET_PORT",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        4 AS "SINK"."MAX_QUEUED_BUFFERS",
        10000 AS "SINK"."CONNECT_TIMEOUT_MS"
    )
EOF
)"
  assert_success
  query_id=$output

  wait_until awk 'NF { count++ } END { exit(count < 10) }' results.csv
  docker compose restart tcp-client

  wait_until grep -q "TCPChannel disconnected" worker-1/singleNodeWorker.log
  wait_until grep -q "Reconnected TCPChannel" worker-1/singleNodeWorker.log
  wait_until_status tests/good/example.yaml "Stopped" "$query_id" --require-healthy "worker-1"
}

@test "fails after the reconnect timeout" {
  setup_distributed tests/good/example.yaml

  run docker_nes_cli -t tests/good/example.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 10000 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        'EMIT_RATE 100' AS "SOURCE"."GENERATOR_RATE_CONFIG",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO TCP (
        'worker-1:8080' AS "SINK"."HOST",
        'tcp-client' AS "SINK"."SOCKET_HOST",
        9000 AS "SINK"."SOCKET_PORT",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        4 AS "SINK"."MAX_QUEUED_BUFFERS",
        1000 AS "SINK"."CONNECT_TIMEOUT_MS",
        0 AS "SINK"."BACKPRESSURE_LOWER_THRESHOLD",
        1 AS "SINK"."BACKPRESSURE_UPPER_THRESHOLD"
    )
EOF
)"
  assert_success
  query_id=$output

  wait_until test -s results.csv
  docker compose kill tcp-client

  wait_until grep -q "Backpressure acquired:" worker-1/singleNodeWorker.log
  wait_until_status tests/good/example.yaml "Failed" "$query_id" --require-healthy "worker-1"
}

@test "aborts a blocked writer after the close timeout" {
  setup_distributed tests/good/example.yaml
  docker compose pause tcp-client

  run docker_nes_cli -t tests/good/example.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 300 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO TCP (
        'worker-1:8080' AS "SINK"."HOST",
        'tcp-client' AS "SINK"."SOCKET_HOST",
        9000 AS "SINK"."SOCKET_PORT",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        512 AS "SINK"."MAX_QUEUED_BUFFERS",
        1000 AS "SINK"."CLOSE_TIMEOUT_MS"
    )
EOF
)"
  assert_success
  query_id=$output

  wait_until grep -q "accepted buffers may be lost" worker-1/singleNodeWorker.log
  wait_until_status tests/good/example.yaml "Stopped" "$query_id" --require-healthy "worker-1"
}

@test "shuts down the worker while the TCP peer is not reading" {
  setup_distributed tests/good/example.yaml
  docker compose pause tcp-client

  run docker_nes_cli -t tests/good/example.yaml start "$(cat <<'EOF'
    SELECT * FROM GENERATOR (
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        'worker-1:8080' AS "SOURCE"."HOST",
        'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        'SEQUENCE UINT64 0 2000 1, RANDOMSTR 4096 4096' AS "SOURCE"."GENERATOR_SCHEMA",
        SCHEMA(id UINT64 NOT NULL, text VARSIZED NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO TCP (
        'worker-1:8080' AS "SINK"."HOST",
        'tcp-client' AS "SINK"."SOCKET_HOST",
        9000 AS "SINK"."SOCKET_PORT",
        'CSV' AS "SINK"."OUTPUT_FORMAT",
        4 AS "SINK"."MAX_QUEUED_BUFFERS",
        2 AS "SINK"."BACKPRESSURE_LOWER_THRESHOLD",
        4 AS "SINK"."BACKPRESSURE_UPPER_THRESHOLD",
        1000 AS "SINK"."CLOSE_TIMEOUT_MS"
    )
EOF
  )"
  assert_success

  wait_until grep -q "Backpressure acquired:" worker-1/singleNodeWorker.log

  assert_success_within_deadline 10 docker compose stop -t 10 worker-1

  run docker inspect --format '{{.State.ExitCode}}' "$(docker compose ps -aq worker-1)"
  assert_success
  assert_output 143
}
