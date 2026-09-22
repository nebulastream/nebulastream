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

setup_file()    { nes_distributed_setup_file "$NES_CLI" tcp-source; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

@test "launch query from topology" {
  setup_distributed tests/good/example.yaml
  # nc closes the connection once it has sent its input.
  docker compose exec -d tcp-peer sh -c "printf '1\n2\n3\n' | nc -l -p 9999"
  wait_until docker compose exec -T tcp-peer sh -c 'netstat -ltn | grep -q :9999'

  run docker_nes_cli -t tests/good/example.yaml start
  assert_success
  query_id=$output

  wait_until_status tests/good/example.yaml "Stopped" "$query_id" --require-healthy 'worker-1'
  # header + 3 rows
  wait_until assert_file_line_count worker-1/results.csv 4
}

@test "query completes with all data when peer closes the connection" {
  setup_distributed tests/good/single-worker.yaml
  # nc closes the connection once it has sent its input.
  docker compose exec -d tcp-peer sh -c "printf '1\n2\n3\n' | nc -l -p 9999"
  wait_until docker compose exec -T tcp-peer sh -c 'netstat -ltn | grep -q :9999'

  run docker_nes_cli -t tests/good/single-worker.yaml start "$(cat <<'EOF'
    SELECT * FROM TCP(
        'worker-1:8080' AS "SOURCE"."HOST",
        'tcp-peer' AS "SOURCE"."SOCKET_HOST",
        9999 AS "SOURCE"."SOCKET_PORT",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"
  assert_success
  query_id=$output

  wait_until_status tests/good/single-worker.yaml "Stopped" "$query_id" --require-healthy 'worker-1'
  # header + 3 rows
  wait_until assert_file_line_count worker-1/results.csv 4
}

@test "fails query when peer resets the connection mid-stream" {
  setup_distributed tests/good/single-worker.yaml
  docker compose exec -d tcp-peer python3 -c "
import socket, struct, time
connection, _ = socket.create_server(('', 9999)).accept()
connection.sendall(b'1\n2\n3\n')
time.sleep(1)
# Closing a socket with a zero linger timeout sends an RST instead of a FIN.
connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
connection.close()
"
  wait_until docker compose exec -T tcp-peer sh -c 'netstat -ltn | grep -q :9999'

  run docker_nes_cli -t tests/good/single-worker.yaml start "$(cat <<'EOF'
    SELECT * FROM TCP(
        'worker-1:8080' AS "SOURCE"."HOST",
        'tcp-peer' AS "SOURCE"."SOCKET_HOST",
        9999 AS "SOURCE"."SOCKET_PORT",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"
  assert_success
  query_id=$output

  wait_until_status tests/good/single-worker.yaml "Failed" "$query_id" --require-healthy 'worker-1'
}

@test "idle connection keeps the query running until it is stopped" {
  setup_distributed tests/good/single-worker.yaml
  # nc keeps the connection open for as long as its input stays open.
  docker compose exec -d tcp-peer sh -c "sleep infinity | nc -l -p 9999"
  wait_until docker compose exec -T tcp-peer sh -c 'netstat -ltn | grep -q :9999'

  # The connect timeout is also the receive timeout: the query has to survive several of them.
  run docker_nes_cli -t tests/good/single-worker.yaml start "$(cat <<'EOF'
    SELECT * FROM TCP(
        'worker-1:8080' AS "SOURCE"."HOST",
        'tcp-peer' AS "SOURCE"."SOCKET_HOST",
        9999 AS "SOURCE"."SOCKET_PORT",
        1 AS "SOURCE"."CONNECT_TIMEOUT_SECONDS",
        'CSV' AS "INPUT_FORMATTER"."TYPE",
        SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
    ) INTO FILE('CSV' AS "SINK"."OUTPUT_FORMAT", 'worker-1:8080' AS "SINK"."HOST", 'results.csv' AS "SINK"."FILE_PATH")
EOF
)"
  assert_success
  query_id=$output

  wait_until_status tests/good/single-worker.yaml "Running" "$query_id" --require-healthy 'worker-1'
  sleep 3
  wait_until_status tests/good/single-worker.yaml "Running" "$query_id" --require-healthy 'worker-1'

  run docker_nes_cli -t tests/good/single-worker.yaml stop "$query_id"
  assert_success
  wait_until_status tests/good/single-worker.yaml "Stopped" "$query_id" --require-healthy 'worker-1'
}
