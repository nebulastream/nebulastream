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

setup() {
  nes_distributed_setup
  # Records container lifecycle events with timestamps, e.g. to time the restart of the TCP peer.
  # Compose names the project after the test directory, lowercased and without characters other than [a-z0-9_-].
  local project
  project=$(basename "$TEST_DIR" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-')
  local event event_filters=()
  for event in create start restart kill die stop pause unpause oom health_status destroy; do
    event_filters+=(--filter "event=$event")
  done
  docker events --filter "label=com.docker.compose.project=$project" --filter type=container "${event_filters[@]}" \
    --format '{{.TimeNano}} {{index .Actor.Attributes "com.docker.compose.service"}} {{.Action}} {{index .Actor.Attributes "exitCode"}}' \
    > "$TEST_DIR/docker-events.raw" 2>&1 3>&- &
  TCP_SINK_EVENTS_PID=$!
}

teardown() {
  if [ -z "${BATS_TEST_COMPLETED:-}" ]; then
    dump_failure_diagnostics
  fi
  nes_distributed_teardown
  kill "$TCP_SINK_EVENTS_PID" 2>/dev/null || true
  wait "$TCP_SINK_EVENTS_PID" 2>/dev/null || true
  # Docker's format templates cannot format time, so convert the nanosecond timestamps to match the other logs.
  local nanoseconds rest
  [ -f "$TEST_DIR/docker-events.raw" ] || return 0
  while read -r nanoseconds rest; do
    echo "$(date -u -d "@${nanoseconds:0:-9}.${nanoseconds: -9}" +%Y-%m-%dT%H:%M:%S.%NZ) $rest"
  done < "$TEST_DIR/docker-events.raw" > "$TEST_DIR/docker-events.log"
  rm -f "$TEST_DIR/docker-events.raw"
}

# Writes the socket state of both TCP endpoints and a summary of the received rows to *.log files, which CI uploads,
# and to the test output. Send-Q on the worker together with Recv-Q on nc tell whether the peer stopped reading
# or the data never arrived; gaps and duplicates in the ids show data lost or resent around a reconnect.
dump_failure_diagnostics() {
  local service container
  for service in worker-1 tcp-client; do
    container=$(docker compose ps -q "$service" 2>/dev/null)
    {
      echo "Sockets of $service:"
      if [ -n "$container" ]; then
        # Only the sink connection on port 9000; the worker also holds many gRPC health-check sockets.
        docker run --rm --network "container:$container" busybox:1.38.0 netstat -tn 2>&1 | grep -E 'Proto|:9000 '
      else
        echo "not running"
      fi
    } | tee "$TEST_DIR/sockets-$service.log" | sed 's/^/# /' >&3
  done

  local ids distinct
  ids=$(cut -d, -f1 "$TEST_DIR/results.csv" 2>/dev/null | grep -E '^[0-9]+$' | sort -n)
  # grep -c exits non-zero when it counts nothing, which would abort the teardown.
  distinct=$(uniq <<< "$ids" | grep -c . || true)
  {
    echo "results.csv: $(stat -c %s "$TEST_DIR/results.csv" 2>/dev/null || echo 0) bytes, $(grep -c . <<< "$ids") rows with an id"
    if [ "$distinct" -gt 0 ]; then
      local first last
      first=$(head -n 1 <<< "$ids")
      last=$(tail -n 1 <<< "$ids")
      echo "ids $first..$last: $distinct distinct, $((last - first + 1 - distinct)) missing, $(uniq -d <<< "$ids" | grep -c .) duplicated"
    fi
  } | tee "$TEST_DIR/results-summary.log" | sed 's/^/# /' >&3
}

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
