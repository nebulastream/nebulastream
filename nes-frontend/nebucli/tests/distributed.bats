#!/usr/bin/env bats

bats_require_minimum_version 1.5.0

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

query_ids() {
  echo "$1" | jq -r '.[].id'
}

query_state() {
  local id="$1"
  local json="$2"
  echo "$json" | jq -r --argjson id "$id" '.[] | select(.id == $id) | .state'
}

fragment_count() {
  local id="$1"
  local json="$2"
  echo "$json" | jq --argjson id "$id" '.[] | select(.id == $id) | .fragments | length'
}

fragment_state() {
  local query_id="$1"
  local host="$2"
  local json="$3"
  echo "$json" | jq -r --argjson id "$query_id" --arg host "$host" \
    '.[] | select(.id == $id) | .fragments[] | select(.host_addr == $host) | .current_state'
}

worker_state() {
  local query_id="$1"
  local host="$2"
  local json="$3"
  echo "$json" | jq -r --argjson id "$query_id" --arg host "$host" \
    '.[] | select(.id == $id) | .fragments[] | select(.host_addr == $host) | .worker_state'
}

@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -s tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}

@test "launch multiple queries from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml
  run docker_nes_cli -s tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  local count=$(echo "$output" | jq 'length')
  [ "$count" -eq 8 ]

  local all_ids
  all_ids=$(query_ids "$output")

  run docker_nes_cli stop $(echo "$all_ids" | sed -n '1p')
  [ "$status" -eq 0 ]

  run docker_nes_cli stop $(echo "$all_ids" | sed -n '2p') $(echo "$all_ids" | sed -n '3p') $(echo "$all_ids" | sed -n '4p') $(echo "$all_ids" | sed -n '5p') $(echo "$all_ids" | sed -n '6p')
  [ "$status" -eq 0 ]

  run docker_nes_cli stop $(echo "$all_ids" | sed -n '7p') $(echo "$all_ids" | sed -n '8p')
  [ "$status" -eq 0 ]
}

@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "launch query with quoted identifiers and uppercase compatibility" {
  setup_distributed tests/good/quoted-identifiers.yaml
  run docker_nes_cli -s tests/good/quoted-identifiers.yaml start \
    'SELECT "mixedValue" AS "projectedValue", A FROM "quotedSource" INTO "quotedSink"'
  [ "$status" -eq 0 ]
}

@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -s tests/good/select-gen-into-void.yaml start 'selectaaa DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  sleep 1

  run docker_nes_cli stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run --separate-stderr docker_nes_cli -s tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  sleep 1

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run docker_nes_cli -s tests/good/distributed-query-deployment.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  nes_cli_wait 15 'any(.[]; .state == "Running")'
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/chained-joins.yaml

  run --separate-stderr docker_nes_cli start
  [ "$status" -eq 0 ]

  nes_cli_wait 15 'any(.[]; .state == "Running")'
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
  FRAG_COUNT=$(fragment_count "$QUERY_ID" "$output")
  [ "$FRAG_COUNT" -eq 9 ]

  run docker_nes_cli stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/chained-joins-one-fast-source.yaml

  run docker_nes_cli start
  [ "$status" -eq 0 ]

  nes_cli_wait 40 'any(.[]; .state == "Running" and any(.fragments[]; .current_state == "Completed"))'
  QUERY_ID=$(query_ids "$output" | head -1)
  COMPLETED=$(echo "$output" | jq --argjson id "$QUERY_ID" \
    '[.[] | select(.id == $id) | .fragments[] | select(.current_state == "Completed")] | length')

  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
  [ "$COMPLETED" -gt 0 ]

  run docker_nes_cli stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/chained-joins.yaml

  docker compose stop worker-1

  run docker_nes_cli -d start
  [ "$status" -eq 1 ]

  for i in $(seq 1 60); do
    grep -qi "unreachable" nes-server/nes-server.log 2>/dev/null && break
    sleep 1
  done
  grep -i "unreachable" nes-server/nes-server.log

  docker compose up -d --wait worker-1
  # Now it should work
  run docker_nes_cli start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run --separate-stderr docker_nes_cli start
  [ "$status" -eq 0 ]

  sleep 1

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]

  docker compose kill worker-1

  nes_cli_wait 60 "any(.[] | select(.id == $QUERY_ID) | .fragments[]; .host_addr == \"worker-1:8080\" and .worker_state == \"Unreachable\")"
  WORKER1_STATE=$(worker_state "$QUERY_ID" "worker-1:8080" "$output")
  [ "$WORKER1_STATE" = "Unreachable" ]

  # Query stays Running — the system does not auto-fail on unreachability.
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run --separate-stderr docker_nes_cli start
  [ "$status" -eq 0 ]

  sleep 1

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  docker compose kill worker-1
  sleep 2
  docker compose up -d --wait worker-1

  # Auto-recovery: worker comes back, fragments re-register and resume
  nes_cli_wait 60 "any(.[]; .id == $QUERY_ID and .state == \"Running\")"
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Running" ]
}

@test "worker status includes fragments" {
  setup_distributed tests/good/select-gen-into-void.yaml

  run --separate-stderr docker_nes_cli -s tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]

  sleep 1

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  # Status should include fragments with host_addr
  FRAG_COUNT=$(fragment_count "$QUERY_ID" "$output")
  [ "$FRAG_COUNT" -gt 0 ]

  FRAG_HOST=$(echo "$output" | jq -r --argjson id "$QUERY_ID" \
    '.[] | select(.id == $id) | .fragments[0].host_addr')
  [ -n "$FRAG_HOST" ]
  [ "$FRAG_HOST" != "null" ]
}

@test "launch query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start'"
  [ "$status" -eq 0 ]
}

@test "launch query using 3-nodes topology" {
  setup_distributed tests/good/3-nodes.yaml
  run docker_nes_cli start
  [ "$status" -eq 0 ]
}

@test "placement fails with reversed downstream edges" {
  setup_distributed tests/bad/3-nodes-reversed-edges.yaml
  run docker_nes_cli start
  [ "$status" -eq 1 ]
}

@test "launch and stop query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_ID=$(query_ids "$output" | head -1)

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'nes-cli stop $QUERY_ID'"
  [ "$status" -eq 0 ]
}

@test "query status with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -s - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  sleep 1

  run --separate-stderr docker_nes_cli status
  [ "$status" -eq 0 ]
  QUERY_STATE=$(echo "$output" | jq -r '.[0].state')
  [ "$QUERY_STATE" = "Running" ]
}

@test "back pressure using worker config" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]

  run --separate-stderr docker_nes_cli status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run docker_nes_cli stop --wait 60 $QUERY_ID
  # 0 means there is no overwrite and the worker default will be picked.
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "MAX_PENDING_ACKS: 0" worker-2/singleNodeWorker.log
  grep "SENDER_QUEUE_SIZE: 0" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
}

@test "back pressure using optimizer flags" {
  setup_distributed tests/good/backpressure-optimizer-flags.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]

  run --separate-stderr docker_nes_cli status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run docker_nes_cli stop --wait 60 $QUERY_ID
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "MAX_PENDING_ACKS: 25" worker-2/singleNodeWorker.log
  grep "SENDER_QUEUE_SIZE: 32" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run --separate-stderr docker_nes_cli start
  [ $status -eq 0 ]

  run --separate-stderr docker_nes_cli status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  docker compose stop worker-1

  # Poll until the failure propagates
  for i in $(seq 1 20); do
    sleep 1
    if grep -q "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  grep "Backpressure" worker-2/singleNodeWorker.log
  grep "NetworkSink was closed by other side" worker-2/singleNodeWorker.log
  grep "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log

  nes_cli_wait 60 "any(.[]; .id == $QUERY_ID and .state == \"Failed\")"
  QUERY_STATE=$(query_state "$QUERY_ID" "$output")
  [ "$QUERY_STATE" = "Failed" ]
}

@test "order of worker termination when backpressure is applied. terminate source" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run --separate-stderr docker_nes_cli start
  [ $status -eq 0 ]

  run --separate-stderr docker_nes_cli status
  QUERY_ID=$(query_ids "$output" | head -1)

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done
  grep "Backpressure" worker-2/singleNodeWorker.log

  docker compose stop worker-2
  sleep 2

  # worker-1 (sink) should still be running, but overall query should reflect the failure
  run --separate-stderr docker_nes_cli status
  [ $status -eq 0 ]
  WORKER1_STATE=$(fragment_state "$QUERY_ID" "worker-1:8080" "$output")
  [ "$WORKER1_STATE" = "Running" ]
}
