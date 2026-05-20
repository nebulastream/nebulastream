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

@test "launch query from topology" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -t tests/good/select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
}

@test "launch multiple query from topology" {
  setup_distributed tests/good/multiple-select-gen-into-void.yaml

  run docker_nes_cli -t tests/good/multiple-select-gen-into-void.yaml start
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  query_ids=("${lines[@]}")

  run docker_nes_cli -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[0]}"
  [ "$status" -eq 0 ]

  run docker_nes_cli -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[1]}" "${query_ids[2]}" "${query_ids[3]}" "${query_ids[4]}" "${query_ids[5]}"
  [ "$status" -eq 0 ]

  run docker_nes_cli -t tests/good/multiple-select-gen-into-void.yaml stop "${query_ids[6]}" "${query_ids[7]}"
  [ "$status" -eq 0 ]
}

@test "launch query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
}

@test "launch bad query from commandline" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -t tests/good/select-gen-into-void.yaml start 'selectaaa DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 1 ]
}

@test "launch and stop query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+_[0-9]{4}$ ]]
  QUERY_ID=$output

  sleep 1

  run docker_nes_cli -t tests/good/select-gen-into-void.yaml stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor query" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run docker_nes_cli -t tests/good/select-gen-into-void.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+_[0-9]{4}$ ]]
  QUERY_ID=$output

  sleep 1

  run docker_nes_cli -t tests/good/select-gen-into-void.yaml status "$QUERY_ID"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries" {
  setup_distributed tests/good/distributed-query-deployment.yaml

  run docker_nes_cli -t tests/good/distributed-query-deployment.yaml start 'select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK'
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+_[0-9]{4}$ ]]
  QUERY_ID=$output

  for i in $(seq 1 20); do
    sleep 1
    run docker_nes_cli -t tests/good/distributed-query-deployment.yaml status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "Running" ]; then
      break
    fi
  done
  echo "${output}" | jq -e '(. | length) == 3' # 1 global + 2 local
  [ "$QUERY_STATUS" = "Running" ]
}

@test "launch and monitor distributed queries crazy join" {
  setup_distributed tests/good/chained-joins.yaml

  run docker_nes_cli start
  [ "$status" -eq 0 ]
  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+_[0-9]{4}$ ]]
  QUERY_ID=$output

  sleep 1

  run docker_nes_cli status "$QUERY_ID"
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
  [ "$QUERY_STATUS" = "Running" ]

  run docker_nes_cli stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "launch and monitor distributed queries crazy join with a fast source" {
  setup_distributed tests/good/chained-joins-one-fast-source.yaml

  run docker_nes_cli start
  [ "$status" -eq 0 ]

  # Output should be a query ID (human-readable name)
  [[ "$output" =~ ^[a-z_]+_[0-9]{4}$ ]]
  QUERY_ID=$output

  # Poll until the fast source has stopped and the query becomes PartiallyStopped
  for i in $(seq 1 20); do
    sleep 1
    run docker_nes_cli status "$QUERY_ID"
    [ "$status" -eq 0 ]
    QUERY_STATUS=$(echo "$output" | jq -r --arg query_id "$QUERY_ID" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$QUERY_STATUS" = "PartiallyStopped" ]; then
      break
    fi
    # If the query already fully stopped, it won't go back to PartiallyStopped
    if [ "$QUERY_STATUS" = "Stopped" ]; then
      break
    fi
  done
  echo "${output}" | jq -e '(. | length) == 10' # 1 global + 9 local
  [ "$QUERY_STATUS" = "PartiallyStopped" ]

  run docker_nes_cli stop "$QUERY_ID"
  [ "$status" -eq 0 ]
}

@test "test worker not available" {
  setup_distributed tests/good/chained-joins.yaml

  docker compose stop worker-1

  run docker_nes_cli -d start

  sync_workdir
  grep "(5001) : query registration call failed; Status: UNAVAILABLE" nes-cli.log
  [ "$status" -eq 1 ]

  docker compose up -d --wait worker-1
  # now it should work
  run docker_nes_cli start
  [ "$status" -eq 0 ]
}

@test "worker goes offline during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run docker_nes_cli start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # This has to be kill not stop. Stop will gracefully shutdown the worker and all queries on that worker.
  # This would cause the query to fail as it was unexpectedly stopped. If we kill the worker: upstream and downstream
  # will wait for the "crashed" worker to return. However this test does not test that as it is currently not possible.
  docker compose kill worker-1
  run docker_nes_cli status "$QUERY_ID"
  [ "$status" -eq 0 ]

  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-2:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-3:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-8:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-7:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-4:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-9:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-5:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-6:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"
}

@test "worker goes offline and comes back during processing" {
  setup_distributed tests/good/chained-joins.yaml

  run docker_nes_cli start
  [ "$status" -eq 0 ]
  QUERY_ID=$output

  sleep 1

  # Simulate a crash by killing worker-1.
  docker compose kill worker-1
  run docker_nes_cli status "$QUERY_ID"
  [ "$status" -eq 0 ]

  sleep 1

  docker compose up -d --wait worker-1

# While this might not be the most intuitive nor the long-term solution this testcase documents the current behavior.
# The query running on worker-1 is terminated and on restart it is not restarted, this will cause subsequent status
# request to find that the previous local query id is not registered on worker-1, currently this is falsely reported as a ConnectionError.

  run docker_nes_cli status "$QUERY_ID"
  [ "$status" -eq 0 ]
  EXPECTED_STATUS_OUTPUT=$(cat <<EOF
[
  {
    "query_id": "$QUERY_ID",
    "query_status": "Unreachable"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "ConnectionError"
  }
]
EOF
)

  assert_json_contains "${EXPECTED_STATUS_OUTPUT}" "${output}"

  echo $output
}

@test "worker status" {
  setup_distributed tests/good/select-gen-into-void.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]
  query_id=$output

  sleep 1

  run docker_nes_cli status $query_id
  [ $status -eq 0 ]
  assert_json_contains "[{\"query_id\":\"$query_id\", \"query_status\":\"Running\", \"running\": {}, \"started\": {}}]" "$output"

  local_query_id=$(echo "$output" | jq -r '.[1].local_query_id')
  run docker_nes_cli status
  [ $status -eq 0 ]

  # Expect to find the local query in the worker status
  assert_json_contains "[{\"local_query_id\":\"$local_query_id\", \"query_status\":\"Running\", \"started\": {}}]" "$output"
}

@test "back pressure using worker config" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run docker_nes_cli stop $query_id
  # 0 means there is no overwrite and the worker default will be picked.
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "MAX_PENDING_ACKS: 0" worker-2/singleNodeWorker.log
  grep "SENDER_QUEUE_SIZE: 0" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "back pressure using optimizer flags" {
  setup_distributed tests/good/backpressure-optimizer-flags.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  run docker_nes_cli stop $query_id
  grep "host: worker-2:8080" worker-2/singleNodeWorker.log
  grep "MAX_PENDING_ACKS: 25" worker-2/singleNodeWorker.log
  grep "SENDER_QUEUE_SIZE: 32" worker-2/singleNodeWorker.log
  grep "Backpressure" worker-2/singleNodeWorker.log
  [ $status -eq 0 ]
}

@test "order of worker termination when backpressure is applied. terminate sink" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  docker compose stop worker-1

  # Poll until the sink closure propagates
  for i in $(seq 1 20); do
    sleep 1
    sync_workdir
    if grep -q "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done

  grep "Backpressure" worker-2/singleNodeWorker.log
  grep "NetworkSink was closed by other side" worker-2/singleNodeWorker.log
  grep "TaskCallback::callOnFailure" worker-2/singleNodeWorker.log

  run docker_nes_cli status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Failed"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "Failed",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "order of worker termination when backpressure is applied. terminate source" {
  setup_distributed tests/good/backpressure-worker-config.yaml

  run docker_nes_cli start
  [ $status -eq 0 ]
  query_id=$output

  # Poll until backpressure is observed in the worker log
  for i in $(seq 1 30); do
    sleep 1
    sync_workdir
    if grep -q "Backpressure" worker-2/singleNodeWorker.log 2>/dev/null; then
      break
    fi
  done
  grep "Backpressure" worker-2/singleNodeWorker.log

  docker compose stop worker-2
  sleep 2

  run docker_nes_cli status $query_id
  [ $status -eq 0 ]

  expected_json=$(cat <<EOF
  [
    {
      "query_status": "Unreachable"
    },
    {
      "query_status": "Running",
      "worker": "worker-1:8080"
    },
    {
      "query_status": "ConnectionError",
      "worker": "worker-2:8080"
    }
  ]
EOF
  )

  assert_json_contains "$expected_json" "$output"
}

@test "launch query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start'"
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

  sync_workdir
  grep "topology is not connected" nes-cli.log
}

@test "launch and stop query with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  # Output should be a query ID (numeric)
  QUERY_ID=$output

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - stop $QUERY_ID'"
  [ "$status" -eq 0 ]
}

@test "query status with topology from stdin" {
  setup_distributed tests/good/select-gen-into-void.yaml
  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - start \"select DOUBLE from GENERATOR_SOURCE INTO VOID_SINK\"'"
  [ "$status" -eq 0 ]

  # Output should be a query ID (numeric)
  QUERY_ID=$output

  sleep 1

  run bash -c "docker compose exec -T nes-cli bash -c 'cat tests/good/select-gen-into-void.yaml | nes-cli -t - status $QUERY_ID'"
  [ "$status" -eq 0 ]

  QUERY_STATUS=$(echo "$output" | jq -r '.[0].query_status')
  [ "$QUERY_STATUS" = "Running" ]
}
