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

setup_file()    { nes_distributed_setup_file "$NES_CLI" kafka-source; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

TOPOLOGY=tests/good/example.yaml
KAFKA_BIN=/opt/kafka/bin

kafka() { docker compose exec -T kafka-client "$@"; }

create_topic() {
  kafka $KAFKA_BIN/kafka-topics.sh --bootstrap-server kafka-broker:9092 --create --topic "$1" --partitions "${2:-3}" --replication-factor 1
}

# produce <topic> < lines: round-robin over the partitions
produce() {
  kafka $KAFKA_BIN/kafka-console-producer.sh --bootstrap-server kafka-broker:9092 --topic "$1" \
    --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner
}

rows() { seq "$1" "$2" | awk '{ print $1 "," $1 * 10 }'; }

group_lag_total() {
  kafka $KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server kafka-broker:9092 --describe --group "$1" 2>/dev/null \
    | awk '$1 == "'"$1"'" && $6 ~ /^[0-9]+$/ { lag += $6; n++ } END { if (n == 0) exit 1; print lag }'
}

# wait_until_within <seconds> <command...>: like wait_until, with a deadline that fits a cold broker
wait_until_within() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "timed out waiting for: $*"
      return 1
    fi
    sleep 0.2
  done
}

# start_query <topology>: sets query_id and waits until the query runs
start_query() {
  run docker_nes_cli -t "$1" start
  assert_success
  query_id=$output
  wait_until_status "$1" "Running" "$query_id" --require-healthy "worker-1"
}

stop_query() {
  run docker_nes_cli -t "$TOPOLOGY" stop "$1"
  assert_success
  wait_until_status "$TOPOLOGY" "Stopped" "$1"
}

data_rows() { grep -E '^[0-9]+,[0-9]+$' worker-1/out.csv 2>/dev/null; }
data_row_count() { data_rows | wc -l; }
unique_ids() { data_rows | cut -d, -f1 | sort -un | wc -l; }
export -f data_rows data_row_count unique_ids

@test "every message of every partition arrives once and the group lag is 0" {
  setup_distributed "$TOPOLOGY"
  create_topic sensors 3
  rows 1 10000 | produce sensors
  start_query "$TOPOLOGY"

  # The first query joins on a cold broker (offsets topic creation), so give the rows a generous deadline.
  wait_until_within 90 bash -c '[ "$(data_row_count)" -ge 10000 ]'
  stop_query "$query_id"

  [ "$(data_row_count)" -eq 10000 ]
  [ "$(unique_ids)" -eq 10000 ]
  # every row is intact: value = id * 10
  run awk -F, '$2 != $1 * 10 { bad++ } END { print bad + 0 }' worker-1/out.csv
  assert_output "0"
  [ "$(group_lag_total nes-sensors)" -eq 0 ]
}

@test "restart with the same group loses nothing and duplicates at most one buffer per partition" {
  setup_distributed "$TOPOLOGY"
  create_topic sensors 3
  rows 1 5000 | produce sensors
  start_query "$TOPOLOGY"
  wait_until_within 90 bash -c '[ "$(data_row_count)" -ge 5000 ]'
  stop_query "$query_id"

  rows 5001 10000 | produce sensors
  start_query "$TOPOLOGY"
  wait_until_within 90 bash -c '[ "$(unique_ids)" -eq 10000 ]'
  stop_query "$query_id"

  [ "$(unique_ids)" -eq 10000 ]
  # one buffer holds at most 4 KiB of ~12-byte rows, per partition
  duplicates=$(( $(data_row_count) - 10000 ))
  echo "# duplicates after restart: $duplicates" >&3
  [ "$duplicates" -le 1100 ]
}

@test "the query fails when no broker is reachable at startup" {
  setup_distributed tests/good/unreachable.yaml
  run docker_nes_cli -t tests/good/unreachable.yaml start
  assert_success
  wait_until_status tests/good/unreachable.yaml "Failed" "$output" --require-healthy "worker-1"
}
