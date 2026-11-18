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

# End-to-end tests of ISQP/ISQD (docs/technical/isqp_isqd.md): a query runs on a cluster of workers, a topology change moves one of its
# leaves while it runs, and the output must contain every result exactly once.

source "$NES_BATS_LIB"

setup_file()    { nes_distributed_setup_file "$NES_REPL" isqd; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

# Runs the statements of $1, waits until the query processed some data, runs the topology change of $2, and waits until the query
# terminated. Further arguments are passed to nes-repl.
repl_with_topology_change() {
  local setup_sql=$1
  local change_sql=$2
  shift 2
  tail -f /dev/null | docker compose exec nes-repl bash -c \
    "(cat /workdir/$setup_sql; sleep 2; cat /workdir/$change_sql) | nes-repl -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION $*"
}

# The COMMIT of the topology change amended one query without a failure.
assert_one_query_amended() {
  local commit
  commit=$(printf '%s\n' "${lines[@]}" | grep '"changes"')
  echo "# COMMIT: $commit" >&3
  echo "$commit" | jq -e '.[0].changes == 2 and .[0].affected_queries == 1 and .[0].failed_amendments == 0'
}

# Every id in 0..$2-1 is in the first column of the CSV file $1 exactly once. With a third argument, the second column equals the first.
assert_every_id_exactly_once() {
  local file=$1
  local count=$2
  local ids
  assert_file_exists "$file"
  ids=$(grep -E '^[0-9]' "$file" | cut -d, -f1 | sort -n)
  [ -z "$(echo "$ids" | uniq -d)" ] || fail "duplicate ids: $(echo "$ids" | uniq -d | head -5 | tr '\n' ' ')"
  [ "$(echo "$ids" | wc -l)" -eq "$count" ] || fail "expected $count ids, got $(echo "$ids" | wc -l)"
  [ "$(echo "$ids" | head -1)" -eq 0 ] || fail "smallest id is $(echo "$ids" | head -1)"
  [ "$(echo "$ids" | tail -1)" -eq $((count - 1)) ] || fail "largest id is $(echo "$ids" | tail -1)"
  if [ "$#" -eq 3 ]; then
    [ -z "$(grep -E '^[0-9]' "$file" | awk -F, '$1 != $2')" ] || fail "join results with different keys"
  fi
}

@test "a re-placed selection produces every tuple exactly once" {
  setup_distributed tests/topologies/isqd-5-node.yaml
  run repl_with_topology_change tests/sql-file-tests/isqd/stateless.sql tests/sql-file-tests/isqd/move_leaf_1.sql
  [ "$status" -eq 0 ]
  assert_one_query_amended
  assert_every_id_exactly_once "$TEST_DIR/sink-node/out.csv" 5000
}

@test "a moved join migrates its state and produces every result exactly once" {
  setup_distributed tests/topologies/isqd-5-node.yaml
  run repl_with_topology_change tests/sql-file-tests/isqd/join.sql tests/sql-file-tests/isqd/move_leaf_1.sql \
    -- --optimizer.join_strategy=NESTED_LOOP_JOIN
  [ "$status" -eq 0 ]
  assert_one_query_amended
  assert_every_id_exactly_once "$TEST_DIR/sink-node/out.csv" 5000 --join
  grep -q "sends its state" "$TEST_DIR/docker-compose.log" || fail "the join did not migrate its state"
}
