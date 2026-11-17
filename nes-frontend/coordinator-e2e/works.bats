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

# What held up: these pass on 1cb7ad0730.

load lib.bash

setup() { common_setup; }
teardown() { common_teardown; }

@test "a bounded query over a 3-worker chain delivers exactly the expected rows" {
  for n in 1 2 3; do start_worker $n; done
  run repl --on-exit WAIT_FOR_QUERY_TERMINATION <<SQL
$(create_worker_sql 1 2)
$(create_worker_sql 2 3)
$(create_worker_sql 3)
CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);
$(bounded_source_sql 1)
$(file_sink_sql out 3 "$W/out.csv")
SELECT v * UINT64(2) AS v FROM seq WHERE v % UINT64(3) = UINT64(0) INTO out;
SQL
  [ "$status" -eq 0 ]
  # 0, 3, ..., 999 doubled: 334 rows that sum to 2 * 3 * (0 + ... + 333).
  [ "$(data_rows "$W/out.csv" | wc -l)" -eq 334 ]
  [ "$(data_rows "$W/out.csv" | awk '{s += $1} END {print s}')" -eq $((2 * 3 * 333 * 334 / 2)) ]
  [ "$(state_of 1)" = Completed ]
}

@test "physical sources on two workers both feed a sink on a third" {
  for n in 1 2 3; do start_worker $n; done
  run repl --on-exit WAIT_FOR_QUERY_TERMINATION <<SQL
$(create_worker_sql 1 3)
$(create_worker_sql 2 3)
$(create_worker_sql 3)
CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);
$(bounded_source_sql 1)
$(bounded_source_sql 2)
$(file_sink_sql out 3 "$W/out.csv")
SELECT v FROM seq INTO out;
SQL
  [ "$status" -eq 0 ]
  [ "$(data_rows "$W/out.csv" | wc -l)" -eq 2000 ]
  # Every value exactly twice, once from each source.
  [ "$(data_rows "$W/out.csv" | sort -n | uniq -c | awk '$1 != 2' | wc -l)" -eq 0 ]
}

@test "a query created while its worker is down starts once the worker comes up" {
  {
    create_worker_sql 1
    echo "CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);"
    endless_source_sql 1
    file_sink_sql out 1 "$W/out.csv"
    echo "SELECT v FROM seq INTO out;"
    sleep 4
    echo "SHOW QUERIES;"
    start_worker 1 >&2
    sleep 1
    await_rows "$W/out.csv"
    echo "SHOW QUERIES;"
    echo "DROP QUERY;"
    sleep 4
  } | repl -e RECOVER >"$W/session.json" 2>"$W/session.err"
  jq -c '.Queries // empty | .[].query | {state, error}' "$W/session.json"
  has_rows "$W/out.csv"
}

@test "DROP QUERY right after its creation stops the query on the worker" {
  start_worker 1
  run repl <<SQL
$(create_worker_sql 1)
CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);
$(endless_source_sql 1)
$(file_sink_sql out 1 "$W/out.csv")
SQL
  [ "$status" -eq 0 ]
  # One session: create, drop at once, then stay alive so the coordinator can act on both.
  { echo "SELECT v FROM seq INTO out;"; echo "DROP QUERY WHERE id = 1;"; sleep 6; echo "SHOW QUERIES;"; } | repl >"$W/session.json"
  stopped "$W/out.csv"
  [ "$(state_of 1)" = Stopped ]
}

@test "DROP QUERY at any moment of the start (0 ms to 1.5 s after creation) stops the query for good" {
  start_worker 1
  local delays=(0 0.05 0.1 0.2 0.3 0.5 0.75 1 1.5)
  {
    create_worker_sql 1
    echo "CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);"
    endless_source_sql 1
    local i=0
    for d in "${delays[@]}"; do
      i=$((i + 1))
      file_sink_sql "out$i" 1 "$W/q$i.csv"
      echo "SELECT v FROM seq INTO out$i;"
      sleep "$d"
      echo "DROP QUERY WHERE id = $i;"
    done
    sleep 8
    echo "SHOW QUERIES;"
  } | repl >"$W/session.json"
  local before=()
  for i in $(seq ${#delays[@]}); do before[i]=$(lines "$W/q$i.csv"); done
  sleep 5
  local zombies=0
  for i in $(seq ${#delays[@]}); do
    local state now
    state=$(jq -r ".Queries // empty | .[].query | select(.id == $i) | .state" "$W/session.json")
    now=$(lines "$W/q$i.csv")
    echo "drop after ${delays[i - 1]} s: state $state, sink lines ${before[i]} -> $now"
    [ "$now" -gt "${before[i]}" ] && zombies=$((zombies + 1))
  done
  [ "$zombies" -eq 0 ]
}

@test "a query started by one REPL is stopped by DROP QUERY from a later REPL on the same catalog" {
  start_worker 1
  # The first session stays until the sink receives data, then exits and leaves the query running (DO_NOTHING).
  {
    create_worker_sql 1
    echo "CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);"
    endless_source_sql 1
    file_sink_sql out 1 "$W/out.csv"
    echo "SELECT v FROM seq INTO out;"
    await_rows "$W/out.csv"
  } | repl >/dev/null
  # Still flowing with no coordinator alive.
  growing "$W/out.csv"
  { echo "DROP QUERY WHERE id = 1;"; sleep 5; } | repl >/dev/null
  stopped "$W/out.csv"
  [ "$(state_of 1)" = Stopped ]
}

@test "DROP QUERY WHERE name = ... stops a named query started by nes-cli" {
  alert_setup
  run start "$(alert a 0 my_alert)"
  [ "$status" -eq 0 ]
  growing "$W/alert_a.csv"
  run sql "DROP QUERY WHERE name = 'my_alert';"
  [ "$status" -eq 0 ]
  stopped "$W/alert_a.csv"
}

@test "DROP WORKER of a worker that runs a fragment fails the query and stops the fragment" {
  start_worker 1
  {
    create_worker_sql 1
    echo "CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);"
    endless_source_sql 1
    file_sink_sql out 1 "$W/out.csv"
    echo "SELECT v FROM seq INTO out;"
    await_rows "$W/out.csv"
    echo "DROP WORKER WHERE HOST = '$(worker_addr 1)';"
    sleep 5
    echo "SHOW QUERIES;"
  } | repl -e RECOVER >"$W/session.json"
  jq -c '.Queries // empty | .[].query | {state, error}' "$W/session.json"
  stopped "$W/out.csv"
  [ "$(jq -r '.Queries // empty | .[].query.state' "$W/session.json")" = Failed ]
}

@test "a worker that restarts while no coordinator runs: the next coordinator reports the query truthfully" {
  start_worker 1
  {
    create_worker_sql 1
    echo "CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);"
    endless_source_sql 1
    file_sink_sql out 1 "$W/out.csv"
    echo "SELECT v FROM seq INTO out;"
    await_rows "$W/out.csv"
  } | repl >/dev/null
  # The worker crashes and comes back empty while no frontend is running.
  kill_worker 1
  start_worker 1
  local a
  a=$(lines "$W/out.csv")
  { sleep 8; echo "SHOW QUERIES;"; } | repl >"$W/later.json"
  jq -c '.Queries // empty | .[].query | {state, error}' "$W/later.json"
  local state
  state=$(jq -r '.Queries // empty | .[].query.state' "$W/later.json")
  # Either the query runs again (and the sink grows), or it is not reported as Running.
  if [ "$state" = Running ]; then [ "$(lines "$W/out.csv")" -gt "$a" ]; fi
}

@test "nes-cli: start returns once the query runs; status and stop from later invocations act on it" {
  start_worker 1
  write_setup "$W/setup.yaml" "$W/out.csv" 100000000
  run cli -s "$W/setup.yaml" start
  [ "$status" -eq 0 ]
  [ "$output" = 1 ]
  wait_until 10 has_rows "$W/out.csv"
  [ "$(cli status --wait 5 1 | jq -r '.[0].state')" = Running ]
  run cli stop --wait 10 1
  [ "$status" -eq 0 ]
  stopped "$W/out.csv"
  [ "$(cli status 1 | jq -r '.[0].state')" = Stopped ]
}

@test "nes-cli: status and a second start while start --until-completed runs leave the first query's output exact" {
  start_worker 1
  # About 20 s of data, so the other invocations overlap with it.
  write_setup "$W/setup1.yaml" "$W/out1.csv" 2000
  cli -s "$W/setup1.yaml" start --until-completed >"$W/start1.out" 2>"$W/start1.err" &
  local starter=$!
  wait_until 15 has_rows "$W/out1.csv"
  for _ in 1 2 3; do
    cli status >/dev/null
    sleep 1
  done
  write_setup "$W/setup2.yaml" "$W/out2.csv" 100000000
  sed -i 's/^  - name: out$/  - name: out2/; s/INTO out$/INTO out2/' "$W/setup2.yaml"
  run cli -s "$W/setup2.yaml" start
  local rc=0
  wait "$starter" || rc=$?
  echo "first start: rc $rc, stderr $(head -c 300 "$W/start1.err")"
  [ "$rc" -eq 0 ]
  [ "$(data_rows "$W/out1.csv" | wc -l)" -eq 2000 ]
  [ "$(data_rows "$W/out1.csv" | sort -u | wc -l)" -eq 2000 ]
}
