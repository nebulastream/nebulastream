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

# One test per finding of the review on PR #1577, numbered as there. Each test asserts what we would expect, so on
# 1cb7ad0730 every test in this file fails. A failing test prints what happened instead.

load lib.bash

setup() { common_setup; }
teardown() { common_teardown; }

# --- 1. A second catalog on a worker ---------------------------------------------------------------------------------
# A coordinator stops every fragment on its workers that its catalog does not know ("stopping a fragment that the
# catalog does not want on this worker"), and query ids are numbered per catalog. Application A and application B
# below each keep their own catalog (nes-cli's, under their own XDG_STATE_HOME) and share worker 1.

@test "[1] a nes-repl session with its own catalog that registers a worker leaves the queries running there alone" {
  alert_setup
  start "$(alert a 0)"
  growing "$W/alert_a.csv"
  # The REPL's default in-memory catalog; the session only registers the worker.
  { create_worker_sql 1; sleep 6; } | "$REPL_BIN" -f JSON >/dev/null
  echo "nes-cli's query 1 after the REPL session: $(cli status --wait 5 1 | jq -r '.[0].state')"
  growing "$W/alert_a.csv"
}

@test "[1] two catalogs on one worker: B's start leaves A's queries running" {
  alert_setup
  XDG_STATE_HOME=$W/app_a start "$(alert a 0)" "$(alert c 0)"
  growing "$W/alert_c.csv"
  XDG_STATE_HOME=$W/app_b start "$(alert b 0)" || true
  echo "A's query 2 after B started its query 1: $(XDG_STATE_HOME=$W/app_a cli status --wait 5 2 | jq -r '.[0].state')"
  growing "$W/alert_c.csv"
}

@test "[1] two catalogs on one worker: a start the worker rejects is not reported as success" {
  alert_setup
  # Both first queries are query 1.
  XDG_STATE_HOME=$W/app_a run start "$(alert a 0)"
  [ "$status" -eq 0 ]
  growing "$W/alert_a.csv"
  XDG_STATE_HOME=$W/app_b run start "$(alert b 0)"
  echo "B's start: status $status, output: $output"
  echo "B's catalog says: $(XDG_STATE_HOME=$W/app_b cli status --wait 5 1 | jq -r '.[0].state')"
  # Either B's start fails, or B's query runs.
  [ "$status" -ne 0 ] || growing "$W/alert_b.csv"
}

@test "[1] after the nes-cli catalog is deleted, a start is not reported as success for a query that does not run" {
  alert_setup
  start "$(alert a 0)"
  growing "$W/alert_a.csv"
  # The way out of finding 3: delete the catalog. The old query 1 keeps running on the worker.
  rm -f "$XDG_STATE_HOME"/nebucli/coordinator.db*
  run start "$(alert b 0)"
  echo "start on the new catalog: status $status, output: $output"
  [ "$status" -ne 0 ] || growing "$W/alert_b.csv"
}

# --- 2. A script piped into nes-repl with the defaults ---------------------------------------------------------------

@test "[2] a script piped into nes-repl with its defaults starts every query it created" {
  # In our runs this failed 5 of 5 times: 4 times no query started, once 2 of the 4.
  start_worker 1
  run "$REPL_BIN" -f JSON <<SQL
$(create_worker_sql 1)
CREATE LOGICAL SOURCE seq(v UINT64, ts UINT64);
$(bounded_source_sql 1)
$(file_sink_sql out1 1 "$W/q1.csv")
$(file_sink_sql out2 1 "$W/q2.csv")
$(file_sink_sql out3 1 "$W/q3.csv")
$(file_sink_sql out4 1 "$W/q4.csv")
SELECT v FROM seq INTO out1;
SELECT v FROM seq INTO out2;
SELECT v FROM seq INTO out3;
SELECT v FROM seq INTO out4;
SQL
  [ "$status" -eq 0 ]
  [ "$(printf '%s' "$output" | jq -s '[.[] | select(has("CreatedQuery"))] | length')" -eq 4 ]
  # The REPL reported all four as created and exited 0, so all four should run to the end.
  sleep 5
  for q in 1 2 3 4; do echo "query $q: $(data_rows "$W/q$q.csv" | wc -l) of 1000 rows"; done
  for q in 1 2 3 4; do [ "$(data_rows "$W/q$q.csv" | wc -l)" -eq 1000 ]; done
}

# --- 3. nes-cli keeps old definitions --------------------------------------------------------------------------------

@test "[3] nes-cli: after editing the sink in setup.yaml, a new start writes where the file now says" {
  start_worker 1
  write_setup "$W/setup.yaml" "$W/before.csv" 100000000
  run cli -s "$W/setup.yaml" start
  [ "$status" -eq 0 ]
  cli stop --wait 10 1
  # The user points the sink at a new file and starts again.
  sed -i "s|$W/before.csv|$W/after.csv|" "$W/setup.yaml"
  rm -f "$W/before.csv"
  run cli -s "$W/setup.yaml" start
  [ "$status" -eq 0 ]
  sleep 4
  echo "second start printed $output; before.csv: $(lines "$W/before.csv") lines, after.csv: $(lines "$W/after.csv") lines"
  [ "$(lines "$W/after.csv")" -gt 1 ]
}

# --- 4. Query names --------------------------------------------------------------------------------------------------

@test "[4] a second query under a name that is in use is rejected" {
  # The review asks whether query names are meant to be unique, as the names of sinks and logical sources are; this
  # test assumes they are. Today the second start creates a second query, and DROP QUERY WHERE name = 'my_alert'
  # drops both.
  alert_setup
  start "$(alert a 0 my_alert)"
  run start "$(alert b 0 my_alert)"
  echo "second start under the same name: status $status, output: $output"
  [ "$status" -ne 0 ]
}

@test "[4] nes-cli stop accepts a query name" {
  alert_setup
  start "$(alert a 0 my_alert)"
  run cli stop --wait 10 my_alert
  echo "status $status: $output"
  [ "$status" -eq 0 ]
  stopped "$W/alert_a.csv"
}

# --- 5. A workload file can't be applied twice -----------------------------------------------------------------------

# The whole workload as one SQL file: worker, source, sinks and two named queries.
workload_sql() {
  create_worker_sql 1
  echo "CREATE LOGICAL SOURCE readings(value UINT64, ts UINT64);"
  echo "CREATE PHYSICAL SOURCE FOR readings TYPE Generator SET('ALL' AS \"SOURCE\".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES, 'CSV' AS INPUT_FORMATTER.\"TYPE\", '$(worker_addr 1)' AS \"SOURCE\".\"HOST\", 'emit_rate 100' AS \"SOURCE\".GENERATOR_RATE_CONFIG, 'SEQUENCE UINT64 0 100000000 1, SEQUENCE UINT64 0 100000000 1' AS \"SOURCE\".GENERATOR_SCHEMA);"
  for s in a b; do
    echo "CREATE SINK alert_$s(value UINT64) TYPE File SET('$W/alert_$s.csv' AS \"SINK\".FILE_PATH, 'CSV' AS \"SINK\".OUTPUT_FORMAT, '$(worker_addr 1)' AS \"SINK\".\"HOST\");"
    echo "$(alert $s 0 "workload_$s");"
  done
}

# apply_workload [repl options...]: run the workload file on this test's catalog, as after a restart of the
# application; the REPL stays 5 s so its coordinator can start the queries.
apply_workload() { { workload_sql; sleep 5; } | repl "$@"; }

query_count() { { sleep 3; echo "SHOW QUERIES;"; } | repl | jq -s '[.[] | .Queries // empty | .[]] | length'; }

@test "[5] the same workload file runs again on a persisted catalog" {
  start_worker 1
  apply_workload >/dev/null
  # FAIL_FAST is the REPL's default for a script.
  run apply_workload
  echo "second run: status $status, $(printf '%s' "$output" | grep -i error | head -2 | cut -c1-200)"
  [ "$status" -eq 0 ]
  [[ "$output" != *"constraint failed"* ]]
}

@test "[5] the same workload file run again with -e RECOVER does not duplicate its queries" {
  start_worker 1
  apply_workload >/dev/null
  apply_workload -e RECOVER >/dev/null || true
  local n
  n=$(query_count)
  echo "queries after applying the file twice: $n"
  [ "$n" -eq 2 ]
}

# --- 6. Error messages -----------------------------------------------------------------------------------------------

@test "[6] CREATE WORKER accepts an IPv4 address, as the catalog does" {
  run repl <<<"CREATE WORKER '127.0.0.1:$(worker_port 1)' SET ('127.0.0.1:$(($(worker_port 1) + 1))' AS DATA);"
  echo "status $status: $output"
  [ "$status" -eq 0 ]
}

@test "[6] CREATE SINK on a host that is no worker names the host" {
  run repl <<<"CREATE SINK out(V UINT64) TYPE File SET('$W/out.csv' AS \"SINK\".FILE_PATH, 'CSV' AS \"SINK\".OUTPUT_FORMAT, 'nohost:1' AS \"SINK\".\"HOST\");"
  echo "status $status: $output"
  [ "$status" -ne 0 ]
  [[ "$output" == *nohost* ]]
}

@test "[6] CREATE WORKER for a worker that exists says so, not with an SQLite constraint error" {
  run repl <<<"$(create_worker_sql 1)"
  [ "$status" -eq 0 ]
  run repl <<<"$(create_worker_sql 1)"
  echo "status $status: $output"
  [ "$status" -ne 0 ]
  [[ "$output" != *"constraint failed"* ]]
}

# --- 7. Leftovers ----------------------------------------------------------------------------------------------------

@test "[7] nes-repl does not silently ignore --server" {
  run "$REPL_BIN" -s nowhere.invalid:1 -f JSON <<<"SHOW WORKERS;"
  echo "status $status: $output"
  [ "$status" -ne 0 ]
}
