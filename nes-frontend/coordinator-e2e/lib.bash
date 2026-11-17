# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Helpers for the coordinator e2e suite. A test is what a user does: start worker processes, talk to the coordinator
# only through nes-repl / nes-cli, and judge the outcome by what a user can see (statement output, exit code, rows in
# a sink file). Every test gets its own directory, its own catalog and its own ports.

NES_BUILD=${NES_BUILD:-/build}
WORKER_BIN=$NES_BUILD/nes-single-node-worker/nes-single-node-worker
REPL_BIN=$NES_BUILD/nes-frontend/repl/nes-repl
CLI_BIN=$NES_BUILD/nes-frontend/nebucli/nes-cli

common_setup() {
  for bin in "$WORKER_BIN" "$REPL_BIN" "$CLI_BIN"; do
    [ -x "$bin" ] || { echo "missing $bin (set NES_BUILD to the build directory)" >&3; return 1; }
  done
  W=$BATS_TEST_TMPDIR
  # The REPL's catalog; nes-cli keeps its own under XDG_STATE_HOME, which is per test too.
  DB=$W/catalog.db
  export XDG_STATE_HOME=$W/state
  PORT0=$((20000 + ${BATS_SUITE_TEST_NUMBER:-$BATS_TEST_NUMBER} * 100))
  cd "$W" || return 1
}

common_teardown() {
  for pidfile in "$W"/*/pid; do
    [ -f "$pidfile" ] && kill -9 "$(cat "$pidfile")" 2>/dev/null
  done
  return 0
}

wait_port() {
  for _ in $(seq 200); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.05
  done
  echo "port $1 never opened" >&3
  return 1
}

# worker_port N: the gRPC port of the N-th worker of this test; its data port is one above.
worker_port() { echo $((PORT0 + 2 * $1)); }
worker_addr() { echo "localhost:$(worker_port "$1")"; }
worker_data_addr() { echo "localhost:$(($(worker_port "$1") + 1))"; }

# start_worker N [worker options...]
start_worker() {
  local n=$1
  shift
  local port
  port=$(worker_port "$n")
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is already taken (a leftover worker?)" >&3
    return 1
  fi
  mkdir -p "$W/worker$n"
  (cd "$W/worker$n" && exec "$WORKER_BIN" -- --grpc="[::]:$port" --data_address="localhost:$((port + 1))" "$@" >>worker.out 2>&1) &
  echo $! >"$W/worker$n/pid"
  wait_port "$port"
}

# kill_worker N [SIGNAL]: returns once the process is gone and its gRPC port is free again.
kill_worker() {
  local pid
  pid=$(cat "$W/worker$1/pid")
  kill -"${2:-9}" "$pid" 2>/dev/null
  wait_until 30 eval "! kill -0 $pid 2>/dev/null"
  wait_until 30 eval "! (exec 3<>/dev/tcp/127.0.0.1/$(worker_port "$1")) 2>/dev/null"
  rm -f "$W/worker$1/pid"
}

# create_worker_sql N [peer N...]: the statement that registers worker N, with its downstream peers.
create_worker_sql() {
  local n=$1
  shift
  local peers=""
  for p in "$@"; do peers+=", '$(worker_addr "$p")' AS DOWNSTREAM"; done
  echo "CREATE WORKER '$(worker_addr "$n")' SET ('$(worker_data_addr "$n")' AS DATA$peers);"
}

# A bounded source on worker N: v and ts both count 0..999, then the source stops.
bounded_source_sql() {
  echo "CREATE PHYSICAL SOURCE FOR seq TYPE Generator SET('ALL' AS \"SOURCE\".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES, 'CSV' AS INPUT_FORMATTER.\"TYPE\", '$(worker_addr "$1")' AS \"SOURCE\".\"HOST\", 'SEQUENCE UINT64 0 1000 1, SEQUENCE UINT64 0 1000 1' AS \"SOURCE\".GENERATOR_SCHEMA);"
}

# An endless source on worker N: 100 tuples a second for far longer than any test runs.
endless_source_sql() {
  echo "CREATE PHYSICAL SOURCE FOR seq TYPE Generator SET('ALL' AS \"SOURCE\".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES, 'CSV' AS INPUT_FORMATTER.\"TYPE\", '$(worker_addr "$1")' AS \"SOURCE\".\"HOST\", 'emit_rate 100' AS \"SOURCE\".GENERATOR_RATE_CONFIG, 'SEQUENCE UINT64 0 100000000 1, SEQUENCE UINT64 0 100000000 1' AS \"SOURCE\".GENERATOR_SCHEMA);"
}

# file_sink_sql NAME WORKER FILE: a CSV file sink with the single column V.
file_sink_sql() {
  echo "CREATE SINK $1(V UINT64) TYPE File SET('$3' AS \"SINK\".FILE_PATH, 'CSV' AS \"SINK\".OUTPUT_FORMAT, '$(worker_addr "$2")' AS \"SINK\".\"HOST\");"
}

# repl [repl options...] <<< "SQL": one nes-repl process on this test's persistent catalog, JSON output.
repl() { "$REPL_BIN" -f JSON --db "$DB" "$@"; }

# sql STATEMENTS [SECONDS]: one REPL process on the catalog that stays SECONDS (default 5) so its coordinator can act.
sql() { { echo "$1"; sleep "${2:-5}"; } | repl -e RECOVER; }

cli() { "$CLI_BIN" "$@"; }

# Lines of a file, 0 when it does not exist.
lines() { [ -f "$1" ] && wc -l <"$1" | tr -d ' ' || echo 0; }

# has_rows FILE: the CSV sink file holds at least one data row.
has_rows() { [ "$(lines "$1")" -gt 1 ]; }

# Data rows of a CSV sink file (the first line is the header).
data_rows() { tail -n +2 "$1" 2>/dev/null; }

# growing FILE: the sink file gains lines within 2 s.
growing() {
  local a
  a=$(lines "$1")
  sleep 2
  [ "$(lines "$1")" -gt "$a" ]
}

# A negated command does not fail a bats test unless it is the last one; a function's status does.
stopped() { ! growing "$1"; }

# wait_until SECONDS CMD...: retry CMD every 0.25 s until it succeeds or SECONDS pass. CMD's arguments are expanded
# once, at the call: poll a function, not a $(...).
wait_until() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@"; do
    [ $SECONDS -ge $deadline ] && return 1
    sleep 0.25
  done
}

# await_rows FILE: for use inside the input of a REPL session. Waits until the sink has written a data row, and never
# ends that input early (bats runs with set -e, so a failing wait would end it and the REPL would exit mid-script).
await_rows() {
  wait_until 90 has_rows "$1" || echo "no row in $1 after 90 s" >&3
}

# state_of ID: the state SHOW QUERIES reports for query ID, from a coordinator that lives long enough to observe the
# workers (a fresh one first reports what the catalog last recorded).
state_of() {
  { sleep "${OBSERVE_SECONDS:-3}"; echo "SHOW QUERIES WHERE id = $1;"; } | repl | jq -r '.. | objects | select(has("sql")) | .state' | tail -1
}

# write_setup FILE SINK_FILE COUNT [RATE]: a nes-cli setup with one worker, a generator seq(v, ts) counting
# 0..COUNT-1 at RATE tuples a second, a file sink `out` and the query `SELECT v FROM seq INTO out`.
write_setup() {
  cat >"$1" <<YAML
workers:
  - host: $(worker_addr 1)
    data_address: $(worker_data_addr 1)
logical:
  - name: seq
    schema:
      - {name: v, type: UINT64}
      - {name: ts, type: UINT64}
physical:
  - logical: seq
    host: $(worker_addr 1)
    type: Generator
    parser_config: {type: CSV}
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate ${4:-100}
      stop_generator_when_sequence_finishes: ALL
      generator_schema: SEQUENCE UINT64 0 $3 1, SEQUENCE UINT64 0 $3 1
sinks:
  - name: out
    host: $(worker_addr 1)
    type: File
    schema:
      - {name: v, type: UINT64}
    config: {file_path: $2, output_format: CSV}
query: SELECT v FROM seq INTO out
YAML
}

# An application that runs alert queries: worker 1, an endless feed readings(value, ts) and a file sink per alert,
# alert_a .. alert_c writing to $W/alert_a.csv ... The REPL is pointed at nes-cli's catalog, so a test can use
# whichever frontend has the feature it needs.
alert_setup() {
  start_worker 1
  DB=$XDG_STATE_HOME/nebucli/coordinator.db
  mkdir -p "$(dirname "$DB")"
  cat >"$W/setup.yaml" <<YAML
workers:
  - host: $(worker_addr 1)
    data_address: $(worker_data_addr 1)
logical:
  - name: readings
    schema:
      - {name: value, type: UINT64}
      - {name: ts, type: UINT64}
physical:
  - logical: readings
    host: $(worker_addr 1)
    type: Generator
    parser_config: {type: CSV}
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 100
      stop_generator_when_sequence_finishes: ALL
      generator_schema: SEQUENCE UINT64 0 100000000 1, SEQUENCE UINT64 0 100000000 1
sinks:
$(for s in a b c; do printf '  - {name: alert_%s, host: "%s", type: File, schema: [{name: value, type: UINT64}], config: {file_path: %s, output_format: CSV}}\n' "$s" "$(worker_addr 1)" "$W/alert_$s.csv"; done)
YAML
}

# alert SINK THRESHOLD [NAME]: an alert query into alert_SINK.
alert() {
  local named=""
  [ -n "${3:-}" ] && named=" SET ('$3' AS \"QUERY\".NAME)"
  echo "SELECT value FROM readings WHERE value > UINT64($2) INTO alert_$1$named"
}

# start QUERY...: nes-cli start on the alert setup; returns once the queries run and prints their ids.
start() { cli -s "$W/setup.yaml" start "$@"; }
