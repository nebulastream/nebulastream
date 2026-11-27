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

setup() {
  [ -x "$NES_REPL_EMBEDDED" ]
  command -v socat >/dev/null
  # Keep the pathname short enough for sockaddr_un
  work_dir=$(mktemp -d /tmp/nes-unix-socket-sink-XXXXXX)
  cd "$work_dir"
  # No fork: accept one connection and exit at EOF, without sending replies.
  timeout --kill-after=2s 25s socat -d -d -u \
    UNIX-LISTEN:output.sock,unlink-close=0 STDOUT >received 2>receiver.log 3>&- &
  receiver_pid=$!

  local deadline=$((SECONDS + 5))
  # Wait for listen(), not merely for the socket pathname to appear.
  until grep -q 'listening on' receiver.log; do
    kill -0 "$receiver_pid" 2>/dev/null
    [ "$SECONDS" -lt "$deadline" ]
    sleep 0.05
  done
}

teardown() {
  if [ -n "${receiver_pid:-}" ]; then
    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
  fi
  if [ -n "${work_dir:-}" ]; then
    cat "$work_dir/receiver.log" >&3
    rm -rf "$work_dir"
  fi
}

write_query() {
  local format=$1
  local predicate=${2:-'value % 2 = 0'}
  local path=${3-$work_dir/output.sock}
  cat >query.sql <<EOF
CREATE LOGICAL SOURCE numbers(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR numbers TYPE Generator SET(
    'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' AS INPUT_FORMATTER."TYPE",
    'emit_rate 20' AS "SOURCE".GENERATOR_RATE_CONFIG,
    10 AS "SOURCE".FLUSH_INTERVAL_MS,
    'SEQUENCE UINT64 0 8 1' AS "SOURCE".GENERATOR_SCHEMA
);
CREATE SINK result("value" UINT64 NOT NULL) TYPE unix_socket SET(
    '$path' AS "SINK".PATH,
    '$format' AS "SINK".OUTPUT_FORMAT
);
SELECT value * 2 AS "value" FROM numbers WHERE $predicate INTO result;
EOF
}

run_query() {
  run timeout --kill-after=2s 20s "$NES_REPL_EMBEDDED" -f JSON \
    --on-exit WAIT_FOR_QUERY_TERMINATION -- \
    --worker.default_query_execution.execution_mode=INTERPRETER <query.sql
  [ "$status" -eq 0 ]
}

execute_query() {
  write_query "$@"
  run_query
  wait "$receiver_pid"
  unset receiver_pid
  [ -S output.sock ]  # The sink must neither remove nor replace the socket.
}

@test "Unix socket sink writes deterministic CSV tuples to an existing Unix socket" {
  execute_query CSV
  printf '"value":UINT64:NOT_NULLABLE\n0\n4\n8\n12\n' >expected
  run diff -u expected received
  [ "$status" -eq 0 ]  # Includes exactly one schema header; catches missing rows or duplicates.
}

@test "Unix socket sink writes a socket schema even when the query produces no tuples" {
  execute_query JSON 'value > 100'
  printf '"value":UINT64:NOT_NULLABLE\n' >expected
  run diff -u expected received
  [ "$status" -eq 0 ]
}

@test "Unix socket sink writes deterministic JSON tuples over one persistent connection" {
  execute_query JSON
  printf '"value":UINT64:NOT_NULLABLE\n{"value":0}\n{"value":4}\n{"value":8}\n{"value":12}\n' >expected
  run diff -u expected received
  [ "$status" -eq 0 ]
}

@test "Unix socket sink reports a missing socket path without creating a file" {
  write_query CSV 'value % 2 = 0' "$work_dir/missing.sock"
  run_query
  grep -q 'Could not connect to Unix socket sink:.*missing.sock.*error=No such file or directory' nes-repl.log
  [ ! -e missing.sock ]
}

@test "Unix socket sink reports a refused connection without removing the socket" {
  kill "$receiver_pid"
  wait "$receiver_pid" 2>/dev/null || true
  unset receiver_pid
  [ -S output.sock ]
  write_query CSV
  run_query
  grep -q 'Could not connect to Unix socket sink:.*error=Connection refused' nes-repl.log
  [ -S output.sock ]
}

@test "Unix socket sink rejects an empty socket path during binding" {
  write_query CSV 'value % 2 = 0' ''
  run timeout --kill-after=2s 20s "$NES_REPL_EMBEDDED" -f JSON <query.sql
  # InvalidConfigParameter (1000), truncated to a POSIX 8-bit exit status.
  [ "$status" -eq $((1000 % 256)) ]
  [[ "$output" == *'Config literal cannot be empty'* ]]
}

@test "Unix socket sink rejects an overlong socket path" {
  write_query CSV 'value % 2 = 0' "$work_dir/$(printf '%0120d' 0)"
  run_query
  grep -q 'Invalid Unix socket path:' nes-repl.log
}
