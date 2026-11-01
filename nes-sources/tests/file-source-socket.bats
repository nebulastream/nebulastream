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
  work_dir=$(mktemp -d /tmp/nes-file-source-XXXXXX)
  cd "$work_dir"
}

teardown() {
  if [ -n "${sender_pid:-}" ]; then
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
  fi
  if [ -n "${work_dir:-}" ]; then
    [ ! -f "$work_dir/sender.log" ] || cat "$work_dir/sender.log" >&3
    rm -rf "$work_dir"
  fi
}

start_sender() {
  # Open the producer only after accepting the source's single connection.
  # -U transfers from the right-hand address to the left-hand address.
  timeout --kill-after=2s 25s socat -d -d -U \
    UNIX-LISTEN:input.sock,unlink-close=0 "${1:-OPEN:input,rdonly}" >sender.log 2>&1 3>&- &
  sender_pid=$!

  local deadline=$((SECONDS + 5))
  until grep -q 'listening on' sender.log; do
    kill -0 "$sender_pid" 2>/dev/null
    [ "$SECONDS" -lt "$deadline" ]
    sleep 0.05
  done
}

write_query() {
  local format=$1
  local path=${2:-$work_dir/input.sock}
  cat >query.sql <<EOF
CREATE LOGICAL SOURCE numbers("value" UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR numbers TYPE File SET(
    '$path' AS "SOURCE".FILE_PATH,
    '$format' AS INPUT_FORMATTER."TYPE"
);
CREATE SINK result("value" UINT64 NOT NULL) TYPE File SET(
    '$work_dir/result' AS "SINK".FILE_PATH,
    '$format' AS "SINK".OUTPUT_FORMAT
);
SELECT "value" * 2 AS "value" FROM numbers WHERE "value" % 2 = 0 INTO result;
EOF
}

execute_query() {
  write_query "$@"
  run timeout --kill-after=2s 20s "$NES_REPL_EMBEDDED" -f JSON \
    --on-exit WAIT_FOR_QUERY_TERMINATION -- \
    --worker.default_query_execution.execution_mode=INTERPRETER <query.sql
  [ "$status" -eq 0 ]
  if [ -n "${sender_pid:-}" ]; then
    wait "$sender_pid"
    unset sender_pid
    [ -S input.sock ]  # The source must not remove or replace the socket.
  fi
  run diff -u expected result
  [ "$status" -eq 0 ]
}

@test "File source reads deterministic CSV tuples from an existing Unix socket" {
  printf '0\n1\n2\n3\n4\n5\n6\n7\n' >input
  printf '"value":UINT64:NOT_NULLABLE\n0\n4\n8\n12\n' >expected
  start_sender
  execute_query CSV
}

@test "File source reads deterministic JSON tuples over one connection" {
  printf '{"value":0}\n{"value":1}\n{"value":2}\n{"value":3}\n{"value":4}\n{"value":5}\n{"value":6}\n{"value":7}\n' >input
  printf '"value":UINT64:NOT_NULLABLE\n{"value":0}\n{"value":4}\n{"value":8}\n{"value":12}\n' >expected
  start_sender
  execute_query JSON
}

@test "File source treats an empty socket stream as EOF" {
  touch input
  printf '"value":UINT64:NOT_NULLABLE\n' >expected
  start_sender
  execute_query CSV
}

@test "File source preserves a tuple split across socket writes" {
  # socat starts this shell after accept(), so the pause separates live writes.
  cat >send.sh <<'EOF'
printf '1'
sleep 0.2
printf '2\n3\n'
EOF
  printf '"value":UINT64:NOT_NULLABLE\n24\n' >expected
  start_sender 'EXEC:sh send.sh'
  execute_query CSV
}

@test "File source preserves JSON split across socket writes" {
  cat >send.sh <<'EOF'
printf '{"val'
sleep 0.2
printf 'ue":12}\n{"value":3}\n'
EOF
  printf '"value":UINT64:NOT_NULLABLE\n{"value":24}\n' >expected
  start_sender 'EXEC:sh send.sh'
  execute_query JSON
}

@test "File source still reads ordinary files" {
  printf '0\n1\n2\n3\n4\n5\n6\n7\n' >input
  printf '"value":UINT64:NOT_NULLABLE\n0\n4\n8\n12\n' >expected
  execute_query CSV "$work_dir/input"
}

@test "File source cancels a blocking read without waiting for the peer" {
  start_sender 'EXEC:sleep 20'
  write_query CSV
  run timeout --kill-after=2s 5s "$NES_REPL_EMBEDDED" -f JSON \
    --on-exit WAIT_FOR_QUERY_TERMINATION -- \
    --worker.default_query_execution.execution_mode=INTERPRETER <query.sql
  [ "$status" -eq 124 ]  # SIGTERM must finish without timeout escalating to SIGKILL (137).
  grep -q 'accepting connection' sender.log
}

@test "File source reports a missing input file" {
  write_query CSV "$work_dir/missing.csv"
  run timeout --kill-after=2s 20s "$NES_REPL_EMBEDDED" -f JSON \
    --on-exit WAIT_FOR_QUERY_TERMINATION -- \
    --worker.default_query_execution.execution_mode=INTERPRETER <query.sql
  [ "$status" -eq 0 ]
  grep -q 'Could not open input file:.*missing.csv.*error=No such file or directory' nes-repl.log
}

@test "File source reports a refused Unix socket connection" {
  start_sender
  kill "$sender_pid"
  wait "$sender_pid" 2>/dev/null || true
  unset sender_pid
  [ -S input.sock ]
  write_query CSV
  run timeout --kill-after=2s 20s "$NES_REPL_EMBEDDED" -f JSON \
    --on-exit WAIT_FOR_QUERY_TERMINATION -- \
    --worker.default_query_execution.execution_mode=INTERPRETER <query.sql
  [ "$status" -eq 0 ]  # The REPL also exits normally when a query reaches Failed.
  grep -q 'Could not connect to Unix socket source:.*error=Connection refused' nes-repl.log
}
