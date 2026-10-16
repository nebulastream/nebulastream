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

# E2E tests for asynchronously executed functions (HTTP_CALL, ASYNC_DELAY) in the embedded REPL,
# against tests/http_path_server.py, which answers every POST with "<path>:<body>".

source "$NES_BATS_LIB"

NES_REPL="$NES_REPL_EMBEDDED"

# Worker options for every REPL in this file, e.g. the execution mode of the interpreter variant of this suite.
WORKER_ARGS=()
if [ -n "${NES_ASYNC_WORKER_ARGS:-}" ]; then
  read -r -a WORKER_ARGS <<<"-- $NES_ASYNC_WORKER_ARGS"
fi

setup_file()    { nes_offline_setup_file; }
teardown_file() { nes_offline_teardown_file; }

setup() {
  nes_offline_setup
  python3 tests/http_path_server.py > http_port.txt 2> http_paths.log &
  HTTP_SERVER_PID=$!
  wait_until test -s http_port.txt
  HTTP_SERVER="http://127.0.0.1:$(head -1 http_port.txt)"
}

teardown() { kill "$HTTP_SERVER_PID" 2>/dev/null || true; }

# Runs tests/sql/$1 with HTTP_SERVER replaced by the server's url, until its queries terminated.
# Further arguments are passed on to the REPL. A query that never terminates fails the test after two minutes
# instead of blocking the suite.
run_async_sql() {
  sed "s|HTTP_SERVER|$HTTP_SERVER|g" "tests/sql/$1" > "$1"
  run timeout -k 5 120 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION "${@:2}" "${WORKER_ARGS[@]}" <"$1"
}

# The rows of a CSV sink without its header, sorted, since a query does not fix the output order.
sink_rows() { tail -n +2 "$1" | sort; }

@test "HTTP_CALL: generated ids call different urls" {
  run_async_sql generated_urls.sql
  [ "$status" -eq 0 ]

  # One row per generated id, answered by the url that its id selected.
  [ "$(sink_rows responses.csv | wc -l)" -eq 30 ]
  sink_rows responses.csv | awk -F, '{ split("a,b,c", path, ","); if ($2 != "/" path[$1 % 3 + 1] ":ping") { print "unexpected row: " $0; exit 1 } }'

  # Each url was requested once per id that selected it.
  [ "$(grep -c '^/a$' http_paths.log)" -eq 10 ]
  [ "$(grep -c '^/b$' http_paths.log)" -eq 10 ]
  [ "$(grep -c '^/c$' http_paths.log)" -eq 10 ]
}

@test "HTTP_CALL: pass-through fields, computed arguments, nested calls and a second async function" {
  run_async_sql http_call.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows pass_through.csv)" = "$(printf '1,/echo:Hello\n2,/echo:NebulaStream\n3,/echo:äöü')" ]
  [ "$(sink_rows computed.csv)" = "$(printf '1,/echo:Hello!?\n2,/echo:NebulaStream!?\n3,/echo:äöü!?')" ]
  [ "$(sink_rows nested.csv)" = "$(printf '1,/b:/a:Hello-\n2,/b:/a:NebulaStream-\n3,/b:/a:äöü-')" ]
  [ "$(sink_rows mixed.csv)" = "$(printf '1,Hello/echo:Hello\n2,NebulaStream/echo:NebulaStream\n3,äöü/echo:äöü')" ]
}

@test "ASYNC_DELAY: the rows of a buffer wait concurrently" {
  local start=$SECONDS
  run_async_sql delay.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows delayed.csv)" = "$(printf '1,Hello\n2,NebulaStream\n3,äöü')" ]
  # Three rows of one second each; waiting one after the other would take at least three.
  [ $((SECONDS - start)) -lt 3 ]
}

@test "HTTP_CALL: an unreachable service fails the query" {
  run_async_sql unreachable.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows responses.csv | wc -l)" -eq 0 ]
  grep -q "Query Failed: .*HTTP_CALL to http://127.0.0.1:1/echo failed" nes-repl.log
}

@test "ASYNC_DELAY: dropping a query cancels a call that is still waiting" {
  local query="5a1c0de0-0000-4000-8000-000000000001"
  local start=$SECONDS
  run bash -c "{ cat tests/sql/long_delay.sql; sleep 2;
                 echo \"SHOW QUERIES WHERE id = '$query';\";
                 echo \"DROP QUERY WHERE id = '$query';\"; sleep 2;
                 echo \"SHOW QUERIES WHERE id = '$query';\"; } | timeout -k 5 60 $NES_REPL -f JSON ${WORKER_ARGS[*]}"
  [ "$status" -eq 0 ]

  # Before the drop the producer half has finished and the consumer half is waiting in the call.
  echo "${lines[4]}" | jq -e '.[] | select(.local_query_id == null) | .query_status == "PartiallyStopped"'
  # After the drop every part has stopped, long before the ten minute delay ran out.
  echo "${lines[6]}" | jq -e 'all(.[]; .query_status == "Stopped")'
  [ $((SECONDS - start)) -lt 30 ]
  [ "$(sink_rows delayed.csv | wc -l)" -eq 0 ]
}

@test "HTTP_CALL in WHERE: in the predicate itself, and on the result of a projection" {
  run_async_sql where.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows direct.csv)" = "1,Hello" ]
  [ "$(sink_rows on_result.csv)" = "2,/result:NebulaStream" ]
  # One call per row: a filter on a call's result must not repeat the call.
  [ "$(grep -c '^/direct$' http_paths.log)" -eq 3 ]
  [ "$(grep -c '^/result$' http_paths.log)" -eq 3 ]
}

@test "HTTP_CALL stress: 20 concurrent queries with 50 requests per second each" {
  local start=$SECONDS
  run_async_sql stress.sql
  [ "$status" -eq 0 ]

  for query in $(seq 0 19); do
    # Every generated id was answered exactly once, by the path its query called.
    [ "$(sink_rows "stress_$query.csv" | cut -d, -f1 | sort -n | uniq | wc -l)" -eq 500 ]
    [ "$(sink_rows "stress_$query.csv" | wc -l)" -eq 500 ]
    [ "$(sink_rows "stress_$query.csv" | cut -d, -f2 | sort -u)" = "/q$query:ping" ]
    [ "$(grep -c "^/q$query\$" http_paths.log)" -eq 500 ]
  done
  # The sources emit for ten seconds and the same queries without calls take about twelve; calls that
  # cannot keep up with 1000 requests per second queue behind the sources and push this well past that.
  [ $((SECONDS - start)) -lt 20 ]
}

@test "HTTP_CALL backpressure: a slow service holds the source back instead of losing or piling up rows" {
  # --debug logs when the Handoff sink applies and releases backpressure.
  run_async_sql backpressure.sql --debug
  [ "$status" -eq 0 ]

  [ "$(sink_rows responses.csv | cut -d, -f1 | sort -n | uniq | wc -l)" -eq 2000 ]
  [ "$(sink_rows responses.csv | cut -d, -f2 | sort -u)" = "/slow?delay_ms=50:ping" ]
  [ "$(wc -l < http_paths.log)" -eq 2000 ]

  # The calls cannot keep up with the source, so the sink applied backpressure, and released it every time.
  local acquired released
  acquired=$(grep -c "Backpressure acquired" nes-repl.log)
  released=$(grep -c "Backpressure released" nes-repl.log)
  [ "$acquired" -gt 0 ]
  [ "$acquired" -eq "$released" ]
}

@test "HTTP_CALL chaining and reuse: nested calls in one stage, WHERE results read by SELECT" {
  run_async_sql chaining.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows chained.csv)" = "$(printf '1,/outer:/inner:Hello\n2,/outer:/inner:NebulaStream\n3,/outer:/inner:äöü')" ]
  [ "$(grep -c '^/inner$' http_paths.log)" -eq 3 ]
  [ "$(grep -c '^/outer$' http_paths.log)" -eq 3 ]

  [ "$(sink_rows reused.csv)" = "1,/both:/both:Hello" ]
  # Two calls per row for the predicate; the matching row's SELECT reuses them instead of calling twice more.
  [ "$(grep -c '^/both$' http_paths.log)" -eq 6 ]
}

@test "HTTP_CALL: a service failing while the producer is held back fails the query and stops every part of it" {
  local start=$SECONDS
  run_async_sql failing_service.sql
  [ "$status" -eq 0 ]

  grep -q "Query Failed: .*HTTP_CALL to 127.0.0.1:[0-9]*/slow?delay_ms=50&fail_after=300 got status 500" nes-repl.log
  # The calls stop with the failure: at most the ones already in flight reach the service after it.
  [ "$(wc -l < http_paths.log)" -lt 400 ]
  # The source would emit for ten seconds; the query ends with the failure, not when the source is done.
  [ $((SECONDS - start)) -lt 10 ]
}

@test "HTTP_CALL: dropping a query while the producer is held back stops every part of it" {
  local query="5a1c0de0-0000-4000-8000-000000000002"
  sed "s|HTTP_SERVER|$HTTP_SERVER|g" tests/sql/backpressure_drop.sql > backpressure_drop.sql
  local start=$SECONDS
  run bash -c "{ cat backpressure_drop.sql; sleep 3;
                 echo \"DROP QUERY WHERE id = '$query';\"; sleep 2;
                 echo \"SHOW QUERIES WHERE id = '$query';\"; } | timeout -k 5 60 $NES_REPL -f JSON --debug ${WORKER_ARGS[*]}"
  [ "$status" -eq 0 ]

  # The producer was held back when the drop arrived.
  grep -q "Backpressure acquired" nes-repl.log
  echo "${lines[-1]}" | jq -e 'all(.[]; .query_status == "Stopped")'
  [ $((SECONDS - start)) -lt 30 ]
}

@test "HTTP_CALL: a failing query leaves another query on the shared event loop alone" {
  run_async_sql isolation.sql
  [ "$status" -eq 0 ]

  grep -q "Query Failed: .*HTTP_CALL to http://127.0.0.1:1/echo failed" nes-repl.log
  [ "$(sink_rows failed.csv | wc -l)" -eq 0 ]
  [ "$(sink_rows healthy.csv)" = "$(printf '1,/slow?delay_ms=1000:Hello\n2,/slow?delay_ms=1000:NebulaStream\n3,/slow?delay_ms=1000:äöü')" ]
}
