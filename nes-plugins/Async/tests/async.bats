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
  python3 tests/http_path_server.py http_received.log > http_port.txt 2> http_paths.log &
  HTTP_SERVER_PID=$!
  wait_until test -s http_port.txt
  HTTP_SERVER="http://127.0.0.1:$(head -1 http_port.txt)"
}

teardown() { kill "$HTTP_SERVER_PID" 2>/dev/null || true; }

# Runs tests/sql/$1 with HTTP_SERVER replaced by the server's url, until its queries terminated.
# Further arguments are passed on to the REPL; configuration options (--optimizer.*, --worker.*) go after its `--`,
# next to WORKER_ARGS. A query that never terminates fails the test after two minutes instead of blocking the suite.
run_async_sql() {
  local repl_args=() config_args=("${WORKER_ARGS[@]:1}")
  local arg
  for arg in "${@:2}"; do
    case "$arg" in
      --optimizer.* | --worker.*) config_args+=("$arg") ;;
      *) repl_args+=("$arg") ;;
    esac
  done
  if [ "${#config_args[@]}" -gt 0 ]; then
    config_args=(-- "${config_args[@]}")
  fi
  sed "s|HTTP_SERVER|$HTTP_SERVER|g" "tests/sql/$1" > "$1"
  run timeout -k 5 120 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION "${repl_args[@]}" "${config_args[@]}" <"$1"
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

@test "HTTP_CALL retries: async.max_retries repeats failed calls until the service answers" {
  run_async_sql flaky.sql --optimizer.async.max_retries=2
  [ "$status" -eq 0 ]

  [ "$(sink_rows responses.csv)" = "$(printf '1,/flaky?fail_first=2:Hello\n2,/flaky?fail_first=2:NebulaStream\n3,/flaky?fail_first=2:äöü')" ]
  # Three rows, two of whose first attempts failed.
  [ "$(wc -l < http_paths.log)" -eq 5 ]
}

@test "HTTP_CALL retries: without async.max_retries the first failed call fails the query" {
  run_async_sql flaky.sql
  [ "$status" -eq 0 ]

  grep -q "Query Failed: .*HTTP_CALL to 127.0.0.1:[0-9]*/flaky?fail_first=2 got status 500" nes-repl.log
  [ "$(sink_rows responses.csv | wc -l)" -eq 0 ]
}

@test "HTTP_CALL timeout: a service slower than the call's timeout fails the query" {
  local start=$SECONDS
  run_async_sql timeout.sql
  [ "$status" -eq 0 ]

  grep -q "Query Failed: .*HTTP_CALL to http://127.0.0.1:[0-9]*/slow?delay_ms=5000 failed" nes-repl.log
  # The service would answer after five seconds.
  [ $((SECONDS - start)) -lt 5 ]
}

@test "ASYNC_DELAY: async.max_concurrency bounds the calls in flight" {
  local start=$SECONDS
  run_async_sql delay.sql --optimizer.async.max_concurrency=1
  [ "$status" -eq 0 ]

  [ "$(sink_rows delayed.csv)" = "$(printf '1,Hello\n2,NebulaStream\n3,äöü')" ]
  # Three rows of one second each, one after the other.
  [ $((SECONDS - start)) -ge 3 ]
}

@test "ASYNC_DELAY: rows of different buffers wait concurrently" {
  local start=$SECONDS
  run_async_sql overlap.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows delayed.csv | wc -l)" -eq 20 ]
  [ "$(sink_rows delayed.csv | cut -d, -f2 | sort -u)" = "waited" ]
  # One row per buffer: waiting buffer after buffer would take 40 seconds, with 16 rows in flight it takes about four.
  [ $((SECONDS - start)) -lt 15 ]
}

@test "ASYNC_OUTPUT: reads the second value of a call next to its first" {
  run_async_sql values.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows delayed.csv)" = "$(printf '1,Hello,1000\n2,NebulaStream,1000\n3,äöü,1000')" ]
}

@test "ASYNC_DELAY: a function's own concurrency limit bounds the calls in flight" {
  local start=$SECONDS
  run_async_sql function_concurrency.sql
  [ "$status" -eq 0 ]

  [ "$(sink_rows delayed.csv)" = "$(printf '1,Hello\n2,NebulaStream\n3,äöü')" ]
  # The stage allows 16 calls, the function only one: three rows of one second each, one after the other.
  [ $((SECONDS - start)) -ge 3 ]
}

@test "ASYNC_OUTPUT: reading a value the call does not produce is rejected" {
  run_async_sql output_out_of_range.sql
  { echo "$output"; cat nes-repl.log; } | grep -q "ASYNC_OUTPUT reads value 1 of HTTP_CALL"
  [ "$(sink_rows responses.csv 2>/dev/null | wc -l)" -eq 0 ]
}

@test "HTTP_CALL: a call that timed out on a kept-alive connection is not sent again" {
  run_async_sql resend_timeout.sql --optimizer.async.max_concurrency=1
  [ "$status" -eq 0 ]

  grep -q "Query Failed: .*HTTP_CALL to http://127.0.0.1:[0-9]*/slow?slow_after=1&delay_ms=3000 failed" nes-repl.log
  # The first row's request, and the second row's once: the service received it, only its answer was too slow.
  [ "$(grep -c '^/slow?slow_after=1&delay_ms=3000$' http_received.log)" -eq 2 ]
}
