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

setup_file() {
  nes_distributed_setup_file "$NES_CLI"
  # Bake the PostgreSQL ODBC driver into $WORKER_IMAGE once per suite,
  # re-tagging back to the same image name. Each test then reuses the
  # rebuilt image instead of paying the install cost on every run.
  local build_ctx
  build_ctx=$(mktemp -d)
  cat > "$build_ctx/Dockerfile" <<EOF
FROM $WORKER_IMAGE
USER root
RUN apt-get update -qq \\
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends odbc-postgresql \\
 && rm -rf /var/lib/apt/lists/*
EOF
  docker build --quiet --tag "$WORKER_IMAGE" "$build_ctx" >&3
  rm -rf "$build_ctx"
}
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

# Run a SQL statement against the postgres container. The ODBC source's
# query window is [previous_lower, now()], so any rows inserted via this
# helper after the query is started should be picked up on the next poll.
docker_execute_sql() {
  docker compose exec -T -e PGPASSWORD=nes postgres \
    psql -v ON_ERROR_STOP=1 -q -t -U nes -d nesdb -c "$1"
}

create_events_table() {
  docker_execute_sql "DROP TABLE IF EXISTS events;"
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"
}

@test "launch query from topology" {
  setup_distributed tests/good/example.yaml
  create_events_table

  run docker_nes_cli -t tests/good/example.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  # Let the source open and complete a few polls so the window starts
  # advancing past the connect timestamp.
  sleep 2
  docker_execute_sql "INSERT INTO events(id) VALUES (1), (2);"
  # Allow multiple poll cycles to capture the rows.
  sleep 3

  run docker_nes_cli -t tests/good/example.yaml stop $query_id
  sleep 1

  sync_workdir
  # checksum sink writes "Count,Checksum"; 2 rows were inserted.
  grep "^2," worker-1/results.csv
}

@test "long poll interval" {
  setup_distributed tests/good/long-poll.yaml
  create_events_table

  run docker_nes_cli -t tests/good/long-poll.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  docker_execute_sql "INSERT INTO events(id) VALUES (1), (2);"
  sleep 1

  sync_workdir
  # poll_interval_ms is 5000; nothing should have been fetched yet.
  [ ! -f worker-1/results.csv ] || [ "$(wc -l < worker-1/results.csv)" -eq 0 ]

  sleep 6
  sync_workdir
  # FILE sink writes header + 2 rows after the first poll completes.
  [ "$(wc -l < worker-1/results.csv)" -eq 3 ]
}

@test "more data" {
  setup_distributed tests/good/example.yaml
  create_events_table

  run docker_nes_cli -t tests/good/example.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1

  # Insert 500 rows in a single batch using generate_series; the source
  # may split them across multiple buffers depending on poll cadence.
  docker_execute_sql "INSERT INTO events(id) SELECT g FROM generate_series(1, 500) g;"
  sleep 2

  run docker_nes_cli -t tests/good/example.yaml stop $query_id
  sleep 1

  sync_workdir
  grep "^500," worker-1/results.csv
}
