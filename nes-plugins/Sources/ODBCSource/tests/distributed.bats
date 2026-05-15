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
  cat >"$build_ctx/Dockerfile" <<EOF
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
setup() { nes_distributed_setup; }
teardown() { nes_distributed_teardown; }

# Run a SQL statement against the postgres container. The ODBC source's
# query window is [previous_lower, now()], so any rows inserted via this
# helper after the query is started should be picked up on the next poll.
docker_execute_sql() {
  docker compose exec -T -e PGPASSWORD=nes postgres \
    psql -v ON_ERROR_STOP=1 -q -t -U nes -d nesdb -c "$1"
}

@test "launch query from topology" {
  setup_distributed tests/good/example.yaml
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/example.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  # Let the source open and complete a few polls so the window starts
  # advancing past the connect timestamp.
  sleep 2
  docker_execute_sql "INSERT INTO events(id) VALUES (1), (2);"
  docker_execute_sql "SELECT * from events;"
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
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/long-poll.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  docker_execute_sql "INSERT INTO events(id) VALUES (1), (2);"
  sleep 1

  sync_workdir
  # poll_interval_ms is 5000; nothing should have been fetched yet.
  [ ! -f worker-1/results.csv ] || [ "$(wc -l <worker-1/results.csv)" -eq 0 ]

  sleep 6
  sync_workdir
  # FILE sink writes header + 2 rows after the first poll completes.
  [ "$(wc -l <worker-1/results.csv)" -eq 3 ]
}

@test "more data" {
  setup_distributed tests/good/example.yaml
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

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

@test "database stopped during query" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT id FROM events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(id INT32 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  # Ingest a couple of rows through the normal path first so we know the
  # source has reached the steady poll loop before we yank the database.
  sleep 1
  docker_execute_sql "INSERT INTO events(id) VALUES (1), (2);"
  sleep 2

  # SIGTERM postgres. The next SQLExecute hits SQL_ERROR; the source's CHECK
  # macro throws CannotOpenSource and the query transitions to Failed.
  docker compose stop postgres

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" $query_id
}

@test "stop with very long poll interval" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  # poll_interval_ms = 600000 (10 min). If the stop_token weren't honored,
  # stopping would block until the next poll fires.
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        600000 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT id FROM events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(id INT32 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  # Let the source reach FetchFromDatabaseState and enter cv.wait_for.
  sleep 2

  local start
  start=$(date +%s)
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  [ "$status" -eq 0 ]

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Stopped" $query_id

  local end
  end=$(date +%s)
  # A non-interruptible wait would block ~600s; the bound is generous to
  # absorb scheduler jitter in CI.
  [ $((end - start)) -lt 30 ]
}

@test "number of columns missmatch" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  docker_execute_sql "CREATE TABLE typed_events (
    txt_not_null TEXT NOT NULL,
    txt TEXT,
    vc_not_null  VARCHAR(64) NOT NULL,
    vc  VARCHAR(64),
    ba  BYTEA,
    ba_not_null  BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'typed_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT txt_not_null, vc_not_null, ba_not_null, txt, vc, ba FROM typed_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(
            txt_not_null VARSIZED NOT NULL,
            vc_not_null  VARSIZED NOT NULL,
            ba_not_null  VARSIZED NOT NULL,
            txt VARSIZED,
            vc  VARSIZED,
            ba  VARSIZED,
            ts  UINT64
        ) AS `SOURCE`.`SCHEMA`
    ) INTO File("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`, "CSV" AS `SINK`.`OUTPUT_FORMAT`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" $query_id

  sync_workdir
  grep " failed to open a source; Query returned 6 columns, but schema expects 7." worker-1/singleNodeWorker.log
}

#bats test_tags=bats:focus
@test "text type tests" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  docker_execute_sql "CREATE TABLE typed_events (
    txt_not_null TEXT NOT NULL,
    txt TEXT,
    vc_not_null  VARCHAR(64) NOT NULL,
    vc  VARCHAR(64),
    ba  BYTEA,
    ba_not_null  BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'typed_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT txt_not_null, vc_not_null, ba_not_null, txt, vc, ba FROM typed_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(
            txt_not_null VARSIZED NOT NULL,
            vc_not_null  VARSIZED NOT NULL,
            ba_not_null  VARSIZED NOT NULL,
            txt VARSIZED,
            vc  VARSIZED,
            ba  VARSIZED
        ) AS `SOURCE`.`SCHEMA`
    ) INTO File("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`, "CSV" AS `SINK`.`OUTPUT_FORMAT`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1

  # Three rows: min, zero and max per NES type width. Values stay inside
  # postgres INTEGER (signed 32-bit) range — that's the wire type — and
  # also inside the declared NES width so the driver-side SQL_C_*INT
  # conversion doesn't truncate.
  docker_execute_sql "INSERT INTO typed_events
    (txt_not_null, vc_not_null, ba_not_null, txt, vc, ba) VALUES
    ('small_text', 'small_text', E'\\\\x', 'small_text', 'small_text', E'\\\\x'),
    (
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque sem placerat in id cursus mi pretium tellus duis convallis tempus leo eu aenean sed diam urna tempor pulvinar vivamus fringilla lacus nec metus bibendum egestas iaculis massa nisl malesuada lacinia integer nunc posuere ut hendrerit semper vel class aptent taciti sociosqu ad litora torquent per conubia nostra inceptos himenaeos orci varius natoque penatibus et magnis dis parturient montes nascetur ridiculus mus donec rhoncus eros lobortis nulla molestie mattis scelerisque maximus eget fermentum odio phasellus non purus est efficitur laoreet mauris pharetra vestibulum fusce dictum risus blandit quis suspendisse aliquet nisi sodales consequat magna ante condimentum neque at luctus nibh finibus facilisis dapibus etiam interdum tortor ligula congue sollicitudin erat viverra ac tincidunt nam porta elementum a enim euismod quam justo lectus commodo augue arcu dignissim velit aliquam imperdiet mollis nullam volutpat porttitor ullamcorper rutrum gravida cras eleifend turpis fames primis vulputate ornare sagittis vehicula praesent dui felis venenatis ultrices proin libero feugiat tristique accumsan maecenas potenti ultricies habitant morbi senectus netus suscipit auctor curabitur facilisi cubilia curae hac habitasse platea dictumst lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque.',
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque f',
     '\xF09F9A80'::bytea,
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque sem placerat in id cursus mi pretium tellus duis convallis tempus leo eu aenean sed diam urna tempor pulvinar vivamus fringilla lacus nec metus bibendum egestas iaculis massa nisl malesuada lacinia integer nunc posuere ut hendrerit semper vel class aptent taciti sociosqu ad litora torquent per conubia nostra inceptos himenaeos orci varius natoque penatibus et magnis dis parturient montes nascetur ridiculus mus donec rhoncus eros lobortis nulla molestie mattis scelerisque maximus eget fermentum odio phasellus non purus est efficitur laoreet mauris pharetra vestibulum fusce dictum risus blandit quis suspendisse aliquet nisi sodales consequat magna ante condimentum neque at luctus nibh finibus facilisis dapibus etiam interdum tortor ligula congue sollicitudin erat viverra ac tincidunt nam porta elementum a enim euismod quam justo lectus commodo augue arcu dignissim velit aliquam imperdiet mollis nullam volutpat porttitor ullamcorper rutrum gravida cras eleifend turpis fames primis vulputate ornare sagittis vehicula praesent dui felis venenatis ultrices proin libero feugiat tristique accumsan maecenas potenti ultricies habitant morbi senectus netus suscipit auctor curabitur facilisi cubilia curae hac habitasse platea dictumst lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque.',
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque f',
     '\xF09F9A80'::bytea
    ),
    (
      '',
      '',
      '\xE29DA4EFB88F'::bytea,
      '',
      '',
      E''
    ),
    (
      '',
      '',
      E'',
      NULL,
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque f',
      E''
    ),
    (
      '',
      '',
      E'',
      'Lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque sem placerat in id cursus mi pretium tellus duis convallis tempus leo eu aenean sed diam urna tempor pulvinar vivamus fringilla lacus nec metus bibendum egestas iaculis massa nisl malesuada lacinia integer nunc posuere ut hendrerit semper vel class aptent taciti sociosqu ad litora torquent per conubia nostra inceptos himenaeos orci varius natoque penatibus et magnis dis parturient montes nascetur ridiculus mus donec rhoncus eros lobortis nulla molestie mattis scelerisque maximus eget fermentum odio phasellus non purus est efficitur laoreet mauris pharetra vestibulum fusce dictum risus blandit quis suspendisse aliquet nisi sodales consequat magna ante condimentum neque at luctus nibh finibus facilisis dapibus etiam interdum tortor ligula congue sollicitudin erat viverra ac tincidunt nam porta elementum a enim euismod quam justo lectus commodo augue arcu dignissim velit aliquam imperdiet mollis nullam volutpat porttitor ullamcorper rutrum gravida cras eleifend turpis fames primis vulputate ornare sagittis vehicula praesent dui felis venenatis ultrices proin libero feugiat tristique accumsan maecenas potenti ultricies habitant morbi senectus netus suscipit auctor curabitur facilisi cubilia curae hac habitasse platea dictumst lorem ipsum dolor sit amet consectetur adipiscing elit quisque faucibus ex sapien vitae pellentesque.',
      NULL,
      E''
    ),
    (
      '',
      '',
      E'',
      NULL,
      NULL,
      NULL
    );"

  sleep 3

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml status $query_id >&3
  sleep 1

  sync_workdir
}

@test "various sql data types" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  # Covers every SQL→NES translation the ODBC source supports:
  #   - SQL_INTEGER  → INT8/16/32/64 + UINT8/16/32/64 via Direct
  #   - SQL_LONGVARCHAR / SQL_VARCHAR / SQL_LONGVARBINARY → VARSIZED via VarSized
  #   - SQL_TYPE_TIMESTAMP → UINT64 (ms since epoch) via Fixup
  docker_execute_sql "CREATE TABLE typed_events (
    i8  INTEGER NOT NULL,
    i16 INTEGER NOT NULL,
    i32 INTEGER NOT NULL,
    i64 INTEGER NOT NULL,
    u8  INTEGER NOT NULL,
    u16 INTEGER NOT NULL,
    u32 INTEGER NOT NULL,
    u64 INTEGER NOT NULL,
    txt TEXT NOT NULL,
    vc  VARCHAR(64) NOT NULL,
    ba  BYTEA NOT NULL,
    ts  TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'typed_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT i8, i16, i32, i64, u8, u16, u32, u64, txt, vc, ba, ts FROM typed_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(
            i8  INT8     NOT NULL,
            i16 INT16    NOT NULL,
            i32 INT32    NOT NULL,
            i64 INT64    NOT NULL,
            u8  UINT8    NOT NULL,
            u16 UINT16   NOT NULL,
            u32 UINT32   NOT NULL,
            u64 UINT64   NOT NULL,
            txt VARSIZED NOT NULL,
            vc  VARSIZED NOT NULL,
            ba  VARSIZED NOT NULL,
            ts  UINT64   NOT NULL
        ) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1

  # Three rows: min, zero and max per NES type width. Values stay inside
  # postgres INTEGER (signed 32-bit) range — that's the wire type — and
  # also inside the declared NES width so the driver-side SQL_C_*INT
  # conversion doesn't truncate.
  docker_execute_sql "INSERT INTO typed_events
    (i8, i16, i32, i64, u8, u16, u32, u64, txt, vc, ba, ts) VALUES
    (-128, -32768, -2147483648, -2147483648,
       0,    0,         0,         0,
     'hello', 'world', E'\\\\x010203', '2024-01-15 10:30:00.123456');"
  docker_execute_sql "INSERT INTO typed_events
    (i8, i16, i32, i64, u8, u16, u32, u64, txt, vc, ba, ts) VALUES
    (0, 0, 0, 0, 0, 0, 0, 0, '', '', E'\\\\x', '2024-06-01 00:00:00');"
  docker_execute_sql "INSERT INTO typed_events
    (i8, i16, i32, i64, u8, u16, u32, u64, txt, vc, ba, ts) VALUES
    (127, 32767, 2147483647, 2147483647,
     255, 65535, 2147483647, 2147483647,
     'long-string-spanning-many-characters-to-exercise-varsized',
     'short',
     E'\\\\xdeadbeefcafebabe',
     '2024-12-31 23:59:59.999999');"

  sleep 3

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  sleep 1

  sync_workdir
  # CHECKSUM sink writes "Count,Checksum"; three rows ingested across all
  # twelve columns confirms every type bound and fetched without error.
  grep "^3," worker-1/results.csv
}

@test "nullable columns both sides" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  # Postgres columns without NOT NULL are nullable. The source allows
  # binding a nullable SQL column to a nullable NES type — inline schema
  # fields without `NOT NULL` are nullable.
  docker_execute_sql "CREATE TABLE nullable_events (
    id  INTEGER,
    val INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'nullable_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT id, val FROM nullable_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(id INT32, val INT32) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  # Mix of present and NULL values across both columns. The Native parser
  # writes a 1-byte null indicator before each nullable value via the
  # nullByteFixup; we only assert the count, not byte-exact content.
  docker_execute_sql "INSERT INTO nullable_events (id, val) VALUES
    (1,    10),
    (2,    NULL),
    (NULL, 30),
    (NULL, NULL);"
  sleep 3

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  sleep 1

  sync_workdir
  grep "^4," worker-1/results.csv
}

@test "rejects nullable column bound to not-null nes type" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  # Postgres column is nullable (no NOT NULL); declared NES type is
  # `INT32 NOT NULL`. The source's lookupColumnMapping returns
  # "Cannot bind nullable SQL type to non nullable NES Type" and bind()
  # throws CannotOpenSource on the first fillTupleBuffer.
  docker_execute_sql "CREATE TABLE nullable_events (
    id  INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'nullable_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT id FROM nullable_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(id INT32 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" $query_id
}

@test "rejects incompatible type binding" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml

  # Postgres column is TEXT (SQL_LONGVARCHAR); declared NES type is INT32.
  # The conversions table has no {SQL_LONGVARCHAR, INT32} entry, so
  # lookupColumnMapping returns "Cannot bind SQLType ... to NES type ..."
  # and bind() throws on the source's first fillTupleBuffer call.
  docker_execute_sql "CREATE TABLE typed_events (
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(
    cat <<'EOF'
    SELECT * FROM ODBC(
        "worker-1:8080" AS `SOURCE`.`HOST`,
        'postgres' AS `SOURCE`.SERVER,
        '5432' AS `SOURCE`.PORT,
        'nesdb' AS `SOURCE`.DATABASE,
        'nes' AS `SOURCE`.USERNAME,
        'nes' AS `SOURCE`.PASSWORD,
        'PostgreSQL' AS `SOURCE`.DRIVER,
        'typed_events' AS `SOURCE`.SYNC_TABLE,
        'true' AS `SOURCE`.TRUST_SERVER_CERTIFICATE,
        250 AS `SOURCE`.POLL_INTERVAL_MS,
        'SELECT payload FROM typed_events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS `SOURCE`.`QUERY`,
        'Native' AS PARSER.`TYPE`,
        SCHEMA(payload INT32 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" AS `SINK`.`HOST`, "results.csv" AS `SINK`.`FILE_PATH`)
EOF
  )"
  [ "$status" -eq 0 ]
  query_id=$output

  wait_until_status tests/good/single-worker-with-4k-buffers.yaml "Failed" $query_id
}

@test "two queries against the same table" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  docker_execute_sql "CREATE TABLE events (id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"

  # Two independent ODBC sources poll the same `events` table concurrently
  # and write to different result files. Each opens its own ODBC connection
  # and prepared statement; the postgres server multiplexes them at the
  # protocol layer. Both sinks must see every row inserted after they
  # started polling.
  local -a query_def
  build_query() {
    local file_path=$1
    cat <<EOF
    SELECT * FROM ODBC(
        "worker-1:8080" AS \`SOURCE\`.\`HOST\`,
        'postgres' AS \`SOURCE\`.SERVER,
        '5432' AS \`SOURCE\`.PORT,
        'nesdb' AS \`SOURCE\`.DATABASE,
        'nes' AS \`SOURCE\`.USERNAME,
        'nes' AS \`SOURCE\`.PASSWORD,
        'PostgreSQL' AS \`SOURCE\`.DRIVER,
        'events' AS \`SOURCE\`.SYNC_TABLE,
        'true' AS \`SOURCE\`.TRUST_SERVER_CERTIFICATE,
        250 AS \`SOURCE\`.POLL_INTERVAL_MS,
        'SELECT id FROM events WHERE created_at > ? AND created_at <= ? ORDER BY created_at ASC' AS \`SOURCE\`.\`QUERY\`,
        'Native' AS PARSER.\`TYPE\`,
        SCHEMA(id INT32 NOT NULL) AS \`SOURCE\`.\`SCHEMA\`
    ) INTO CHECKSUM("worker-1:8080" AS \`SINK\`.\`HOST\`, "$file_path" AS \`SINK\`.\`FILE_PATH\`)
EOF
  }

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(build_query results_a.csv)"
  [ "$status" -eq 0 ]
  local q1=$output

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml start "$(build_query results_b.csv)"
  [ "$status" -eq 0 ]
  local q2=$output

  # Give both sources time to finish connecting and reach the steady
  # poll loop before any row appears.
  sleep 2
  docker_execute_sql "INSERT INTO events(id) SELECT g FROM generate_series(1, 10) g;"
  sleep 3

  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $q1
  run docker_nes_cli -t tests/good/single-worker-with-4k-buffers.yaml stop $q2
  sleep 1

  sync_workdir
  grep "^10," worker-1/results_a.csv
  grep "^10," worker-1/results_b.csv
}
