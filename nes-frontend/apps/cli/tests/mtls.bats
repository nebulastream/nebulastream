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

export BATS_TEST_TIMEOUT=120

setup_file() {
  nes_require_env NES_WORKER
  nes_require_env NES_CLI
  nes_require_env NES_RUNTIME_BASE_IMAGE
  nes_require_env NES_TEST_TMP_DIR
  nes_require_executable "$NES_WORKER"
  nes_require_executable "$NES_CLI"
  command -v openssl

  # Use unique image tags without cleaning up resources belonging to other suites.
  nes_build_runtime_image WORKER_IMAGE nes-worker-mtls-test "$NES_WORKER" nes-single-node-worker
  nes_build_app_image CLI_IMAGE nes-cli-mtls-test "$NES_CLI" nes-cli
  export NES_BATS_APP_IMAGE_VAR=CLI_IMAGE
}

teardown_file() { nes_distributed_teardown_file; }

setup() {
  nes_distributed_setup
  mkdir tls
  # Keep CA signing keys outside the volume shared with workers.
  export ISSUER_DIR="$BATS_TEST_TMPDIR/issuers"
  mkdir -p "$ISSUER_DIR"
  create_authority trusted
  cp "$ISSUER_DIR/trusted.pem" tls/ca.pem
  issue_worker_certificate worker-1 trusted 1
  issue_worker_certificate worker-2 trusted 2
}

teardown() {
  if [ -n "${TEST_VOLUME:-}" ]; then
    docker compose logs --no-color > compose.log 2>&1 || true
    nes_distributed_teardown
  fi
}

create_authority() {
  local authority="$1"
  run openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -sha256 \
    -days 2 -subj "/CN=NES test $authority" \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' \
    -keyout "$ISSUER_DIR/$authority.key" -out "$ISSUER_DIR/$authority.pem"
  assert_success
}

issue_worker_certificate() {
  local worker="$1" authority="$2" serial="$3"
  run openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -sha256 \
    -subj "/CN=$worker" -keyout "tls/$worker.key" -out "$ISSUER_DIR/$worker.csr"
  assert_success
  cat > "$ISSUER_DIR/$worker.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$worker
EOF
  run openssl x509 -req -in "$ISSUER_DIR/$worker.csr" \
    -CA "$ISSUER_DIR/$authority.pem" -CAkey "$ISSUER_DIR/$authority.key" \
    -set_serial "$serial" -days 2 -sha256 -extfile "$ISSUER_DIR/$worker.ext" \
    -out "tls/$worker.pem"
  assert_success
}

start_mtls_workers() {
  local copy_container copy_status=0
  copy_container=$(docker create -v "$TEST_VOLUME:/workdir" --entrypoint /bin/true "$CLI_IMAGE")
  docker cp tls "$copy_container:/workdir" || copy_status=$?
  docker rm "$copy_container"
  [ "$copy_status" -eq 0 ]

  tests/util/create_compose.sh tests/good/two-workers-mtls.yaml > docker-compose.yaml
  yq -i '.networks.default.labels."nes-test" = "worker-mtls"' docker-compose.yaml
  run docker compose up -d --wait --wait-timeout 60
  assert_success
}

untrusted_client_was_rejected() {
  sync_workdir
  grep -q 'Transport handshake failed:.*UnknownIssuer' worker-2/singleNodeWorker.log
}

assert_finite_query_completes() {
  run docker_nes_cli start
  assert_success
  local query_id="$output"

  wait_until_status tests/good/two-workers-mtls.yaml Stopped "$query_id" \
    --require-healthy '^worker-[12]$'
  assert_json_contains '[
    {"worker":"worker-1:8080","query_status":"Stopped"},
    {"worker":"worker-2:8080","query_status":"Stopped"}
  ]' "$output"
  [ "$(jq 'length' <<< "$output")" -eq 3 ]

  sync_workdir
  assert_file_exists worker-2/out.csv
  seq 0 9999 > expected.csv
  # FileSink writes one schema header; scheduling need not preserve row order.
  tail -n +2 worker-2/out.csv | LC_ALL=C sort -n > actual.csv
  run diff -u expected.csv actual.csv
  assert_success
}

@test "two workers transfer every query row over mTLS" {
  start_mtls_workers
  assert_finite_query_completes
}

@test "an untrusted worker certificate prevents query data transfer" {
  create_authority untrusted
  issue_worker_certificate worker-1 untrusted 1
  # Both workers still trust the original CA, so only worker-1's identity is invalid.
  start_mtls_workers
  run docker_nes_cli start
  assert_success
  local query_id="$output"

  wait_until untrusted_client_was_rejected
  sync_workdir
  # An opened file can contain its schema header, but it must contain no data rows.
  if [ -f worker-2/out.csv ]; then
    [ "$(tail -n +2 worker-2/out.csv | wc -l)" -eq 0 ]
  fi
  run docker_nes_cli stop "$query_id"
  assert_success
}

transfer_has_output() {
  docker compose exec -T worker-2 sh -c 'test -f out.csv && [ "$(wc -l < out.csv)" -gt 1 ]'
}

assert_survivor_is_healthy() {
  local worker="$1" container="$2" original_state="$3"
  run docker inspect --format '{{.State.Running}} {{.State.StartedAt}} {{.RestartCount}}' "$container"
  assert_success
  [ "$output" = "$original_state" ]
  run docker compose exec -T "$worker" /bin/grpc_health_probe \
    -addr="$worker:8080" -connect-timeout=2s -rpc-timeout=2s
  assert_success
}

assert_peer_crash_is_isolated() {
  local killed_worker="$1" surviving_worker="$2"
  start_mtls_workers
  run docker_nes_cli start "$(cat <<'SQL'
SELECT * FROM GENERATOR(
    'CSV' AS "INPUT_FORMATTER"."TYPE",
    'worker-1:8080' AS "SOURCE"."HOST",
    'NONE' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
    'SEQUENCE UINT64 0 10000000 1' AS "SOURCE"."GENERATOR_SCHEMA",
    'EMIT_RATE 1000' AS "SOURCE"."GENERATOR_RATE_CONFIG",
    SCHEMA(NUMBER UINT64 NOT NULL) AS "SOURCE"."SCHEMA"
) INTO FILE_SINK
SQL
)"
  assert_success
  local query_id="$output"
  wait_until_status tests/good/two-workers-mtls.yaml Running "$query_id" \
    --require-healthy '^worker-[12]$'
  wait_until transfer_has_output

  local killed_container surviving_container original_state
  killed_container=$(docker compose ps -q "$killed_worker")
  surviving_container=$(docker compose ps -q "$surviving_worker")
  original_state=$(docker inspect --format '{{.State.Running}} {{.State.StartedAt}} {{.RestartCount}}' "$surviving_container")
  [[ "$original_state" == true\ * ]]

  run docker compose kill --signal SIGKILL "$killed_worker"
  assert_success
  run docker inspect --format '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}' "$killed_container"
  assert_success
  [ "$output" = 'exited 137 false' ]

  # Exercise the survivor's health RPC while disconnect handling and retries run.
  for attempt in {1..5}; do
    sleep 1
    assert_survivor_is_healthy "$surviving_worker" "$surviving_container" "$original_state"
  done

  run docker compose up -d --wait --wait-timeout 60 "$killed_worker"
  assert_success
  # A new query must use the surviving worker's network service successfully.
  assert_finite_query_completes
  assert_survivor_is_healthy "$surviving_worker" "$surviving_container" "$original_state"
}

@test "receiver survives an abruptly killed mTLS sender" {
  assert_peer_crash_is_isolated worker-1 worker-2
}

@test "sender survives an abruptly killed mTLS receiver" {
  assert_peer_crash_is_isolated worker-2 worker-1
}
