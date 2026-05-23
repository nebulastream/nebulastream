# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Shared bats helpers for the distributed/docker-compose test suites in this
# repo. Sourced by individual *.bats files via the NES_BATS_LIB env var
# injected from CMake.
#
# The library has two layers:
#
#   Layer 1 — primitives (use anywhere):
#     nes_require_env, nes_require_executable
#     nes_cleanup_leaked_resources, nes_build_runtime_image, nes_build_app_image
#     assert_json_equal, assert_json_contains
#
#   Layer 2 — preset for the cli/repl/MQTT-style suites:
#     nes_distributed_setup_file, nes_distributed_teardown_file
#     nes_distributed_setup, nes_distributed_teardown
#     sync_workdir, setup_distributed
#     docker_nes_cli, wait_until_status   (used by cli/MQTT* suites)
#
#   Layer-2 callers pass the client binary path to nes_distributed_setup_file
#   (e.g. "$NES_CLI"); the lib derives the test label / image-name prefixes /
#   app-image env-var name from `basename` of that path. Testdata is the
#   directory containing the .bats file.

# Load the standard bats helper libraries when sourced from inside a bats test
# (bats injects `bats_load_library`). They are resolved via BATS_LIB_PATH, which
# CMake's CheckTestUtilities.cmake sets after verifying they're installed.
if declare -F bats_load_library >/dev/null 2>&1; then
    bats_load_library bats-support
    bats_load_library bats-assert
    bats_load_library bats-file
fi

# Compose-emitter helpers (emit_workers, emit_frontend). Sourced relative to
# this file's directory.
source "$(dirname "${BASH_SOURCE[0]}")/compose_lib.sh"

# ---------------------------------------------------------------------------
# Layer 1 — primitives
# ---------------------------------------------------------------------------

nes_require_env() {
  local var
  for var in "$@"; do
    if [ -z "${!var}" ]; then
      echo "ERROR: $var environment variable must be set" >&2
      exit 1
    fi
  done
}

nes_require_executable() {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "ERROR: file does not exist: $path" >&2
    exit 1
  fi
  if [ ! -x "$path" ]; then
    echo "ERROR: file is not executable: $path" >&2
    exit 1
  fi
}

# Clean up containers, networks and images leaked by previous (potentially
# crashed) test runs. Networks are matched by the suite-wide `nes-test=<label>`
# tag attached to the default network by setup_distributed; images are matched
# against the reference patterns passed as remaining args.
nes_cleanup_leaked_resources() {
  local label="$1"
  shift

  for net in $(docker network ls --filter "label=nes-test=$label" -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter "label=nes-test=$label" 2>/dev/null || true

  if [ "$#" -eq 0 ]; then
    return 0
  fi

  local filter_args=()
  for ref in "$@"; do
    filter_args+=(--filter "reference=$ref")
  done

  for img in $(docker images "${filter_args[@]}" -q 2>/dev/null); do
    docker ps -aq --filter ancestor="$img" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker images "${filter_args[@]}" -q | xargs -r docker image rm -f 2>/dev/null || true
}

# Build a worker-style image (FROM $NES_RUNTIME_BASE_IMAGE, COPY <bin>, ENTRYPOINT <bin>).
# Exports the resulting image tag under the env var named in arg1.
nes_build_runtime_image() {
  local image_var="$1"
  local prefix="$2"
  local bin_path="$3"
  local container_bin="$4"

  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  local image_tag="${prefix}-${suffix}"
  local ctx=$(mktemp -d)
  cp "$(realpath "$bin_path")" "$ctx/$container_bin"
  docker build --load -t "$image_tag" -f - "$ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY $container_bin /usr/bin
    ENTRYPOINT ["$container_bin"]
EOF
  rm -rf "$ctx"
  export "$image_var=$image_tag"
}

# Build an app-style image (FROM $NES_RUNTIME_BASE_IMAGE, COPY <bin>, no entrypoint —
# invoked via `docker compose exec`). Exports the tag under arg1.
nes_build_app_image() {
  local image_var="$1"
  local prefix="$2"
  local bin_path="$3"
  local container_bin="$4"

  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  local image_tag="${prefix}-${suffix}"
  local ctx=$(mktemp -d)
  cp "$(realpath "$bin_path")" "$ctx/$container_bin"
  docker build --load -t "$image_tag" -f - "$ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY $container_bin /usr/bin
EOF
  rm -rf "$ctx"
  export "$image_var=$image_tag"
}

# Normalize JSON (sort keys, pretty-print). On parse failure, print a labelled
# error with the raw input and the jq diagnostic, then return 1.
_nes_jq_normalize() {
  local label="$1"
  local input="$2"
  local out
  if ! out=$(echo "$input" | jq --sort-keys . 2>&1); then
    {
      echo "JSON parse error in $label:"
      echo "--- raw $label ---"
      echo "$input"
      echo "--- jq error ---"
      echo "$out"
    } >&2
    return 1
  fi
  printf '%s\n' "$out"
}

assert_json_equal() {
  local expected="$1"
  local actual="$2"

  local exp_norm act_norm
  exp_norm=$(_nes_jq_normalize expected "$expected") || return 1
  act_norm=$(_nes_jq_normalize actual   "$actual")   || return 1

  if [ "$exp_norm" = "$act_norm" ]; then
    return 0
  fi

  {
    echo "assert_json_equal: JSON differs"
    diff -u --label expected --label actual \
      <(printf '%s\n' "$exp_norm") \
      <(printf '%s\n' "$act_norm")
  } >&2
  return 1
}

assert_json_contains() {
  local expected="$1"
  local actual="$2"

  local exp_norm act_norm result
  exp_norm=$(_nes_jq_normalize 'expected (subset)' "$expected") || return 1
  act_norm=$(_nes_jq_normalize actual              "$actual")   || return 1

  result=$(jq -n --argjson exp "$expected" --argjson act "$actual" '$act | contains($exp)')
  if [ "$result" = "true" ]; then
    return 0
  fi

  {
    echo "assert_json_contains: actual does not contain expected subset"
    echo "--- expected (subset) ---"
    printf '%s\n' "$exp_norm"
    echo "--- actual ---"
    printf '%s\n' "$act_norm"
  } >&2
  return 1
}

# ---------------------------------------------------------------------------
# Layer 2 — preset for cli/repl/MQTT distributed suites
# ---------------------------------------------------------------------------

nes_distributed_setup_file() {
  # Sets up per-suite docker resources for one of the supported frontend kinds.
  # Naming is driven by NES_CTEST_NAME (exported by add_e2e_test) so every
  # artifact is uniquely tied to its ctest name without any path-derived magic:
  #   NES_COMPOSE_PROJECT       = $NES_CTEST_NAME
  #   worker / app image prefix = ${NES_CTEST_NAME}-worker / -app
  #   NES_BATS_APP_IMAGE_VAR    = <KIND>_IMAGE  (CLI_IMAGE, REPL_IMAGE, SYSTEST_IMAGE)
  local kind="$1"
  local bin_path
  case "$kind" in
    cli)     bin_path="$NES_CLI" ;;
    repl)    bin_path="$NES_REPL" ;;
    systest) bin_path="$NES_SYSTEST" ;;
    *) echo "ERROR: nes_distributed_setup_file: unsupported kind '$kind' (expected cli|repl|systest)" >&2; return 1 ;;
  esac

  nes_require_env NES_CTEST_NAME NES_WORKER NES_TEST_TMP_DIR NES_RUNTIME_BASE_IMAGE
  nes_require_executable "$NES_WORKER"
  nes_require_executable "$bin_path"

  export NES_COMPOSE_PROJECT="$NES_CTEST_NAME"
  export NES_BATS_APP_IMAGE_VAR="${kind^^}_IMAGE"
  local worker_prefix="${NES_CTEST_NAME}-worker"
  local app_prefix="${NES_CTEST_NAME}-app"

  nes_cleanup_leaked_resources "$NES_COMPOSE_PROJECT" "${worker_prefix}-*" "${app_prefix}-*"

  # Per-test images use random suffixes so parallel checkouts don't collide.
  # Docker's COPY layer is content-addressed, so unchanged binaries hit cache.
  nes_build_runtime_image WORKER_IMAGE "$worker_prefix" \
    "$NES_WORKER" nes-single-node-worker
  nes_build_app_image "$NES_BATS_APP_IMAGE_VAR" "$app_prefix" \
    "$bin_path" "$(basename "$bin_path")"

  echo "# Using kind: $kind" >&3
  echo "# Using client binary: $bin_path" >&3
  echo "# Using NES_WORKER: $NES_WORKER" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using ${NES_BATS_APP_IMAGE_VAR}: ${!NES_BATS_APP_IMAGE_VAR}" >&3
}

nes_distributed_teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi "$WORKER_IMAGE" || true
  docker rmi "${!NES_BATS_APP_IMAGE_VAR}" || true
}

nes_distributed_setup() {
  # Deterministic per-test working dir under the mounted workspace (not /tmp,
  # so docker-compose containers running on the host can access it). The path
  # is a function of the ctest name and the bats-internal test name — both
  # shell-safe and globally unique — so reruns of the same test land at the
  # same location and suites with the same .bats filename (cli/repl both name
  # their distributed suite distributed.bats) can't collide. Coverage relies
  # on this: the bats-coverage-collect fixture copies profraws into
  # build/ccov/bats-profraws/ preserving the source subpath, so deterministic
  # source paths → deterministic destination filenames → the entries
  # cmake-codecov has in profraw.list stay valid across reruns.
  nes_require_env NES_CTEST_NAME
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR="$NES_TEST_TMP_DIR/${NES_CTEST_NAME}/${BATS_TEST_NAME}"
  rm -rf "$TMP_DIR"
  mkdir -p "$TMP_DIR"
  # Testdata lives in the same dir as the .bats file (by convention named
  # `tests/`); copying it as a subdir of TMP_DIR matches the path prefix used
  # in the @test bodies (e.g. tests/good/example.yaml).
  cp -r "$(dirname "$BATS_TEST_FILENAME")" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  local volume
  volume=$(docker volume create)
  local volume_host_container
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker cp . $volume_host_container:/data
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "# Using test volume: $TEST_VOLUME" >&3
}

sync_workdir() {
  local volume_host_container
  volume_host_container=$(docker run -d --rm -v $TEST_VOLUME:/data alpine sleep infinite)
  docker cp $volume_host_container:/data/. .
  docker stop -t0 $volume_host_container
}

nes_distributed_teardown() {
  sync_workdir || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

# Assemble a docker-compose stack from generated frontend + workers files,
# plus an optional suite-local overlay (tests/util/compose.overlay.yaml) for
# extra services like mqtt-broker. Assumes the caller has `cd`'d into the
# per-test working directory.
#
# Usage: setup_distributed <kind> <topology.yaml>     kind ∈ {cli, repl, systest}
setup_distributed() {
  local kind="$1"
  local topology="$2"
  # The cli reads its topology either from -t or from NES_TOPOLOGY_FILE; forward
  # the path via the compose env so tests that omit -t still resolve it.
  export NES_TOPOLOGY_FILE="$topology"
  emit_frontend "$kind"  > frontend.compose.yaml
  emit_workers  "$topology" > workers.compose.yaml
  # Tag the default network with a suite-wide label so leftover networks from
  # crashed runs can be matched by nes_cleanup_leaked_resources. The compose
  # project name is left to compose's default (cwd basename), which is unique
  # per test because each test runs in its own TMP_DIR — keeps container
  # names from colliding between tests in the same suite.
  cat > network.compose.yaml <<EOF
networks:
  default:
    labels:
      nes-test: "${NES_COMPOSE_PROJECT:-distributed}"
EOF
  # Export the file set via the standard COMPOSE_FILE env so subsequent
  # `docker compose` invocations (exec, down, logs, ...) in the same test pick
  # up the same stack without needing -f flags re-passed.
  local files="frontend.compose.yaml:workers.compose.yaml:network.compose.yaml"
  if [ -f tests/util/compose.overlay.yaml ]; then
    files+=":tests/util/compose.overlay.yaml"
  fi
  export COMPOSE_FILE="$files"
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
    echo "# [docker compose logs]:" >&3
    docker compose logs --no-color 2>&1 \
      | while IFS= read -r line; do echo "#   $line" >&3; done
  fi
  return $exit_code
}

docker_nes_cli() {
  # docker compose exec v2 disconnects the session when its stdin reaches EOF
  # (docker/compose#10418). In bats subshells stdin is closed, so we pipe from
  # tail to keep the connection alive.
  tail -f /dev/null | docker compose exec -T nes-cli nes-cli "$@"
}

wait_until_status() {
  local topology_file=$1
  local desired_status=$2
  local query_id=$3
  local query_status=

  for i in $(seq 1 80); do
    sleep 1
    run docker_nes_cli -t "$topology_file" status "$query_id"
    [ "$status" -eq 0 ]
    query_status=$(echo "$output" | jq -r --arg query_id "$query_id" '.[] | select(.query_id == $query_id and (has("local_query_id") | not)) | .query_status')
    if [ "$query_status" = "$desired_status" ]; then
      break
    fi
  done
  [ "$query_status" = "$desired_status" ]
}

# ---------------------------------------------------------------------------
# Offline preset — for bats suites that run binaries directly without docker.
# Suites reference the CMake-injected $NES_CLI / $NES_REPL / $NES_REPL_EMBEDDED
# / $NES_SYSTEST vars directly. Testdata is the directory containing the .bats
# file.
# ---------------------------------------------------------------------------

nes_offline_setup_file() {
  nes_require_env NES_CLI
  nes_require_env NES_REPL_EMBEDDED
  nes_require_env NES_REPL
  nes_require_env NES_SYSTEST
  nes_require_env NES_TEST_TMP_DIR

  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NES_REPL_EMBEDDED: $NES_REPL_EMBEDDED" >&3
  echo "# Using NES_REPL: $NES_REPL" >&3
  echo "# Using NES_SYSTEST: $NES_SYSTEST" >&3
}

nes_offline_teardown_file() {
  echo "# Test suite completed" >&3
}

nes_offline_setup() {
  # See nes_distributed_setup for the deterministic-path rationale.
  nes_require_env NES_CTEST_NAME
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR="$NES_TEST_TMP_DIR/${NES_CTEST_NAME}/${BATS_TEST_NAME}"
  rm -rf "$TMP_DIR"
  mkdir -p "$TMP_DIR"
  cp -r "$(dirname "$BATS_TEST_FILENAME")" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3
}
