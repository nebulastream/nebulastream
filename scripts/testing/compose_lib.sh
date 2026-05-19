# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Building blocks for assembling docker-compose stacks for distributed bats
# suites. Two emitters print compose YAML to stdout:
#
#   emit_workers  <topology.yaml>
#   emit_frontend <kind>             # cli | repl | systest
#
# The framework wraps a stack as the merge of (frontend, workers, optional
# suite-local overlay), giving:
#   docker compose -f frontend.yaml -f workers.yaml [-f tests/util/compose.overlay.yaml] up
#
# Both emitters propagate the same cross-cutting env vars via the bare-name
# `environment: [VAR]` form so they are forwarded from the host shell when set
# and silently absent otherwise:
#
#   LLVM_PROFILE_FILE   coverage (set by add_e2e_test when CODE_COVERAGE=ON)
#   ASAN_OPTIONS        sanitizer (future)
#   UBSAN_OPTIONS       sanitizer (future)
#   TSAN_OPTIONS        sanitizer (future)
#
# All worker-side tuning (execution_mode, number_of_worker_threads, buffer
# sizes, ...) lives in the topology's per-worker `config:` block, written into
# the container by an entrypoint shim that decodes base64 — never passed as
# extra CLI args here.

_compose_propagated_env=(LLVM_PROFILE_FILE ASAN_OPTIONS UBSAN_OPTIONS TSAN_OPTIONS)

_compose_env_block() {
  # Emit `environment:` with the propagated names, indented by $1 spaces.
  local indent="$1"
  local pad
  pad=$(printf '%*s' "$indent" '')
  printf '%s%s\n' "$pad" "environment:"
  local var
  for var in "${_compose_propagated_env[@]}"; do
    printf '%s  - %s\n' "$pad" "$var"
  done
}

_compose_require() {
  local var
  for var in "$@"; do
    if [ -z "${!var}" ]; then
      echo "ERROR: $var environment variable must be set" >&2
      return 1
    fi
  done
}

emit_workers() {
  local topology="$1"
  if [ ! -f "$topology" ]; then
    echo "ERROR: topology file not found: $topology" >&2
    return 1
  fi
  _compose_require WORKER_IMAGE TEST_VOLUME || return 1
  if ! command -v yq >/dev/null 2>&1; then
    echo "ERROR: yq is required" >&2
    return 1
  fi

  local workdir="${CONTAINER_WORKDIR:-/workdir}"

  printf 'services:\n'

  local count
  count=$(yq '.workers | length' "$topology")
  local i host host_name port data has_config config_b64
  for i in $(seq 0 $((count - 1))); do
    host=$(yq -r ".workers[$i].host" "$topology")
    host_name="${host%:*}"
    port="${host#*:}"
    data=$(yq -r ".workers[$i].data_address" "$topology")
    has_config=$(yq ".workers[$i] | has(\"config\")" "$topology")

    printf '  %s:\n' "$host_name"
    printf '    image: %s\n' "$WORKER_IMAGE"
    printf '    pull_policy: never\n'
    printf '    working_dir: %s/%s\n' "$workdir" "$host_name"
    _compose_env_block 4
    printf '    healthcheck:\n'
    printf '      test: ["CMD", "/bin/grpc_health_probe", "-addr=%s:%s", "-connect-timeout", "5s"]\n' "$host_name" "$port"
    printf '      interval: 1s\n'
    printf '      timeout: 5s\n'
    printf '      retries: 10\n'
    printf '      start_period: 60s\n'

    if [ "$has_config" = "true" ]; then
      config_b64=$(yq ".workers[$i].config" "$topology" | base64 | tr -d '\n')
      printf '    entrypoint: ["/bin/bash", "-c"]\n'
      printf '    command:\n'
      printf '      - |\n'
      printf '        set -e\n'
      printf '        mkdir -p %s/configs\n' "$workdir"
      printf "        echo '%s' | base64 -d > %s/configs/%s.yaml\n" "$config_b64" "$workdir" "$host_name"
      printf '        exec nes-single-node-worker --grpc=%s:%s --data_address=%s --configPath=%s/configs/%s.yaml\n' \
        "$host_name" "$port" "$data" "$workdir" "$host_name"
    else
      printf '    command: ["--grpc=%s:%s", "--data_address=%s"]\n' "$host_name" "$port" "$data"
    fi

    printf '    volumes:\n'
    printf '      - %s:%s\n' "$TEST_VOLUME" "$workdir"
    if [ -n "$TESTDATA_VOLUME" ]; then
      printf '      - %s:/data\n' "$TESTDATA_VOLUME"
    fi
    if [ -n "$TESTCONFIG_VOLUME" ] && [ -n "$NES_DIR" ]; then
      printf '      - %s:%s\n' "$TESTCONFIG_VOLUME" "$NES_DIR"
    fi
  done

  printf 'volumes:\n'
  printf '  %s:\n' "$TEST_VOLUME"
  printf '    external: true\n'
  if [ -n "$TESTDATA_VOLUME" ]; then
    printf '  %s:\n' "$TESTDATA_VOLUME"
    printf '    external: true\n'
  fi
  if [ -n "$TESTCONFIG_VOLUME" ]; then
    printf '  %s:\n' "$TESTCONFIG_VOLUME"
    printf '    external: true\n'
  fi
}

emit_frontend() {
  local kind="$1"
  _compose_require TEST_VOLUME || return 1
  local workdir="${CONTAINER_WORKDIR:-/workdir}"

  case "$kind" in
    cli)
      _compose_require CLI_IMAGE || return 1
      printf 'services:\n'
      printf '  nes-cli:\n'
      printf '    image: %s\n' "$CLI_IMAGE"
      printf '    pull_policy: never\n'
      printf '    environment:\n'
      local var
      for var in "${_compose_propagated_env[@]}"; do
        printf '      - %s\n' "$var"
      done
      printf '      - NES_TOPOLOGY_FILE\n'
      printf '      - XDG_STATE_HOME=%s/.xdg-state\n' "$workdir"
      printf '    stop_grace_period: 0s\n'
      printf '    working_dir: %s\n' "$workdir"
      printf '    command: ["sleep", "infinity"]\n'
      printf '    volumes:\n'
      printf '      - %s:%s\n' "$TEST_VOLUME" "$workdir"
      ;;
    repl)
      _compose_require REPL_IMAGE || return 1
      printf 'services:\n'
      printf '  nes-repl:\n'
      printf '    image: %s\n' "$REPL_IMAGE"
      printf '    pull_policy: never\n'
      _compose_env_block 4
      printf '    stop_grace_period: 0s\n'
      printf '    working_dir: %s\n' "$workdir"
      printf '    command: ["sleep", "infinity"]\n'
      printf '    volumes:\n'
      printf '      - %s:%s\n' "$TEST_VOLUME" "$workdir"
      ;;
    systest)
      _compose_require SYSTEST_IMAGE NES_DIR CONTAINER_WORKDIR TESTDATA_VOLUME TESTCONFIG_VOLUME || return 1
      printf 'services:\n'
      printf '  systest:\n'
      printf '    image: %s\n' "$SYSTEST_IMAGE"
      printf '    pull_policy: never\n'
      _compose_env_block 4
      printf '    stop_grace_period: 0s\n'
      printf '    working_dir: %s\n' "$workdir"
      printf '    command: ["sleep", "infinity"]\n'
      printf '    volumes:\n'
      printf '      - %s:/data\n' "$TESTDATA_VOLUME"
      printf '      - %s:%s\n' "$TESTCONFIG_VOLUME" "$NES_DIR"
      printf '      - %s:%s\n' "$TEST_VOLUME" "$workdir"
      ;;
    *)
      echo "ERROR: unknown frontend kind: $kind (expected cli|repl|systest)" >&2
      return 1
      ;;
  esac

  printf 'volumes:\n'
  printf '  %s:\n' "$TEST_VOLUME"
  printf '    external: true\n'
  if [ -n "$TESTDATA_VOLUME" ]; then
    printf '  %s:\n' "$TESTDATA_VOLUME"
    printf '    external: true\n'
  fi
  if [ -n "$TESTCONFIG_VOLUME" ]; then
    printf '  %s:\n' "$TESTCONFIG_VOLUME"
    printf '    external: true\n'
  fi
}
