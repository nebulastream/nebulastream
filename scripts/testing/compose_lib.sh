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
#   ASAN_OPTIONS, UBSAN_OPTIONS, TSAN_OPTIONS   sanitizer runtime options
#
# Worker-side tuning (execution_mode, number_of_worker_threads, buffer sizes,
# ...) is sourced from `_compose_default_worker_config` and deep-merged with
# the topology's per-worker `config:` block (topology wins). The resulting
# yaml is base64-encoded into the worker's entrypoint, which decodes it onto
# the mounted TEST_VOLUME at `${workdir}/configs/<host>.yaml` before exec'ing
# the worker binary. We avoid docker compose's `configs:` with inline
# `content:` because in Docker-out-of-Docker setups it's backed by a bind
# mount from a path the host daemon can't see.

_compose_propagated_env=(LLVM_PROFILE_FILE ASAN_OPTIONS UBSAN_OPTIONS TSAN_OPTIONS)

# Default worker config applied to every worker unless the topology overrides
# specific keys. See _emit_worker_effective_config for merge semantics.
_compose_default_worker_config='worker:
  default_query_execution:
    execution_mode: INTERPRETER
  query_engine:
    number_of_worker_threads: 2'

# Emit `- VAR` lines for each propagated env var, prefixed by $1 (e.g. '      ').
_compose_env_items() {
  local prefix="$1" var
  for var in "${_compose_propagated_env[@]}"; do
    printf '%s- %s\n' "$prefix" "$var"
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

# Compute a worker's effective config yaml by deep-merging the topology's
# per-worker `config:` block onto the library default. jq's `*` operator is a
# recursive merge with right-side wins on scalars (a topology setting
# `worker.query_engine.number_of_worker_threads: 8` keeps the default
# execution_mode while replacing the thread count). Arrays are replaced
# wholesale, not concatenated, so a list-valued override fully supplants the
# default list.
#
# The merge is delegated to jq rather than yq's `*` because the dev image
# ships python-yq (a jq wrapper, apt's `yq` package), not mike-farah/yq, so
# `yq ea -P 'select(fi==N) * ...'` and friends aren't available — yq is only
# used here for yaml<->json conversions on either side of the jq pipeline.
_emit_worker_effective_config() {
  local topology="$1" idx="$2"
  local default_json topo_json
  default_json=$(printf '%s\n' "$_compose_default_worker_config" | yq .)
  topo_json=$(yq ".workers[$idx].config // {}" "$topology")
  jq -n --argjson d "$default_json" --argjson t "$topo_json" '$d * $t' | yq -y .
}

# Emit a single worker service block. Internal; called by emit_workers.
# The effective worker config is base64-encoded into the entrypoint, which
# decodes it into a file on the mounted TEST_VOLUME before exec'ing the worker
# binary. This avoids docker compose's `configs:` with inline `content:`, which
# in Docker-out-of-Docker setups (compose running inside the dev container
# against the host daemon) is implemented as a bind mount sourced from a path
# the daemon can't see — failing with "bind source path does not exist".
_emit_worker_service() {
  local topology="$1" idx="$2" workdir="$3"
  local host host_name port data
  host=$(yq -r ".workers[$idx].host" "$topology")
  host_name="${host%:*}"
  port="${host#*:}"
  data=$(yq -r ".workers[$idx].data_address" "$topology")
  local config_b64
  config_b64=$(_emit_worker_effective_config "$topology" "$idx" | base64 | tr -d '\n')

  cat <<EOF
  ${host_name}:
    image: ${WORKER_IMAGE}
    pull_policy: never
    working_dir: ${workdir}/${host_name}
    environment:
$(_compose_env_items '      ')
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=${host_name}:${port}", "-connect-timeout", "5s"]
      interval: 1s
      timeout: 5s
      retries: 10
      start_period: 60s
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        mkdir -p ${workdir}/configs
        echo '${config_b64}' | base64 -d > ${workdir}/configs/${host_name}.yaml
        exec nes-single-node-worker --grpc=${host_name}:${port} --data_address=${data} --configPath=${workdir}/configs/${host_name}.yaml
    volumes:
      - ${TEST_VOLUME}:${workdir}
EOF
  if [ -n "$TESTDATA_VOLUME" ]; then
    cat <<EOF
      - ${TESTDATA_VOLUME}:/data
EOF
  fi
  if [ -n "$TESTCONFIG_VOLUME" ] && [ -n "$NES_DIR" ]; then
    cat <<EOF
      - ${TESTCONFIG_VOLUME}:${NES_DIR}
EOF
  fi
}

# Emit the trailing `volumes:` block declaring external volumes referenced
# anywhere in the stack. Shared by emit_workers and emit_frontend.
_emit_external_volumes() {
  cat <<EOF
volumes:
  ${TEST_VOLUME}:
    external: true
EOF
  if [ -n "$TESTDATA_VOLUME" ]; then
    cat <<EOF
  ${TESTDATA_VOLUME}:
    external: true
EOF
  fi
  if [ -n "$TESTCONFIG_VOLUME" ]; then
    cat <<EOF
  ${TESTCONFIG_VOLUME}:
    external: true
EOF
  fi
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
  local count
  count=$(yq '.workers | length' "$topology")

  echo 'services:'
  local i
  for i in $(seq 0 $((count - 1))); do
    _emit_worker_service "$topology" "$i" "$workdir"
  done
  _emit_external_volumes
}

emit_frontend() {
  local kind="$1"
  _compose_require TEST_VOLUME || return 1
  local workdir="${CONTAINER_WORKDIR:-/workdir}"

  case "$kind" in
    cli)
      _compose_require CLI_IMAGE || return 1
      cat <<EOF
services:
  nes-cli:
    image: ${CLI_IMAGE}
    pull_policy: never
    environment:
$(_compose_env_items '      ')
      - NES_TOPOLOGY_FILE
      - XDG_STATE_HOME=${workdir}/.xdg-state
    stop_grace_period: 0s
    working_dir: ${workdir}
    command: ["sleep", "infinity"]
    volumes:
      - ${TEST_VOLUME}:${workdir}
EOF
      ;;
    repl)
      _compose_require REPL_IMAGE || return 1
      cat <<EOF
services:
  nes-repl:
    image: ${REPL_IMAGE}
    pull_policy: never
    environment:
$(_compose_env_items '      ')
    stop_grace_period: 0s
    working_dir: ${workdir}
    command: ["sleep", "infinity"]
    volumes:
      - ${TEST_VOLUME}:${workdir}
EOF
      ;;
    systest)
      _compose_require SYSTEST_IMAGE NES_DIR CONTAINER_WORKDIR TESTDATA_VOLUME TESTCONFIG_VOLUME || return 1
      cat <<EOF
services:
  systest:
    image: ${SYSTEST_IMAGE}
    pull_policy: never
    environment:
$(_compose_env_items '      ')
    stop_grace_period: 0s
    working_dir: ${workdir}
    command: ["sleep", "infinity"]
    volumes:
      - ${TESTDATA_VOLUME}:/data
      - ${TESTCONFIG_VOLUME}:${NES_DIR}
      - ${TEST_VOLUME}:${workdir}
EOF
      ;;
    *)
      echo "ERROR: unknown frontend kind: $kind (expected cli|repl|systest)" >&2
      return 1
      ;;
  esac

  _emit_external_volumes
}
