#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs a topology/query combo the same way nes-cli-compose.sh does, but
# starts nes-single-node-worker itself first, inside one ephemeral
# `nebulastream/nes-development:local` container -- the "start the worker,
# then register the query via nes-cli" workflow in a single command. Uses
# the dev image (not a packaged nes-worker/nes-cli image) specifically
# because it already has `ovc`/OpenVINO on PATH, so MODEL_INFERENCE's model
# import works with zero venv/PATH setup on your part. Nothing is left
# running afterward: the container is removed on exit (--rm) and the
# worker is killed when nes-cli returns or you Ctrl-C.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: nes-cli-compose-docker.sh -t topo1.yaml [-t topo2.yaml ...] -q query1.sql [-q query2.sql ...]
                                  [-c start|dump] [-i IMAGE] [-w WORKER_BIN] [-b CLI_BIN] [-p PORT]
                                  [-- extra nes-cli args]

  -t FILE   topology/source yaml file (repeatable, at least one required)
  -q FILE   query file (repeatable, at least one required); passed straight
            through to nes-cli-compose.sh, same rules apply (multiple
            queries per file separated by a line containing only ';'; a
            query file must not START with a '--' comment -- nes-cli's own
            argument parser reads a leading '--' as a flag, not a query)
  -c CMD    nes-cli subcommand: start (default) or dump
  -p PORT   port to wait on before registering the query, i.e. the worker's
            grpc bind port (default: 8080, nes-single-node-worker's own
            default and what every current FischerTechnik yaml uses).
            Only starts/waits for ONE worker -- a multi-worker topology
            (e.g. factory-fault-demo.yaml's edge+laptop split) needs its
            own nes-single-node-worker per host, which this script does not
            set up; run those by hand instead.
  -i IMAGE  docker image to run in (default: nebulastream/nes-development:local)
  -w PATH   path to the nes-single-node-worker binary, relative to the repo
            root (default: cmake-build-debug-docker/nes-single-node-worker/nes-single-node-worker)
  -b PATH   path to the nes-cli binary, relative to the repo root
            (default: cmake-build-debug-docker/nes-frontend/apps/nes-cli)
  --        remaining args are passed through to nes-cli-compose.sh (and on to nes-cli)

Both binaries must already be built (e.g. via a `docker run ... ninja
nes-single-node-worker nes-cli` in this same image) -- this script only
runs them, it does not build.
EOF
}

image="nebulastream/nes-development:local"
worker_bin="cmake-build-debug-docker/nes-single-node-worker/nes-single-node-worker"
cli_bin="cmake-build-debug-docker/nes-frontend/apps/nes-cli"
subcommand="start"
port="8080"
topo_args=()
query_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t) topo_args+=(-t "$2"); shift 2 ;;
    -q) query_args+=(-q "$2"); shift 2 ;;
    -c) subcommand="$2"; shift 2 ;;
    -p) port="$2"; shift 2 ;;
    -i) image="$2"; shift 2 ;;
    -w) worker_bin="$2"; shift 2 ;;
    -b) cli_bin="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done
extra_args=("$@")

[[ ${#topo_args[@]} -gt 0 ]] || { echo "Error: at least one -t <topology.yaml> is required" >&2; exit 1; }
[[ ${#query_args[@]} -gt 0 ]] || { echo "Error: at least one -q <query file> is required" >&2; exit 1; }

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for f in "$repo_root/$worker_bin" "$repo_root/$cli_bin"; do
  [[ -x "$f" ]] || { echo "Error: not an executable file: $f (build it first)" >&2; exit 1; }
done

# Runs inside the container as PID 1: starts the worker in the background,
# waits for its gRPC port to accept connections, runs nes-cli-compose.sh,
# then always kills the worker on exit (including Ctrl-C via the INT/TERM
# trap) so nothing is left running once this script returns.
#
# `nes-cli start` registers the query and returns immediately -- it does
# NOT block for the query's lifetime. For "start" (unlike "dump", which is
# a one-shot check), the worker is kept alive afterward with `wait
# "$worker_pid"` so the query actually keeps streaming until you Ctrl-C;
# without this the trap fires the instant nes-cli-compose.sh returns and
# kills the worker before a single tuple is processed (hit this directly).
inner_script='
set -uo pipefail
worker_bin="$1"; cli_bin="$2"; subcommand="$3"; port="$4"; shift 4

"$worker_bin" &
worker_pid=$!
cleanup() { kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

ready=0
for _ in $(seq 1 50); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.2
done
[[ "$ready" -eq 1 ]] || { echo "Error: nes-single-node-worker did not start listening on :$port in time" >&2; exit 1; }

./scripts/nes-cli-compose.sh -b "$cli_bin" -c "$subcommand" "$@"

if [ "$subcommand" = "start" ]; then
  echo "Query registered -- worker running in the foreground. Ctrl-C to stop." >&2
  wait "$worker_pid"
fi
'

# -t only when stdin is an actual terminal, so Ctrl-C reaches the container
# as SIGINT for interactive `start` runs, without breaking non-interactive
# invocations (e.g. `dump` piped into `tail`, no TTY to allocate).
docker_tty_flag=()
[[ -t 0 ]] && docker_tty_flag=(-t)

# --network host: the worker needs to reach services bound to the host's own
# localhost (e.g. a local MQTT broker for a webapp demo) -- inside the
# container's default network namespace, "localhost" means the container
# itself, not the host, so those connections fail otherwise. LAN targets
# like the factory broker are unaffected either way.
docker run --rm -i "${docker_tty_flag[@]}" --network host \
  -v "$repo_root:$repo_root" -w "$repo_root" \
  "$image" \
  bash -c "$inner_script" bash "$worker_bin" "$cli_bin" "$subcommand" "$port" "${topo_args[@]}" "${query_args[@]}" "${extra_args[@]}"
