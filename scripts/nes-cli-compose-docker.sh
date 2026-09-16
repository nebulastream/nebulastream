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
#
# `-c none` / `--none` skips nes-cli entirely -- just the worker, no
# topology/query, no -t/-q needed -- for measuring the worker on its own
# (e.g. baseline memory before any query runs).
#
# `-A` / `--attach` skips starting a worker at all: it assumes one is
# already listening on -p (e.g. a separate terminal running this same
# script with --none), and just runs the requested nes-cli operation
# (start/dump/stop/status) against it, then exits -- the worker's lifetime
# is managed entirely by that other terminal, not this invocation.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: nes-cli-compose-docker.sh -t topo1.yaml [-t topo2.yaml ...] -q query1.sql [-q query2.sql ...]
                                  [-c start|dump|stop|status|none | --none] [-A | --attach]
                                  [-i IMAGE] [-w WORKER_BIN] [-b CLI_BIN] [-p PORT] [-W VAR]
                                  [-- extra args]

  -t FILE   topology/source yaml file (repeatable). Required for start/dump
            (merged the same way nes-cli-compose.sh does) and for
            stop/status (just needs ONE file that has the right worker:
            host, to know which worker to talk to -- if you pass several,
            only the first is used for stop/status). Not required for -c none.
  -q FILE   query file (repeatable; required for start/dump, unused/not
            accepted for stop/status/none); same rules as
            nes-cli-compose.sh (multiple queries per file separated by a
            line containing only ';'; a query file must not START with a
            '--' comment -- nes-cli's own argument parser reads a leading
            '--' as a flag, not a query)
  -c CMD    nes-cli subcommand: start (default), dump, stop, status, or
            none.
              start/dump  -- as before: register (and for start, run)
                             query file(s) against a topology.
              stop        -- stop one or more running queries; pass their
                             queryId(s) after --, e.g. `-c stop -- 3`.
              status      -- show status of given queryId(s) after --, or
                             all queries if none given.
              none        -- just start nes-single-node-worker and leave it
                             running with no topology/query registered at
                             all (nes-cli is never invoked, so -t/-q are
                             neither required nor used); for
                             research/baseline measurements, e.g.
                             idle-worker memory before any query runs.
  --none    shorthand for -c none, no value needed -- combine freely with
            -W, e.g. `--none -W --worker.total_memory_in_bytes=104857600`.
  -A        shorthand for --attach.
  --attach  don't start a worker -- attach to one already listening on -p
            (run this same script with --none in another terminal first)
            and just perform -c's operation against it, then exit. This is
            how you start/stop/check queries against a long-running worker
            from a second terminal instead of spinning up a new one each
            time. Not valid with -c none (nothing to attach for).
  -W VAR    worker config override, e.g. -W --worker.total_memory_in_bytes=104857600
            (repeatable; forwarded to nes-single-node-worker's own `--`
            passthrough -- see its --help for the full dotted-path option
            tree. Unlike -t/-q/the trailing --, these go to the worker
            process itself, not to nes-cli. Ignored with --attach, since no
            worker is started here.)
  -p PORT   the worker's grpc bind port -- what to wait on before running
            an operation (default: 8080, nes-single-node-worker's own
            default and what every current FischerTechnik yaml uses), and,
            with --attach, the port assumed already listening.
            Only one worker at a time -- a multi-worker topology (e.g.
            factory-fault-demo.yaml's edge+laptop split) needs its own
            nes-single-node-worker per host, which this script does not
            set up; run those by hand instead.
  -i IMAGE  docker image to run in (default: nebulastream/nes-development:local)
  -w PATH   path to the nes-single-node-worker binary, relative to the repo
            root (default: cmake-build-debug-docker/nes-single-node-worker/nes-single-node-worker)
  -b PATH   path to the nes-cli binary, relative to the repo root
            (default: cmake-build-debug-docker/nes-frontend/apps/nes-cli)
  --        remaining args: for start/dump, passed through to
            nes-cli-compose.sh (and on to nes-cli); for stop/status, these
            ARE the queryId(s) to act on.

Both binaries must already be built (e.g. via a `docker run ... ninja
nes-single-node-worker nes-cli` in this same image) -- this script only
runs them, it does not build.

Example: start a worker in one terminal, drive it from another.
  Terminal 1:
    ./scripts/nes-cli-compose-docker.sh --none -W --worker.total_memory_in_bytes=104857600
  Terminal 2:
    ./scripts/nes-cli-compose-docker.sh --attach -t topo.yaml -q query.sql          # start a query
    ./scripts/nes-cli-compose-docker.sh --attach -t topo.yaml -c status             # list query status
    ./scripts/nes-cli-compose-docker.sh --attach -t topo.yaml -c stop -- 1          # stop queryId 1
EOF
}

image="nebulastream/nes-development:local"
worker_bin="cmake-build-debug-docker/nes-single-node-worker/nes-single-node-worker"
cli_bin="cmake-build-debug-docker/nes-frontend/apps/nes-cli"
subcommand="start"
port="8080"
attach=0
topo_files=()
query_files=()
worker_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t) topo_files+=("$2"); shift 2 ;;
    -q) query_files+=("$2"); shift 2 ;;
    -c) subcommand="$2"; shift 2 ;;
    --none) subcommand="none"; shift ;;
    -A|--attach) attach=1; shift ;;
    -W) worker_args+=("$2"); shift 2 ;;
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

if [[ "$attach" -eq 1 && "$subcommand" == "none" ]]; then
  echo "Error: --attach doesn't combine with -c none -- there would be nothing to attach to run." \
       "Start the worker itself with --none in its own terminal, then use --attach start/dump/stop/status against it." >&2
  exit 1
fi

case "$subcommand" in
  none) : ;;
  start|dump)
    [[ ${#topo_files[@]} -gt 0 ]] || { echo "Error: at least one -t <topology.yaml> is required (or use -c none)" >&2; exit 1; }
    [[ ${#query_files[@]} -gt 0 ]] || { echo "Error: at least one -q <query file> is required (or use -c none)" >&2; exit 1; }
    ;;
  stop|status)
    [[ ${#topo_files[@]} -gt 0 ]] || { echo "Error: -c $subcommand needs one -t <topology.yaml>, so nes-cli knows which worker to talk to" >&2; exit 1; }
    [[ ${#query_files[@]} -eq 0 ]] || { echo "Error: -c $subcommand doesn't take -q query files -- pass queryId(s) after --" >&2; exit 1; }
    if [[ "$subcommand" == "stop" && ${#extra_args[@]} -eq 0 ]]; then
      echo "Error: -c stop needs at least one queryId after -- (e.g. -c stop -- 3)" >&2
      exit 1
    fi
    ;;
  *) echo "Error: unknown -c subcommand '$subcommand' (expected start, dump, stop, status, or none)" >&2; exit 1 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# nes-cli persists each registered query's id -> per-worker local-query-id
# mapping to a JSON file under $XDG_STATE_HOME/nebucli/ (see
# QueryStateBackend.cpp), which `stop`/`status <id>` need to resolve that
# id. Every invocation of this script runs in its own fresh, --rm'd
# container, so that state must live somewhere that survives across
# separate containers -- point it at a repo-relative directory instead of
# the container's own (ephemeral) $HOME, since repo_root is bind-mounted
# 1:1 into every container this script starts. Without this, `start` in
# one --attach invocation and `stop`/`status <id>` in another can't see
# each other's state at all (hit this directly -- stop failed with
# "Could not find query with id ...").
nes_cli_state_dir="$repo_root/.nes-cli-state"
mkdir -p "$nes_cli_state_dir"

required_bins=("$repo_root/$cli_bin")
[[ "$attach" -eq 1 ]] || required_bins+=("$repo_root/$worker_bin")
for f in "${required_bins[@]}"; do
  [[ -x "$f" ]] || { echo "Error: not an executable file: $f (build it first)" >&2; exit 1; }
done

# Runs inside the container as PID 1.
#
# Without --attach: starts the worker in the background, waits for its
# gRPC port to accept connections, runs the requested nes-cli operation,
# then always kills the worker on exit (including Ctrl-C via the INT/TERM
# trap) so nothing is left running once this script returns. `nes-cli
# start` registers the query and returns immediately -- it does NOT block
# for the query's lifetime, so (unlike "dump", a one-shot check) the
# worker is kept alive afterward with `wait "$worker_pid"` so the query
# actually keeps streaming until you Ctrl-C; without this the trap fires
# the instant the operation returns and kills the worker before a single
# tuple is processed (hit this directly).
#
# With --attach: skips starting a worker entirely -- some other process
# (another terminal running this script with --none) owns the worker's
# lifetime. This invocation just waits for the port (already listening, so
# that's effectively instant) and runs one nes-cli operation against it,
# then exits. start/dump go through nes-cli-compose.sh (topology+query
# merge, same as always); stop/status call nes-cli directly since they
# take a queryId, not a query file.
inner_script='
set -uo pipefail
worker_bin="$1"; cli_bin="$2"; subcommand="$3"; port="$4"; attach="$5"; n_worker_args="$6"; shift 6
worker_extra=()
for ((i = 0; i < n_worker_args; i++)); do
  worker_extra+=("$1")
  shift
done

n_topo="$1"; shift
topo_files=()
for ((i = 0; i < n_topo; i++)); do
  topo_files+=("$1")
  shift
done

n_query="$1"; shift
query_files=()
for ((i = 0; i < n_query; i++)); do
  query_files+=("$1")
  shift
done
# "$@" is now exactly the trailing extra_args (nes-cli passthrough for
# start/dump, or queryId(s) for stop/status).

if [ "$attach" != "1" ]; then
  if [ "${#worker_extra[@]}" -gt 0 ]; then
    "$worker_bin" -- "${worker_extra[@]}" &
  else
    "$worker_bin" &
  fi
  worker_pid=$!
  cleanup() { kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; }
  trap cleanup EXIT INT TERM
fi

ready=0
for _ in $(seq 1 50); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.2
done
if [ "$ready" -ne 1 ]; then
  if [ "$attach" = "1" ]; then
    echo "Error: no worker listening on :$port -- start one first, e.g. in another terminal: nes-cli-compose-docker.sh --none" >&2
  else
    echo "Error: nes-single-node-worker did not start listening on :$port in time" >&2
  fi
  exit 1
fi

case "$subcommand" in
  none)
    echo "Worker up on :$port -- no topology/query registered, nes-cli was never invoked. Running in the foreground. Ctrl-C to stop." >&2
    wait "$worker_pid"
    ;;
  start|dump)
    compose_topo_args=()
    for f in "${topo_files[@]}"; do compose_topo_args+=(-t "$f"); done
    compose_query_args=()
    for f in "${query_files[@]}"; do compose_query_args+=(-q "$f"); done
    ./scripts/nes-cli-compose.sh -b "$cli_bin" -c "$subcommand" "${compose_topo_args[@]}" "${compose_query_args[@]}" "$@"
    if [ "$subcommand" = "start" ]; then
      if [ "$attach" = "1" ]; then
        echo "Query registered against the already-running worker on :$port." >&2
      else
        echo "Query registered -- worker running in the foreground. Ctrl-C to stop." >&2
        wait "$worker_pid"
      fi
    fi
    ;;
  stop|status)
    "$cli_bin" -t "${topo_files[0]}" "$subcommand" "$@"
    ;;
esac
'

# -t only when stdin is an actual terminal, so Ctrl-C reaches the container
# as SIGINT for interactive `start` runs, without breaking non-interactive
# invocations (e.g. `dump` piped into `tail`, no TTY to allocate).
docker_tty_flag=()
[[ -t 0 ]] && docker_tty_flag=(-t)

# --network host: the worker needs to reach services bound to the host's own
# localhost (e.g. a local MQTT broker for a webapp demo), and (with
# --attach) this container needs to reach a worker some OTHER container
# already bound to the host's localhost:$port -- inside the container's
# default network namespace, "localhost" means the container itself, not
# the host, so those connections fail otherwise. LAN targets like the
# factory broker are unaffected either way.
docker run --rm -i "${docker_tty_flag[@]}" --network host \
  -v "$repo_root:$repo_root" -w "$repo_root" \
  -e "XDG_STATE_HOME=$nes_cli_state_dir" \
  "$image" \
  bash -c "$inner_script" bash \
    "$worker_bin" "$cli_bin" "$subcommand" "$port" "$attach" \
    "${#worker_args[@]}" "${worker_args[@]}" \
    "${#topo_files[@]}" "${topo_files[@]}" \
    "${#query_files[@]}" "${query_files[@]}" \
    "${extra_args[@]}"
