#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pulls the nightly worker/cli images, retags them to the local names the demo
# compose file uses (it runs with pull_policy: never), restarts the stack and
# verifies the queries actually produce data. If they do not, the previous
# images are restored and the stack is restarted again, so a broken nightly
# never leaves the demo dead.
#
# Intended to run from cron on the Pi:
#   0 5 * * * /path/to/update-demo-images.sh >> /home/pi/nes-demo-update.log 2>&1

set -euo pipefail

# cron runs with a bare PATH; add the usual locations without dropping an
# inherited one (Homebrew, /snap/bin, ...) so the script also works interactively.
PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin${PATH:+:${PATH}}"
export PATH

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

UPSTREAM_WORKER=nebulastream/worker
UPSTREAM_CLI=nebulastream/nes-cli
LOCAL_WORKER=nes-worker
LOCAL_CLI=nes-cli
# container_name of the one-shot query registration service in the compose file.
CLI_CONTAINER=nes-cli
ROLLBACK_TAG=rollback
TELEMETRY_PORT=2222
# Only the queries the compose file starts; keep in sync with its `for q in ...` loop.
TOPICS=(water-usage bean-consumption machine-activity)
QUERY_COUNT=${#TOPICS[@]}
# The relay replays its last row to every new subscriber, so one message proves
# nothing -- a second one is the first that can only come from live telemetry.
MQTT_MESSAGES=2
MQTT_TIMEOUT=60
STATUS_RETRIES=30

COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
TAG=latest
FORCE=0
DRY_RUN=0
ROLLBACK_ENABLED=1
LOCK_FILE=/tmp/nes-demo-update.lock

usage() {
    echo "Usage: $0 [-f|--compose-file <path>] [-t|--tag <tag>] [--force] [--dry-run] [--no-rollback]"
    echo "Options:"
    echo "  -f, --compose-file   Compose file to restart (default: alongside this script)"
    echo "  -t, --tag            Upstream tag to pull (default: latest)"
    echo "      --force          Restart even if the pulled images are unchanged"
    echo "      --dry-run        Print what would happen, change nothing"
    echo "      --no-rollback    Keep the new images even if verification fails"
    echo "  -h, --help           Show this help"
    exit 1
}

log() {
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"
}

die() {
    log "ERROR: $*"
    exit 1
}

run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "DRY-RUN: $*"
        return 0
    fi
    "$@"
}

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

# Image ID of a local tag, empty string when the tag does not exist.
image_id() {
    docker image inspect --format '{{.Id}}' "$1" 2>/dev/null || true
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -f|--compose-file)
            [[ -n "${2:-}" && ! "$2" =~ ^- ]] || die "--compose-file requires a path"
            COMPOSE_FILE="$2"
            shift 2
            ;;
        -t|--tag)
            [[ -n "${2:-}" && ! "$2" =~ ^- ]] || die "--tag requires a value"
            TAG="$2"
            shift 2
            ;;
        --force) FORCE=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        --no-rollback) ROLLBACK_ENABLED=0; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Only one run at a time: a slow verification must never race the next night's run.
# flock is util-linux, so it exists on the Pi but not on macOS -- degrade quietly there.
if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCK_FILE"
    flock -n 9 || { log "another update is already running, skipping"; exit 0; }
fi

command -v docker >/dev/null 2>&1 || die "docker not found in PATH ($PATH)"
docker info >/dev/null 2>&1 || die "cannot talk to the docker daemon (is the user in the 'docker' group?)"
command -v jq >/dev/null 2>&1 || die "jq not found in PATH ($PATH)"
[ -f "$COMPOSE_FILE" ] || die "compose file not found: $COMPOSE_FILE"

log "using compose file $COMPOSE_FILE, upstream tag '$TAG'"

### The :rollback tags mean "last images that PASSED verification", so they are
### only moved forward at the very end of a successful run. Capturing whatever is
### deployed right now would be wrong: if a previous run died after retagging but
### before rolling back, that is a known-bad image and we would enshrine it as the
### fallback. Bootstrap them once when they do not exist yet.
OLD_WORKER_ID=$(image_id "${LOCAL_WORKER}:latest")
OLD_CLI_ID=$(image_id "${LOCAL_CLI}:latest")
if [ -z "$(image_id "${LOCAL_WORKER}:${ROLLBACK_TAG}")" ] && [ -n "$OLD_WORKER_ID" ]; then
    run docker tag "${LOCAL_WORKER}:latest" "${LOCAL_WORKER}:${ROLLBACK_TAG}"
    run docker tag "${LOCAL_CLI}:latest" "${LOCAL_CLI}:${ROLLBACK_TAG}"
    log "seeded the rollback point from the currently deployed images"
fi

ROLLBACK_WORKER_ID=$(image_id "${LOCAL_WORKER}:${ROLLBACK_TAG}")
ROLLBACK_CLI_ID=$(image_id "${LOCAL_CLI}:${ROLLBACK_TAG}")
CAN_ROLLBACK=0
if [ -n "$ROLLBACK_WORKER_ID" ] && [ -n "$ROLLBACK_CLI_ID" ]; then
    CAN_ROLLBACK=1
    log "rollback point: worker ${ROLLBACK_WORKER_ID:7:12}, cli ${ROLLBACK_CLI_ID:7:12}"
else
    log "no rollback point yet -- first run"
fi

### Pull before touching anything: a failed pull must leave the stack alone.
### With no local images this is also the bootstrap path -- the compose file runs
### pull_policy: never, so without this the stack could not start at all.
if [ -z "$OLD_WORKER_ID" ] || [ -z "$OLD_CLI_ID" ]; then
    log "no local ${LOCAL_WORKER}/${LOCAL_CLI} image found -- pulling ${UPSTREAM_WORKER}:${TAG} and ${UPSTREAM_CLI}:${TAG} to set the demo up"
else
    log "pulling ${UPSTREAM_WORKER}:${TAG} and ${UPSTREAM_CLI}:${TAG}"
fi
run docker pull "${UPSTREAM_WORKER}:${TAG}" || die "could not pull ${UPSTREAM_WORKER}:${TAG}"
run docker pull "${UPSTREAM_CLI}:${TAG}" || die "could not pull ${UPSTREAM_CLI}:${TAG}"

NEW_WORKER_ID=$(image_id "${UPSTREAM_WORKER}:${TAG}")
NEW_CLI_ID=$(image_id "${UPSTREAM_CLI}:${TAG}")

if [ "$FORCE" -eq 0 ] && [ "$NEW_WORKER_ID" = "$OLD_WORKER_ID" ] && [ "$NEW_CLI_ID" = "$OLD_CLI_ID" ]; then
    log "already up to date (no new images tonight), leaving the running stack alone"
    exit 0
fi

run docker tag "${UPSTREAM_WORKER}:${TAG}" "${LOCAL_WORKER}:latest"
run docker tag "${UPSTREAM_CLI}:${TAG}" "${LOCAL_CLI}:latest"
log "retagged to ${LOCAL_WORKER}:latest (${NEW_WORKER_ID:7:12}) and ${LOCAL_CLI}:latest (${NEW_CLI_ID:7:12})"

restart_stack() {
    run compose down --remove-orphans
    # `up --wait` exits non-zero as soon as a service is not left running, and
    # nes-cli is one-shot by design -- so its status says nothing about health.
    # verify() is the real gate; a worker that failed to start is caught there.
    if ! run compose up -d --wait; then
        log "compose up --wait reported a non-running service (expected: nes-cli is one-shot)"
    fi
    # ... but only nes-cli may be down. A broker that never came up (a stray
    # mosquitto already holding :1883 is the usual reason) otherwise surfaces much
    # later as the sink's "Failed to connect to MQTT broker mqtt://mosquitto:1883".
    if [ "$DRY_RUN" -eq 1 ]; then
        return 0
    fi
    local svc
    for svc in mosquitto nes-worker; do
        if [ -z "$(compose ps -q --status running "$svc")" ]; then
            log "WARNING: '${svc}' is not running after the restart"
            compose logs --tail 20 "$svc" || true
        fi
    done
}

# Is the telemetry feed alive? Used to tell "the new image is broken" apart from
# "the coffee machine is unplugged" -- rolling back for the latter is pointless.
# The relay listens on the host, which is what the worker reaches via host-gateway.
telemetry_reachable() {
    # `timeout` is coreutils: present on the Pi, absent on macOS. A refused
    # connection on loopback returns immediately, so going without it is safe.
    if command -v timeout >/dev/null 2>&1; then
        timeout 3 bash -c "echo > /dev/tcp/127.0.0.1/${TELEMETRY_PORT}" 2>/dev/null
    else
        (echo > "/dev/tcp/127.0.0.1/${TELEMETRY_PORT}") 2>/dev/null
    fi
}

# The compose nes-cli container is one-shot and keeps no XDG state volume, so the
# query ids it printed are gone. The no-id form reports the worker's view instead.
# `run` rather than `exec`: an exec session dies when its stdin hits EOF, which is
# exactly what happens under cron (docker/compose#10418).
query_states() {
    compose run --rm -T --entrypoint nes-cli nes-cli \
        -t "/queries/${TOPICS[0]}.yaml" status 2>/dev/null
}

# One-off subscriber on the demo network; `run` does not publish the service's
# ports, so this cannot collide with the running broker.
subscribe() {
    compose run --rm -T --entrypoint mosquitto_sub mosquitto \
        -h mosquitto -p 1883 -t "$1" -C "$MQTT_MESSAGES" -W "$MQTT_TIMEOUT"
}

verify() {
    # Status as well as exit code: a container that was created but never started
    # (because the worker died first) also reports ExitCode 0, which would read as
    # a successful registration.
    local cli_state
    cli_state=$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' "$CLI_CONTAINER" 2>/dev/null || echo "missing:-")
    if [ "$cli_state" != "exited:0" ]; then
        log "query registration failed: nes-cli is '${cli_state}' (want exited:0)"
        compose logs --tail 20 nes-cli || true
        return 1
    fi
    log "queries registered (nes-cli exited 0)"

    local i states running
    for i in $(seq 1 "$STATUS_RETRIES"); do
        states=$(query_states | jq -r '.[].query_status' 2>/dev/null || true)
        running=$(echo "$states" | grep -c '^Running$' || true)
        if [ "$running" -ge "$QUERY_COUNT" ]; then
            log "all ${QUERY_COUNT} queries are Running"
            break
        fi
        if [ "$i" -eq "$STATUS_RETRIES" ]; then
            log "only ${running}/${QUERY_COUNT} queries reached Running; states: $(echo "$states" | tr '\n' ' ')"
            return 1
        fi
        sleep 2
    done

    if ! telemetry_reachable; then
        log "WARNING: telemetry on host.docker.internal:2222 is unreachable -- skipping the data check."
        log "         Queries are Running, so the images are accepted; fix the relay to get data flowing."
        return 0
    fi

    local topic
    for topic in "${TOPICS[@]}"; do
        if subscribe "$topic" >/dev/null 2>&1; then
            log "topic '${topic}' is publishing"
        else
            log "no fresh data on topic '${topic}' within ${MQTT_TIMEOUT}s"
            return 1
        fi
    done
    return 0
}

rollback() {
    local reason="$1"
    if [ "$ROLLBACK_ENABLED" -eq 0 ]; then
        log "ROLLBACK DISABLED: keeping the new images despite: ${reason}"
        exit 1
    fi
    if [ "$CAN_ROLLBACK" -eq 0 ]; then
        log "CANNOT ROLL BACK (no previous images): ${reason}"
        exit 1
    fi
    log "rolling back to the previous images because: ${reason}"
    run docker tag "${LOCAL_WORKER}:${ROLLBACK_TAG}" "${LOCAL_WORKER}:latest"
    run docker tag "${LOCAL_CLI}:${ROLLBACK_TAG}" "${LOCAL_CLI}:latest"
    restart_stack
    if verify; then
        log "ROLLED BACK: ${reason} -- previous images restored, demo is running again"
    else
        log "ROLLED BACK: ${reason} -- previous images restored, but the demo is STILL not healthy"
    fi
    exit 1
}

restart_stack

if [ "$DRY_RUN" -eq 1 ]; then
    log "DRY-RUN: would now verify the queries and roll back on failure"
    exit 0
fi

if verify; then
    # These images are now the last known good ones.
    docker tag "${LOCAL_WORKER}:latest" "${LOCAL_WORKER}:${ROLLBACK_TAG}"
    docker tag "${LOCAL_CLI}:latest" "${LOCAL_CLI}:${ROLLBACK_TAG}"
    log "updated: worker ${NEW_WORKER_ID:7:12}, cli ${NEW_CLI_ID:7:12}"
    exit 0
fi

rollback "verification failed after updating the images"
