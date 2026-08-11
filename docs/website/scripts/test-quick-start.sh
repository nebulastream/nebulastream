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

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
quick_start_doc="$script_dir/../content/reference/GetStarted.md"

work_dir="$(mktemp -d)"
compose_file="$work_dir/compose.yaml"
commands_dir="$work_dir/commands"

cleanup() {
  docker rm -f worker >/dev/null 2>&1 || true
  if [[ -f "$compose_file" ]]; then
    (cd "$work_dir" && docker compose down --volumes --remove-orphans) >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

mkdir -p "$work_dir/output" "$commands_dir"

extract_code_block() {
  local name="$1"
  local language="$2"
  local output="$3"
  local start_marker="<!-- quick-start-${name}:start -->"
  local end_marker="<!-- quick-start-${name}:end -->"
  local opening_fence="\`\`\`$language"

  if ! awk -v start_marker="$start_marker" -v end_marker="$end_marker" -v opening_fence="$opening_fence" '
    $0 == start_marker { capture = 1; next }
    capture && $0 == opening_fence { in_fence = 1; next }
    in_fence && $0 == "```" { in_fence = 0; saw_fence = 1; next }
    in_fence && $0 == end_marker { exit 1 }
    capture && $0 == end_marker { complete = saw_fence; exit }
    in_fence { print }
    END { if (!complete) exit 1 }
  ' "$quick_start_doc" >"$output"; then
    echo "Could not extract the $name $language block from $quick_start_doc." >&2
    return 1
  fi

  if [[ ! -s "$output" ]]; then
    echo "The extracted $name $language block is empty." >&2
    return 1
  fi
}

run_documented_command() {
  local name="$1"
  local query_id="${2:-}"
  local command

  command="$(<"$commands_dir/$name.sh")"
  if [[ -n "$query_id" ]]; then
    command="${command//<query-id>/$query_id}"
  fi
  (cd "$work_dir" && bash -Eeuo pipefail -c "$command")
}

retry_documented_command() {
  local name="$1"

  for _ in {1..30}; do
    if run_documented_command "$name"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

extract_code_block topology yaml "$work_dir/topology.yaml"
extract_code_block compose yaml "$compose_file"
for command in \
  run-worker \
  submit-query \
  inspect-output \
  stop-worker \
  create-compose-topology \
  start-compose \
  list-compose \
  submit-compose-query \
  status-compose-query \
  stop-compose-query \
  status-stopped-compose-query \
  stop-compose; do
  extract_code_block "$command" bash "$commands_dir/$command.sh"
done

wait_for_output() {
  local results_file="$1"

  for _ in {1..30}; do
    if [[ -f "$results_file" ]] && grep -q '^498$' "$results_file"; then
      return 0
    fi
    sleep 1
  done

  echo "Quick Start did not produce the expected CSV output." >&2
  if [[ -f "$results_file" ]]; then
    cat "$results_file" >&2
  fi
  return 1
}

echo "Checking the Docker run flow..."
if docker container inspect worker >/dev/null 2>&1; then
  echo "A container named worker already exists; refusing to replace it." >&2
  exit 1
fi
run_documented_command run-worker
if ! retry_documented_command submit-query; then
  echo "Could not register the Docker run query." >&2
  exit 1
fi

wait_for_output "$work_dir/output/results.csv"
run_documented_command inspect-output
run_documented_command stop-worker || true

echo "Checking the Docker Compose flow..."
run_documented_command create-compose-topology
rm -f "$work_dir/output/results.csv"
run_documented_command start-compose
run_documented_command list-compose

if ! compose_query_output="$(retry_documented_command submit-compose-query)"; then
  echo "Could not register the Docker Compose query." >&2
  exit 1
fi
compose_query_id="$(printf '%s\n' "$compose_query_output" | tail -n 1 | tr -d '\r')"
if [[ ! "$compose_query_id" =~ ^[a-z0-9_]+$ ]]; then
  echo "The Docker Compose query returned an unexpected ID: $compose_query_id" >&2
  exit 1
fi
echo "$compose_query_id"

run_documented_command status-compose-query "$compose_query_id"
wait_for_output "$work_dir/output/results.csv"
run_documented_command stop-compose-query "$compose_query_id"
run_documented_command status-stopped-compose-query "$compose_query_id"
run_documented_command stop-compose

echo "Quick Start commands completed successfully."
