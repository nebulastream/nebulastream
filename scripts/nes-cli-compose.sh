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
# Merges one or more topology yamls into one and runs it with one or more
# query files via nes-cli. logical/physical/sinks/workers/models arrays
# are concatenated across files; workers are deduped by host. A query
# file is one query by default; separate multiple queries in a file with
# a line containing only ';'.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: nes-cli-compose.sh -b path/to/nes-cli -t topo1.yaml [-t topo2.yaml ...] -q queries1.sql [-q queries2.sql ...]
                           [-c start|dump] [-- extra nes-cli args]

  -b PATH   path to the nes-cli binary (required)
  -t FILE   topology/source yaml file (repeatable, at least one required)
  -q FILE   query file (repeatable, at least one required); multiple
            queries in one file are separated by a line containing only ';'
  -c CMD    nes-cli subcommand to run (default: start)
  --        remaining args are passed through to nes-cli verbatim
EOF
}

nes_cli_bin=""
topologies=()
query_files=()
subcommand="start"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -b) nes_cli_bin="$2"; shift 2 ;;
    -t) topologies+=("$2"); shift 2 ;;
    -q) query_files+=("$2"); shift 2 ;;
    -c) subcommand="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done
extra_args=("$@")

[[ -n "$nes_cli_bin" ]] || { echo "Error: -b <path to nes-cli> is required, no default" >&2; exit 1; }
[[ ${#topologies[@]} -gt 0 ]] || { echo "Error: at least one -t <topology.yaml> is required" >&2; exit 1; }
[[ ${#query_files[@]} -gt 0 ]] || { echo "Error: at least one -q <query file> is required" >&2; exit 1; }
for f in "${topologies[@]}" "${query_files[@]}"; do
  [[ -f "$f" ]] || { echo "Error: file not found: $f" >&2; exit 1; }
done

merged="$(mktemp --suffix=.yaml)"
trap 'rm -f "$merged"' EXIT

# Body of a bare top-level "key:" line (column 0, nothing after the colon)
# up to the next such line or EOF. Nested "key:" lines are indented, so
# they don't trip the boundary check.
extract_section() {
  awk -v key="$2" '
    /^[A-Za-z_][A-Za-z0-9_]*:[[:space:]]*$/ { insec = ($0 ~ "^" key ":[[:space:]]*$"); next }
    insec { print }
  ' "$1"
}

# Splits a workers: body into "  - ..." blocks, drops blocks whose host:
# repeats an earlier one.
dedupe_workers() {
  local -A seen=()
  local block="" host="" line dropped=0
  flush() {
    [[ -z "$block" ]] && return
    if [[ -z "$host" || -z "${seen[$host]+x}" ]]; then
      [[ -n "$host" ]] && seen[$host]=1
      printf '%s' "$block"
    else
      dropped=$((dropped + 1))
    fi
  }
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^\ \ -\  ]]; then
      flush
      block="$line"$'\n'
      host=""
    else
      block+="$line"$'\n'
    fi
    [[ -z "$host" && "$line" =~ ^[[:space:]]*-?[[:space:]]*host:[[:space:]]*(.*)$ ]] && host="${BASH_REMATCH[1]}"
  done
  flush
  [[ $dropped -gt 0 ]] && echo "Note: dropped $dropped duplicate worker entries (deduped by host)" >&2
  return 0
}

{
  for key in logical physical sinks workers models; do
    body=""
    for f in "${topologies[@]}"; do
      body+="$(extract_section "$f" "$key")"$'\n'
    done
    [[ -n "${body//[[:space:]]/}" ]] || continue
    echo "$key:"
    if [[ "$key" == workers ]]; then
      printf '%s' "$body" | dedupe_workers
    else
      printf '%s\n' "$body"
    fi
  done
} > "$merged"

# Splits a file into queries on lines containing only ';'; a file with no
# such line becomes a single query.
queries=()
split_queries() {
  local buf="" line
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^[[:space:]]*\;[[:space:]]*$ ]]; then
      [[ -n "${buf//[[:space:]]/}" ]] && queries+=("$buf")
      buf=""
    else
      buf+="$line"$'\n'
    fi
  done < "$1"
  [[ -n "${buf//[[:space:]]/}" ]] && queries+=("$buf")
  return 0
}
for f in "${query_files[@]}"; do
  split_queries "$f"
done
[[ ${#queries[@]} -gt 0 ]] || { echo "Error: no queries found in the given query files" >&2; exit 1; }

"$nes_cli_bin" -t "$merged" "$subcommand" "${queries[@]}" "${extra_args[@]}"
