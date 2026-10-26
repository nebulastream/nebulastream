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

# Records OS scheduling events next to an event trace, so the latency report can show when threads ran, waited for a CPU or slept.
#
#   sudo scripts/latency_sched.sh <out-file> <pid> [seconds]
#
# Records the process <pid> and everything it starts after <out-file>.ready exists, e.g. the shell that then launches the workers; the whole system
# produces hundreds of thousands of events per second. Runs for the given time, or until it is terminated. Put <out-file> next to the trace's *.bin files as sched.txt, and
# scripts/latency_trace.py picks it up. Needs root and the kernel's tracefs (in Docker: a --privileged container). Uses the
# kernel's `mono` trace clock, which is the clock of the trace's steady-clock timestamps.
set -euo pipefail

out=$1
pid=$2
seconds=${3:-}
tracing=/sys/kernel/tracing
[ -d "$tracing/events" ] || mount -t tracefs nodev "$tracing"
events=("$tracing/events/sched/sched_switch/enable" "$tracing/events/sched/sched_wakeup/enable")

stop() {
  echo 0 > "$tracing/tracing_on"
  for event in "${events[@]}"; do echo 0 > "$event"; done
  echo > "$tracing/set_event_pid"
  echo 0 > "$tracing/options/event-fork"
  kill ${reader:-} ${sleeper:-} 2> /dev/null || true
  wait ${reader:-} 2> /dev/null || true
  if [ -n "${buffer:-}" ]; then mv "$buffer" "$out"; fi
}
trap stop EXIT
trap 'exit 0' TERM INT

echo 0 > "$tracing/tracing_on"
echo mono > "$tracing/trace_clock"
echo 65536 > "$tracing/buffer_size_kb"
echo > "$tracing/trace"
echo 1 > "$tracing/options/event-fork"
echo "$pid" > "$tracing/set_event_pid"
for event in "${events[@]}"; do echo 1 > "$event"; done
# Reading trace_pipe into a slow file system, e.g. a Docker volume, cuts off records, so it goes to a local file first.
buffer=$(mktemp)
cat "$tracing/trace_pipe" > "$buffer" &
reader=$!
echo 1 > "$tracing/tracing_on"
# Processes that <pid> starts from now on are recorded; callers wait for this file before starting them.
touch "$out.ready"

sleep "${seconds:-infinity}" &
sleeper=$!
wait $sleeper
