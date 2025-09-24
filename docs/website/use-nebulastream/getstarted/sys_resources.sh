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

# sys_resources.sh — collects system metrics (CPU, memory) and serves them on different ports with different schemas
# Usage:
#   1. Run this script: ./sys_resources.sh
# The script outputs CSV-formatted metrics every second to stdout and serves them on TCP sockets.

CPU_PORT=${CPU_PORT:-9000}
MEM_PORT=${MEM_PORT:-9001}

# Buffer files for aggregated metrics
CPU_BUFFER_FILE=$(mktemp /tmp/cpu_resources.XXXXXX)
MEM_BUFFER_FILE=$(mktemp /tmp/mem_resources.XXXXXX)

# Function to clean up background processes and temporary files
cleanup() {
    echo "Cleaning up..."
    # Kill background processes
    pkill -P $$
    # Remove temporary files
    rm -f "$CPU_BUFFER_FILE" "$MEM_BUFFER_FILE"
    exit
}

# Trap signals to ensure cleanup is called
trap cleanup EXIT INT

# Initialize previous values
prev_user=0
prev_nice=0
prev_system=0
prev_idle=0
prev_iowait=0
prev_irq=0
prev_softirq=0
prev_steal=0
prev_guest=0
prev_guest_nice=0

# Background collection of CPU metrics into CPU buffer
(
  while true; do
    ts=$(($(date +%s)*1000))
    # --- CPU Metrics ---
    read -r _ user nice system idle iowait irq softirq steal guest guest_nice < <(grep '^cpu ' /proc/stat)
    # Calculate differences
    user_diff=$((user - prev_user))
    nice_diff=$((nice - prev_nice))
    system_diff=$((system - prev_system))
    idle_diff=$((idle - prev_idle))
    iowait_diff=$((iowait - prev_iowait))
    irq_diff=$((irq - prev_irq))
    softirq_diff=$((softirq - prev_softirq))
    steal_diff=$((steal - prev_steal))
    guest_diff=$((guest - prev_guest))
    guest_nice_diff=$((guest_nice - prev_guest_nice))
    # Update previous values
    prev_user=$user
    prev_nice=$nice
    prev_system=$system
    prev_idle=$idle
    prev_iowait=$iowait
    prev_irq=$irq
    prev_softirq=$softirq
    prev_steal=$steal
    prev_guest=$guest
    prev_guest_nice=$guest_nice
    # Print differences
    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" "$ts" "$user_diff" "$nice_diff" "$system_diff" "$idle_diff" "$iowait_diff" "$irq_diff" "$softirq_diff" "$steal_diff" "$guest_diff" "$guest_nice_diff" >> "$CPU_BUFFER_FILE"
    sleep 1
  done
) &

# Background collection of memory metrics into memory buffer
(
  while true; do
    ts=$(($(date +%s)*1000))
    # --- Memory Metrics ---
    # Memory Schema: timestamp, MemTotal, MemFree, MemAvailable, Buffers, Cached, SwapTotal, SwapFree
    grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapTotal|SwapFree):' /proc/meminfo | \
      awk -v ts="$ts" 'BEGIN { mem_total=0; mem_free=0; mem_available=0; buffers=0; cached=0; swap_total=0; swap_free=0 }
      {
        gsub(/:/,"",$1);
        if ($1 == "MemTotal") mem_total=$2;
        else if ($1 == "MemFree") mem_free=$2;
        else if ($1 == "MemAvailable") mem_available=$2;
        else if ($1 == "Buffers") buffers=$2;
        else if ($1 == "Cached") cached=$2;
        else if ($1 == "SwapTotal") swap_total=$2;
        else if ($1 == "SwapFree") swap_free=$2;
      }
      END { printf "%s,%s,%s,%s,%s,%s,%s,%s\n", ts, mem_total, mem_free, mem_available, buffers, cached, swap_total, swap_free }' >> "$MEM_BUFFER_FILE"
    sleep 1
  done
) &

# Serve CPU metrics on port 9000
(
  while true; do
    nc -l -k -p "$CPU_PORT" < <(cat "$CPU_BUFFER_FILE"; tail -n0 -f "$CPU_BUFFER_FILE")
  done
) &

# Serve memory metrics on port 9001
(
  while true; do
    nc -l -k -p "$MEM_PORT" < <(cat "$MEM_BUFFER_FILE"; tail -n0 -f "$MEM_BUFFER_FILE")
  done
) &

# Wait for background processes to finish
wait
