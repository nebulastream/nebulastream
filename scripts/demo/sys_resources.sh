#!/usr/bin/env bash
# sys_resources.sh — collects system metrics (CPU, memory, network, temperature, disk)
# Usage:
#   1. Start a TCP listener on localhost port 9000: nc -lk 9000
#   2. Run this script: ./sys_resources.sh
# The script outputs CSV-formatted metrics every second to stdout, and if available, to the TCP socket.
HOST=0.0.0.0
PORT=${PORT:-9000}
# Buffer file for aggregated metrics
BUFFER_FILE=$(mktemp /tmp/sys_resources.XXXXXX)
trap 'rm -f "$BUFFER_FILE"' EXIT

# Background collection of system metrics into buffer
(
  # CSV header
  printf 'timestamp,category,target,field,value\n'
  while true; do
    ts=$(date +%s)
    # --- CPU Metrics ---
    read -r _ user nice system idle iowait irq softirq steal guest guest_nice < <(grep '^cpu ' /proc/stat)
    printf "%s,cpu,user,%s\n" "$ts" "$user"
    printf "%s,cpu,nice,%s\n" "$ts" "$nice"
    printf "%s,cpu,system,%s\n" "$ts" "$system"
    printf "%s,cpu,idle,%s\n" "$ts" "$idle"
    printf "%s,cpu,iowait,%s\n" "$ts" "$iowait"
    printf "%s,cpu,irq,%s\n" "$ts" "$irq"
    printf "%s,cpu,softirq,%s\n" "$ts" "$softirq"
    printf "%s,cpu,steal,%s\n" "$ts" "$steal"
    printf "%s,cpu,guest,%s\n" "$ts" "$guest"
    printf "%s,cpu,guest_nice,%s\n" "$ts" "$guest_nice"

    # --- Memory Metrics ---
    grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapTotal|SwapFree):' /proc/meminfo \
      | awk -v ts="$ts" '{gsub(/:/,"",$1); printf "%s,memory,%s,%s\n", ts, $1, $2}'

    # --- Network Metrics ---
    for iface_path in /sys/class/net/*; do
      iface=$(basename "$iface_path")
      [ "$iface" = "lo" ] && continue
      for stat in rx_bytes tx_bytes rx_packets tx_packets rx_errors tx_errors rx_dropped tx_dropped; do
        value=$(cat "/sys/class/net/$iface/statistics/$stat")
        printf "%s,network,%s,%s,%s\n" "$ts" "$iface" "$stat" "$value"
      done
    done

    # --- Temperature Metrics ---
    temp_raw=""
    for zone in /sys/class/thermal/thermal_zone*; do
      if [ -f "$zone/temp" ] && [ -f "$zone/type" ]; then
        tz_type=$(cat "$zone/type")
        if echo "$tz_type" | grep -iq "cpu"; then
          temp_raw=$(cat "$zone/temp")
          break
        fi
      fi
    done
    if [ -z "$temp_raw" ]; then
      for hw in /sys/class/hwmon/hwmon*; do
        if [ -f "$hw/temp1_input" ]; then
          temp_raw=$(cat "$hw/temp1_input")
          break
        fi
      done
    fi
    if [[ "$temp_raw" =~ ^[0-9]+$ ]]; then
      temp_c=$(awk "BEGIN {printf \"%.1f\", $temp_raw/1000}")
      printf "%s,temperature,cpu,%s\n" "$ts" "$temp_c"
    else
      printf "%s,temperature,cpu,NA\n" "$ts"
    fi

    # --- Disk Metrics ---
    awk -v ts="$ts" '$3 ~ /^[a-z]/ { printf "%s,disk,%s,reads_completed,%s\n", ts, $3, $4;
                                                 printf "%s,disk,%s,reads_merged,%s\n",    ts, $3, $5;
                                                 printf "%s,disk,%s,sectors_read,%s\n",     ts, $3, $6;
                                                 printf "%s,disk,%s,time_reading_ms,%s\n",    ts, $3, $7;
                                                 printf "%s,disk,%s,writes_completed,%s\n",  ts, $3, $8;
                                                 printf "%s,disk,%s,writes_merged,%s\n",     ts, $3, $9;
                                                 printf "%s,disk,%s,sectors_written,%s\n",   ts, $3, $10;
                                                 printf "%s,disk,%s,time_writing_ms,%s\n",    ts, $3, $11; }' /proc/diskstats
    sleep 1
  done
) >> "$BUFFER_FILE" &

# Serve buffered metrics to any TCP client
while true; do
  nc -l -p "$PORT" < <(cat "$BUFFER_FILE"; tail -n0 -f "$BUFFER_FILE")
done
