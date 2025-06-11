#!/usr/bin/env bash
# sys_resources.sh — collects system metrics (CPU, memory, network, temperature, disk)
# Usage:
#   1. Start a TCP listener on localhost port 9000: nc -lk 9000
#   2. Run this script: ./sys_resources.sh
# The script outputs CSV-formatted metrics every second to stdout, and if available, to the TCP socket.
HOST=127.0.0.1
PORT=${PORT:-9000}
# Check if TCP port is reachable
if nc -z -w1 $HOST $PORT 2>/dev/null; then
  TCP_OK=true
else
  TCP_OK=false
  echo "Warning: TCP $HOST:$PORT not reachable, switching to offline mode." >&2
fi

while true; do
  # Generate timestamp for each iteration
  ts=$(date +%s)
  {
    # --- CPU Metrics ---
    # Columns: timestamp,category,field,value
    read -r _ user nice system idle iowait irq softirq steal guest guest_nice < <(grep '^cpu ' /proc/stat)
    echo "$ts,cpu,user,$user"
    echo "$ts,cpu,nice,$nice"
    echo "$ts,cpu,system,$system"
    echo "$ts,cpu,idle,$idle"
    echo "$ts,cpu,iowait,$iowait"
    echo "$ts,cpu,irq,$irq"
    echo "$ts,cpu,softirq,$softirq"
    echo "$ts,cpu,steal,$steal"
    echo "$ts,cpu,guest,$guest"
    echo "$ts,cpu,guest_nice,$guest_nice"

    # --- Memory Metrics ---
    # Columns: timestamp,category,field,value
    grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapTotal|SwapFree):' /proc/meminfo \
      | awk -v ts="$ts" '{gsub(/:/,"",$1); print ts",memory,"$1","$2}'

    # --- Network Metrics ---
    # Columns: timestamp,category,iface,field,value
    for iface_path in /sys/class/net/*; do
      iface=$(basename "$iface_path")
      [ "$iface" = "lo" ] && continue
      for stat in rx_bytes tx_bytes rx_packets tx_packets rx_errors tx_errors rx_dropped tx_dropped; do
        value=$(cat "/sys/class/net/$iface/statistics/$stat")
        echo "$ts,network,$iface,$stat,$value"
      done
    done


    # --- Temperature Metrics ---
    # Columns: timestamp,category,field,value
    temp_raw=""
    # Search for CPU-related thermal zone
    for zone in /sys/class/thermal/thermal_zone*; do
      if [ -f "$zone/temp" ] && [ -f "$zone/type" ]; then
        tz_type=$(cat "$zone/type")
        if echo "$tz_type" | grep -iq "cpu"; then
          temp_raw=$(cat "$zone/temp")
          break
        fi
      fi
    done
    # Fallback to hwmon if no CPU thermal zone found
    if [ -z "$temp_raw" ]; then
      for hw in /sys/class/hwmon/hwmon*; do
        if [ -f "$hw/temp1_input" ]; then
          temp_raw=$(cat "$hw/temp1_input")
          break
        fi
      done
    fi
    # Output in °C with one decimal or NA on error
    if [[ "$temp_raw" =~ ^[0-9]+$ ]]; then
      temp_c=$(LC_NUMERIC=C awk "BEGIN {printf \"%.1f\", $temp_raw/1000}")
      echo "$ts,temperature,cpu,$temp_c"
    else
      echo "$ts,temperature,cpu,NA"
    fi

    # --- Disk Metrics ---
    # Columns: timestamp,category,device,field,value
    awk -v ts="$ts" '$3 ~ /^[a-z]/ {
      printf "%s,disk,%s,reads_completed,%s\n",ts,$3,$4
      printf "%s,disk,%s,reads_merged,%s\n",ts,$3,$5
      printf "%s,disk,%s,sectors_read,%s\n",ts,$3,$6
      printf "%s,disk,%s,time_reading_ms,%s\n",ts,$3,$7
      printf "%s,disk,%s,writes_completed,%s\n",ts,$3,$8
      printf "%s,disk,%s,writes_merged,%s\n",ts,$3,$9
      printf "%s,disk,%s,sectors_written,%s\n",ts,$3,$10
      printf "%s,disk,%s,time_writing_ms,%s\n",ts,$3,$11
    }' /proc/diskstats
  # End of metrics block
  } | ( if [ "$TCP_OK" = true ]; then tee >(nc -q0 $HOST $PORT); else cat; fi )
  sleep 1
done
