#!/usr/bin/env bash
# keystrokes.sh — captures keystrokes and outputs CSV to stdout and TCP socket
# Usage:
#   1. Start a TCP listener on localhost port 9000:
#        nc -lk 9000
#   2. Run this script:
#        ./keystrokes.sh
# Outputs lines in the format: key,timestamp
# If TCP connection is successful, lines are sent to the socket as well.
# Goal: Send keystroke data to local TCP port
# Test with netcat: nc -lk 9000
HOST=127.0.0.1
PORT=9000

# Attempt TCP connection
if exec 3<>/dev/tcp/$HOST/$PORT 2>/dev/null; then
  TCP_OK=true
else
  TCP_OK=false
  echo "Warning: TCP connection to $HOST:$PORT failed, continuing in local-only mode." >&2
fi

# Set terminal to raw mode (no echo, immediate input)
stty -echo -icanon time 0 min 0

header="key,timestamp\n"
printf '%b' "$header"
if [ "$TCP_OK" = true ]; then
  printf '%b' "$header" >&3
fi
while IFS= read -r -n1 key; do
  ts=$(date +%s) # Generate timestamp in seconds
  # Generate CSV line
  line=$(printf '%q,%s\n' "$key" "$ts")
  if [ "$TCP_OK" = true ]; then
    # Send to TCP socket and stdout
    printf '%s\n' "$line" | tee /dev/stderr >&3
    printf '%s\n' "$line"
  else
    # Only local output
    printf '%s\n' "$line"
  fi
done
