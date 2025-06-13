#!/usr/bin/env bash
# keystrokes.sh — captures keystrokes, buffers them, and serves buffered keystrokes to clients on TCP port

HOST=0.0.0.0
PORT=5010

# Buffer file
BUFFER_FILE=$(mktemp /tmp/keystrokes.XXXXXX)
trap 'stty sane; rm -f "$BUFFER_FILE"' EXIT

# Configure terminal for raw mode
stty -echo -icanon time 0 min 0

# Start capturing keystrokes in background and append to buffer
{
  # Print CSV header to buffer
  printf 'key,timestamp\n' >> "$BUFFER_FILE"
  while IFS= read -r -n1 key; do
    ts=$(date +%s)
    printf '%q,%s\n' "$key" "$ts" >> "$BUFFER_FILE"
  done
} &

# Serve clients: for each connection, send buffer and then stream new keystrokes
while true; do
  nc -l -p "$PORT" < <(cat "$BUFFER_FILE"; tail -n0 -f "$BUFFER_FILE")
done
