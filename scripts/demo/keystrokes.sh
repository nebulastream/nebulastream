#!/usr/bin/env bash
# keystrokes.sh — captures keystrokes, buffers them, and serves buffered keystrokes to clients on TCP port

HOST=0.0.0.0
PORT=9000

# Buffer file
BUFFER_FILE=$(mktemp /tmp/keystrokes.XXXXXX)
trap 'stty sane; rm -f "$BUFFER_FILE"' EXIT
TTY=$(tty)
exec 3<"$TTY"

# Configure terminal for raw mode
stty raw -echo

# Start capturing keystrokes in background and append to buffer
{
  # Print CSV header to buffer and terminal with carriage return
  while IFS= read -r -n1 key <&3; do
    ts=$(date +%s)
    # Format line without embedded newline
    line="${key},${ts}"
    # Append to buffer with newline
    printf '%s\n' "$line" >> "$BUFFER_FILE"
    # Echo to terminal at start of line
    printf '\r%s\n' "$line"
  done
} &

# Serve clients: for each connection, send buffer and then stream new keystrokes
while true; do
  nc -l -p "$PORT" < <(cat "$BUFFER_FILE"; tail -n0 -f "$BUFFER_FILE")
done
