#!/usr/bin/env bash
# keystrokes.sh — captures keystrokes, buffers them, and serves buffered
# keystrokes to clients on TCP port 9000. Adds a heartbeat line every 5 s
# so downstream consumers never go idle.

HOST=0.0.0.0
PORT=9000

# ──────────────────────────── setup ──────────────────────────────────────
BUFFER_FILE=$(mktemp /tmp/keystrokes.XXXXXX)
trap 'stty sane; rm -f "$BUFFER_FILE"' EXIT

TTY=$(tty)
exec 3<"$TTY"          # non-blocking read FD for raw keystrokes

# Raw mode: get every key instantly, suppress terminal echo
stty raw -echo

# ───────────────── capture real keystrokes ───────────────────────────────
{
  while IFS= read -r -n1 key <&3; do
    ts=$(date +%s)
    printf '%s,%s\n' "key" "$ts" >>"$BUFFER_FILE"
    printf '\r%s,%s\n' "key" "$ts"
  done
} &

# ───────────────── heartbeat every 5 s ───────────────────────────────────
{
  while true; do
    sleep 5
    printf '_,%s\n' "$(date +%s)" >>"$BUFFER_FILE"
  done
} &

# ───────────────── TCP server (persistent) ───────────────────────────────
# -l  listen        ‖ -k  keep listening after client disconnects
while true; do
  nc -l -k -p "$PORT" < <(
    # send historical buffer first …
    cat "$BUFFER_FILE"
    # … then follow new lines in real time
    tail -n0 -f "$BUFFER_FILE"
  )
done