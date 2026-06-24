#!/usr/bin/env bash
# TMP DIAGNOSTIC (REVERT before merge): per-thread CPU sampler. Launches the given command and every
# ~50ms appends "elapsed_ms tid cpu_ticks comm" for every thread to PROF_OUT (default /tmp/prof.txt).
# Captures ALL threads (worker pool + source/io), unlike the event trace (worker tasks only). Analyze the
# per-thread CPU slope over the steady execution window to get util (compile/idle phases are excluded by
# looking at the window where IOThread/WorkerThread CPU is rising).
set -u
OUT="${PROF_OUT:-/tmp/prof.txt}"
: > "$OUT"
"$@" & PID=$!
START=$(date +%s.%N)
while kill -0 "$PID" 2>/dev/null; do
    now=$(date +%s.%N)
    ms=$(awk "BEGIN{printf \"%d\", ($now-$START)*1000}")
    for t in /proc/"$PID"/task/*; do
        tid=$(basename "$t" 2>/dev/null) || continue
        s=$(cat "$t/stat" 2>/dev/null) || continue
        c=$(cat "$t/comm" 2>/dev/null) || continue
        rest="${s#*) }"
        # shellcheck disable=SC2086
        set -- $rest
        echo "$ms $tid $(( ${12:-0} + ${13:-0} )) $c" >> "$OUT"
    done
    sleep 0.05
done
echo "PROF_DONE $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N)-$START}")s -> $OUT"
