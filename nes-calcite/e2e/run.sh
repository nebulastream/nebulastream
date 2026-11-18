#!/usr/bin/env bash
# Runs the ANTLR/Calcite query pairs in queries.sql on the embedded REPL and checks that each pair writes the same rows.
# Usage: run.sh <nes-repl-embedded> <nes-calcite-planner.jar> [java]
set -euo pipefail
repl=$(realpath "$1")
jar=$(realpath "$2")
java=${3:-java}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
port=$((50000 + RANDOM % 10000))

"$java" -jar "$jar" "$port" > "$work/planner.log" 2>&1 &
planner=$!
trap 'kill $planner 2>/dev/null || true' EXIT
for _ in $(seq 100); do grep -q listening "$work/planner.log" && break; sleep 0.2; done

cd "$work"
echo "working directory: $work"
if ! NES_CALCITE_PLANNER="localhost:$port" NES_CALCITE_FINITE_INPUTS=1 "$repl" -f JSON -e FAIL_FAST --on-exit WAIT_FOR_QUERY_TERMINATION \
    < "$here/queries.sql" > repl.log 2>&1; then
    tail -20 repl.log
    exit 1
fi

status=0
for antlr in *_antlr.csv; do
    name=${antlr%_antlr.csv}
    rows=$(wc -l < "$antlr")
    if [ "$rows" -le 1 ]; then
        echo "FAIL $name: no rows"
        status=1
    elif diff <(tail -n +2 "$antlr" | sort) <(tail -n +2 "${name}_calcite.csv" | sort) > "$name.diff"; then
        echo "ok   $name ($rows lines)"
    else
        echo "FAIL $name (see $work/$name.diff)"
        head -5 "$name.diff"
        status=1
    fi
done
exit $status
