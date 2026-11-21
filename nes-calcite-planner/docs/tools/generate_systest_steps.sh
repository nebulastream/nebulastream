#!/usr/bin/env bash
# Shows the planner's steps for every systest query: runs the systests with --plan-only (they plan the queries but run none,
# about half a minute) and $NES_CALCITE_DUMP_REQUESTS (each query's request to the planner is saved), plans each saved
# request again with detailed steps, and writes one page per test file and an index. Needs the planner jar (mvn package).
#
# Usage: docs/tools/generate_systest_steps.sh <systest binary> <out directory> [java] [viz-standalone.js]
# With viz-standalone.js (from the npm package @viz-js/viz), the pages draw their graphs without loading it from a CDN.
# The planned steps stay in <out directory>/steps.json; render_steps.py renders them again without a systest run.
# e.g.   docs/tools/generate_systest_steps.sh ../cmake-build-release-libstdcxx-none/nes-systests/systest/systest /tmp/steps
set -euo pipefail
cd "$(dirname "$0")/../.."
systest=$(realpath "$1")
out=$(realpath -m "$2")
java=${3:-java}
viz=()
[ -n "${4:-}" ] && viz=(--viz "$(realpath "$4")")
tests=$(realpath ../nes-systests)
work=$(mktemp -d)
mkdir -p "$work/requests" "$work/run"

# One directory at a time: with several, systest runs only the last one. benchmark/ has the queries of benchmark_small/
# over large inputs.
for directory in "$tests"/*/; do
  [ "$(basename "$directory")" = benchmark ] && continue
  [ -n "$(find "$directory" -name '*.test' -print -quit)" ] || continue
  (cd "$(dirname "$systest")/.." && NES_CALCITE_DUMP_REQUESTS="$work/requests" "$systest" --plan-only -t "$directory" --data "$tests/testdata" \
    --workingDir "$work/run" > "$work/$(basename "$directory").log" 2>&1) || true
done
echo "$(ls "$work/requests" | wc -l) requests saved"

mkdir -p "$out"
"$java" -cp target/nes-calcite-planner.jar docs/tools/DumpSteps.java "$out/steps.json" --requests "$work/requests"
python3 docs/tools/render_steps.py "${viz[@]}" --systest "$out/steps.json" "$tests" "$out"
rm -rf "$work"
echo "written: $out/index.html and $out/all.html (everything in one page)"
