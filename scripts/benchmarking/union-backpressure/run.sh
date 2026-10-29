#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs every union_*.test next to this script against every variant, interleaved, and appends one row per run to
# <out dir>/results.tsv: scenario, threads, variant, repetition, seconds (FAIL if the query failed or was killed).
#
# Usage: run.sh <test data dir> <out dir> <label>=<systest binary>[@<max watermark gap in ms>] ...
# The test data dir must contain large/nexmark/bid_653M.csv and large/nexmark/auction_modified_74M.csv (see README.md).
# Environment: THREADS (default "16 23"), REPS (default 3), SCENARIOS (default all), TOTAL_MEMORY in bytes (default 32 GiB),
#              WRAP (command prefix, e.g. "numactl --cpunodebind=0 --membind=0").

set -uo pipefail

TESTS=$(realpath "$(dirname "$0")")
DATA=$(realpath "$1")
WORK=$(realpath -m "$2")
shift 2
THREADS=${THREADS:-"16 23"}
REPS=${REPS:-3}
SCENARIOS=${SCENARIOS:-$(cd "$TESTS" && ls union_*.test | sed 's|\.test$||' | sort -V)}
TOTAL_MEMORY=${TOTAL_MEMORY:-34359738368}
read -ra WRAP <<< "${WRAP:-}"

mkdir -p "$WORK/runs"
for rep in $(seq "$REPS"); do
    for scenario in $SCENARIOS; do
        for threads in $THREADS; do
            for variant in "$@"; do
                label=${variant%%=*}
                spec=${variant#*=}
                binary=${spec%@*}
                gap=()
                [[ $spec == *@* ]] && gap=(--worker.max_watermark_gap_in_ms="${spec##*@}")
                dir=$WORK/runs/$label-$scenario-$threads-$rep
                rm -rf "$dir"
                "${WRAP[@]}" "$binary" -b -t "$TESTS/$scenario.test" --data "$DATA" --workingDir="$dir" --sequential \
                    --query-timeout 0 -- \
                    --worker.default_query_execution.execution_mode=COMPILER \
                    --worker.query_engine.number_of_worker_threads="$threads" \
                    --worker.total_memory_in_bytes="$TOTAL_MEMORY" "${gap[@]}" > "$dir.log" 2>&1
                seconds=FAIL
                if grep -q "1 passed" "$dir.log"; then
                    seconds=$(grep -o '"time":[0-9.]*' "$dir/BenchmarkResults.json" | cut -d: -f2)
                fi
                printf "%s\t%s\t%s\t%s\t%s\n" "$scenario" "$threads" "$label" "$rep" "$seconds" | tee -a "$WORK/results.tsv"
            done
        done
    done
done
python3 "$TESTS/summarize.py" "$WORK/results.tsv"
