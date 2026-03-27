#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# Replicate a YSB CSV data file N times, advancing the timestamp (current_ms,
# column 6) so that it increases monotonically across all copies.
#
# The output file is renamed to include the total number of tuples, e.g.:
#   ysb_10k_data_10000000.csv   (for 5M rows * 2)
#
# Usage:
#   ./replicate_ysb_data.sh <input.csv> <N>
#
# Example:
#   ./replicate_ysb_data.sh nes-systests/testdata/large/ysb/ysb_10k_data_479M.csv 4
# =============================================================================

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <input.csv> <N>"
    echo "  Replicates the input CSV N times with advancing timestamps."
    exit 1
fi

INPUT="$1"
N="$2"

if [[ ! -f "$INPUT" ]]; then
    echo "ERROR: Input file not found: $INPUT"
    exit 1
fi

if [[ "$N" -lt 1 ]]; then
    echo "ERROR: N must be >= 1"
    exit 1
fi

ROWS=$(wc -l < "$INPUT")
TOTAL_ROWS=$((ROWS * N))
INPUT_DIR=$(dirname "$INPUT")
INPUT_BASE=$(basename "$INPUT" .csv)

# Strip any existing size suffix (e.g., _479M or _5000000) to get the base name
# Pattern: ysb_10k_data_XXX -> ysb_10k_data
BASE_NAME=$(echo "$INPUT_BASE" | sed -E 's/_[0-9]+[MmGgKk]?$//')

OUTPUT="${INPUT_DIR}/${BASE_NAME}_${TOTAL_ROWS}.csv"

echo "Input:        $INPUT"
echo "Rows/copy:    $ROWS"
echo "Copies:       $N"
echo "Total rows:   $TOTAL_ROWS"
echo "Output:       $OUTPUT"
echo ""

# Find the max timestamp (column 6) in the input to compute offsets
MAX_TS=$(awk -F, 'END {print $6}' "$INPUT")
echo "Max timestamp in input: $MAX_TS"
echo ""

# Build the output: for each copy i (0..N-1), stream through the input
# and add i*MAX_TS to column 6. This avoids buffering the entire file.
echo "Generating $OUTPUT ..."

> "$OUTPUT"
for (( i = 0; i < N; i++ )); do
    printf "  Copy %d/%d (offset: %d)...\n" "$((i + 1))" "$N" "$((i * MAX_TS))"
    awk -F, -v offset="$((i * MAX_TS))" '
    BEGIN { OFS = "," }
    {
        $6 = $6 + offset
        print
    }
    ' "$INPUT" >> "$OUTPUT"
done

ACTUAL_ROWS=$(wc -l < "$OUTPUT")
FILE_SIZE=$(du -h "$OUTPUT" | cut -f1)

echo ""
echo "Done."
echo "  Output:     $OUTPUT"
echo "  Rows:       $ACTUAL_ROWS"
echo "  Size:       $FILE_SIZE"

# Verify timestamp continuity
LAST_TS_COPY1=$(awk -F, "NR==$ROWS {print \$6}" "$OUTPUT")
FIRST_TS_COPY2=$(awk -F, "NR==$((ROWS + 1)) {print \$6}" "$OUTPUT")
if [[ "$N" -gt 1 ]]; then
    echo "  Last ts (copy 1):   $LAST_TS_COPY1"
    echo "  First ts (copy 2):  $FIRST_TS_COPY2"
    if [[ "$FIRST_TS_COPY2" -gt "$LAST_TS_COPY1" ]]; then
        echo "  Timestamp continuity: OK"
    else
        echo "  WARNING: Timestamp not strictly increasing across copies!"
    fi
fi
