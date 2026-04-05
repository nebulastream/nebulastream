#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end roundtrip test: Generator → ArrowFileSink → ArrowFileSource → CSV FileSink
# Verifies that data written as Arrow IPC can be read back and converted to CSV.

SQL_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/good"

setup_file() {
  if [ -z "$NES_REPL" ]; then
    echo "ERROR: NES_REPL environment variable must be set" >&2
    exit 1
  fi

  if [ ! -x "$NES_REPL" ]; then
    echo "ERROR: NES_REPL is not executable: $NES_REPL" >&2
    exit 1
  fi

  echo "# Using NES_REPL: $NES_REPL" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)
  cd "$TMP_DIR" || exit
}

teardown() {
  rm -rf "$TMP_DIR"
}

@test "Arrow roundtrip: Generator → ArrowFile → ArrowFileSource → CSV" {
  ARROW_FILE="$TMP_DIR/roundtrip.arrows"
  CSV_FILE="$TMP_DIR/roundtrip.csv"

  # ── Step 1: Produce the Arrow file ──────────────────────────────────
  sed "s|ARROW_FILE_PATH|$ARROW_FILE|g" "$SQL_DIR/arrow_roundtrip_produce.sql" \
    > "$TMP_DIR/produce.sql"

  echo "# === Step 1: Producing Arrow file ===" >&3

  run timeout 60 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/produce.sql"
  echo "# Producer REPL exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# Producer: $line" >&3
  done
  # 0=success, 232=query terminated, 134=SIGABRT during shutdown (known issue)
  [ "$status" -eq 0 ] || [ "$status" -eq 232 ] || [ "$status" -eq 134 ]

  # Verify Arrow file was created and has content
  [ -f "$ARROW_FILE" ] || { echo "# Arrow file not found: $ARROW_FILE" >&3; false; }
  FILE_SIZE=$(stat -c%s "$ARROW_FILE" 2>/dev/null || stat -f%z "$ARROW_FILE")
  echo "# Arrow file size: $FILE_SIZE bytes" >&3
  [ "$FILE_SIZE" -gt 0 ]

  # ── Step 2: Consume Arrow file and write CSV ────────────────────────
  sed -e "s|ARROW_FILE_PATH|$ARROW_FILE|g" \
      -e "s|CSV_FILE_PATH|$CSV_FILE|g" \
      "$SQL_DIR/arrow_roundtrip_consume.sql" \
    > "$TMP_DIR/consume.sql"

  echo "# === Step 2: Consuming Arrow file → CSV ===" >&3

  run timeout 60 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/consume.sql"
  echo "# Consumer REPL exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# Consumer: $line" >&3
  done
  [ "$status" -eq 0 ] || [ "$status" -eq 232 ] || [ "$status" -eq 134 ]

  # ── Step 3: Validate the CSV output ─────────────────────────────────
  [ -f "$CSV_FILE" ] || { echo "# CSV file not found: $CSV_FILE" >&3; false; }

  # NES CSV files have a schema header line followed by data rows
  TOTAL_LINES=$(wc -l < "$CSV_FILE" | tr -d ' ')
  DATA_ROWS=$((TOTAL_LINES - 1))
  echo "# CSV total lines: $TOTAL_LINES (1 header + $DATA_ROWS data rows)" >&3
  [ "$DATA_ROWS" -gt 0 ]

  # Show first few rows for debugging
  echo "# First 5 lines:" >&3
  head -5 "$CSV_FILE" | while IFS= read -r row; do
    echo "#   $row" >&3
  done

  # Validate data rows: every data line should have exactly 4 commas (5 fields)
  BAD_LINES=$(tail -n +2 "$CSV_FILE" | grep -cvE '^[^,]+,[^,]+,[^,]+,[^,]+,[^,]+$' || true)
  echo "# Data lines not matching 5-field CSV format: $BAD_LINES" >&3
  [ "$BAD_LINES" -eq 0 ]

  # Validate first data row: id should be a non-negative integer
  FIRST_DATA=$(sed -n '2p' "$CSV_FILE")
  FIRST_ID=$(echo "$FIRST_DATA" | cut -d',' -f1)
  FIRST_VAL=$(echo "$FIRST_DATA" | cut -d',' -f2)
  FIRST_RATIO=$(echo "$FIRST_DATA" | cut -d',' -f3)
  FIRST_SMALL=$(echo "$FIRST_DATA" | cut -d',' -f4)
  FIRST_MEDIUM=$(echo "$FIRST_DATA" | cut -d',' -f5)
  echo "# First data row: id=$FIRST_ID value=$FIRST_VAL ratio=$FIRST_RATIO small=$FIRST_SMALL medium=$FIRST_MEDIUM" >&3
  [[ "$FIRST_ID" =~ ^[0-9]+$ ]] || { echo "# FAIL: first id is not a non-negative integer: $FIRST_ID" >&3; false; }

  # All IDs should be unique (no duplicates from the sequence generator)
  UNIQUE_IDS=$(tail -n +2 "$CSV_FILE" | cut -d',' -f1 | sort -n | uniq | wc -l | tr -d ' ')
  echo "# Unique IDs: $UNIQUE_IDS out of $DATA_ROWS data rows" >&3
  [ "$UNIQUE_IDS" -eq "$DATA_ROWS" ]
}
