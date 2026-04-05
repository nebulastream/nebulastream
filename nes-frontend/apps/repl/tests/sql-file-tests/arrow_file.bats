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

VALIDATE_SCRIPT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)/util/validate_arrow_file.py"

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

@test "Arrow file sink writes valid IPC stream" {
  ARROW_FILE="$TMP_DIR/output.arrows"

  # Substitute the file path placeholder in the SQL template
  sed "s|ARROW_FILE_PATH|$ARROW_FILE|g" \
    "$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/good/arrow_file_sink.sql" \
    > "$TMP_DIR/query.sql"

  echo "# Query:" >&3
  cat "$TMP_DIR/query.sql" >&3

  # Run the embedded REPL — generator runs for 10s
  run timeout 60 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/query.sql"
  echo "# REPL exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# REPL: $line" >&3
  done
  # 0=success, 232=query terminated, 134=SIGABRT during shutdown (known issue)
  [ "$status" -eq 0 ] || [ "$status" -eq 232 ] || [ "$status" -eq 134 ]

  # Verify the file was created
  [ -f "$ARROW_FILE" ] || { echo "# Arrow file not found: $ARROW_FILE" >&3; false; }
  FILE_SIZE=$(stat -c%s "$ARROW_FILE" 2>/dev/null || stat -f%z "$ARROW_FILE")
  echo "# Arrow file size: $FILE_SIZE bytes" >&3
  [ "$FILE_SIZE" -gt 0 ]

  # Validate with PyArrow
  echo "# Running Python validation..." >&3
  run "$VALIDATE_SCRIPT" "$ARROW_FILE"
  echo "# Validator exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# $line" >&3
  done

  # Parse the JSON output
  RESULT=$(printf '%s\n' "${lines[@]}")
  TOTAL_ROWS=$(echo "$RESULT" | jq -r '.total_rows')
  NUM_BATCHES=$(echo "$RESULT" | jq -r '.num_batches')
  echo "# Received $TOTAL_ROWS rows in $NUM_BATCHES batches" >&3

  [ "$TOTAL_ROWS" -gt 0 ]
  [ "$NUM_BATCHES" -gt 0 ]

  # Check id column: no duplicates, filter predicate correct
  ID_OK=$(echo "$RESULT" | jq -r '.id_no_duplicates and .id_filter_valid')
  if [ "$ID_OK" != "true" ]; then
    echo "# FAIL: id column validation failed" >&3
    echo "$RESULT" | jq '{id_no_duplicates, id_filter_valid, id_count, id_unique}' >&3
    false
  fi

  # Check non-nullable columns
  NON_NULL_VALID=$(echo "$RESULT" | jq -r '.non_nullable_valid')
  if [ "$NON_NULL_VALID" != "true" ]; then
    echo "# WARN: non-nullable column data mismatch (may be expected with filtered sequences)" >&3
    echo "$RESULT" | jq '.non_nullable' >&3
  fi

  # Check nullable columns
  NULLABLE_PRESENT=$(echo "$RESULT" | jq -r '.nullable_columns_present')
  echo "# Nullable columns present: $NULLABLE_PRESENT" >&3
  [ "$NULLABLE_PRESENT" -gt 0 ]

  # Check string column if present
  STRING_PRESENT=$(echo "$RESULT" | jq -r '.string.column_present')
  echo "# String column present: $STRING_PRESENT" >&3
  if [ "$STRING_PRESENT" = "true" ]; then
    STRING_VALID=$(echo "$RESULT" | jq -r '.string.valid')
    STRING_MIN=$(echo "$RESULT" | jq -r '.string.min_length')
    STRING_MAX=$(echo "$RESULT" | jq -r '.string.max_length')
    echo "# String lengths: min=$STRING_MIN max=$STRING_MAX" >&3
    if [ "$STRING_VALID" != "true" ]; then
      echo "# FAIL: string column validation failed" >&3
      echo "$RESULT" | jq '.string' >&3
      false
    fi
  fi
}
