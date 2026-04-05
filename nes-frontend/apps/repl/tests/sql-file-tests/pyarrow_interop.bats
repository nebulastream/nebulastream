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

# PyArrow interoperability test:
#   1. PyArrow generates an Arrow IPC file with 1000 rows covering all NES-supported types
#   2. NES reads it via ArrowFileSource and writes back via ArrowFileSink
#   3. PyArrow reads the NES output and validates every value matches
#
# This verifies that NES Arrow support is compatible with the reference PyArrow implementation.

SQL_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/good"
UTIL_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)/../util"

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

@test "PyArrow interop: PyArrow → ArrowFileSource → ArrowFileSink → PyArrow" {
  INPUT_ARROW="$TMP_DIR/pyarrow_input.arrows"
  OUTPUT_ARROW="$TMP_DIR/nes_output.arrows"

  # ── Step 1: Generate test data with PyArrow ─────────────────────────
  echo "# === Step 1: Generating Arrow file with PyArrow ===" >&3

  run python3 "$UTIL_DIR/generate_arrow_testdata.py" "$INPUT_ARROW"
  echo "# Generator exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# Generator: $line" >&3
  done
  [ "$status" -eq 0 ]

  [ -f "$INPUT_ARROW" ] || { echo "# Input Arrow file not found: $INPUT_ARROW" >&3; false; }
  INPUT_SIZE=$(stat -c%s "$INPUT_ARROW" 2>/dev/null || stat -f%z "$INPUT_ARROW")
  echo "# Input Arrow file size: $INPUT_SIZE bytes" >&3
  [ "$INPUT_SIZE" -gt 0 ]

  # ── Step 2: NES reads the file and writes it back ───────────────────
  sed -e "s|ARROW_INPUT_PATH|$INPUT_ARROW|g" \
      -e "s|ARROW_OUTPUT_PATH|$OUTPUT_ARROW|g" \
      "$SQL_DIR/pyarrow_interop_consume.sql" \
    > "$TMP_DIR/consume.sql"

  echo "# === Step 2: NES processing (ArrowFileSource → ArrowFileSink) ===" >&3

  run timeout 60 $NES_REPL -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION < "$TMP_DIR/consume.sql"
  echo "# NES REPL exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# NES: $line" >&3
  done
  [ "$status" -eq 0 ] || [ "$status" -eq 232 ] || [ "$status" -eq 134 ]

  [ -f "$OUTPUT_ARROW" ] || { echo "# Output Arrow file not found: $OUTPUT_ARROW" >&3; false; }
  OUTPUT_SIZE=$(stat -c%s "$OUTPUT_ARROW" 2>/dev/null || stat -f%z "$OUTPUT_ARROW")
  echo "# Output Arrow file size: $OUTPUT_SIZE bytes" >&3
  [ "$OUTPUT_SIZE" -gt 0 ]

  # ── Step 3: Validate with PyArrow ───────────────────────────────────
  echo "# === Step 3: Validating NES output with PyArrow ===" >&3

  run python3 "$UTIL_DIR/validate_arrow_roundtrip.py" "$OUTPUT_ARROW"
  echo "# Validator exit status: $status" >&3
  for line in "${lines[@]}"; do
    echo "# Validator: $line" >&3
  done
  [ "$status" -eq 0 ]
}
