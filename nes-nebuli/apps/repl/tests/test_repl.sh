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


# Check if exactly 6 arguments are provided
if [ $# -ne 6 ]; then
    echo "Usage: $0 <sql_file_path> <binary_path> <testdata_path> <workdir_path> <expected_output_path> <gen_out_path>"
    exit 1
fi

# Assign arguments to variables
SQL_FILE="$1"
BINARY_PATH=$(readlink -f "$2")
TESTDATA_PATH=$(readlink -f "$3")
WORKDIR_PATH=$(readlink -f "$4")
EXPECTED_OUTPUT_PATH=$(readlink -f "$5")
GEN_OUT_PATH=$(readlink -f "$6")

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: SQL file '$SQL_FILE' does not exist"
    exit 1
fi

if [ ! "${SQL_FILE##*.}" = "sql" ]; then
  echo "Error: Expected .sql file, got: $SQL_FILE"
  exit 1
fi

if [ ! -f "$EXPECTED_OUTPUT_PATH" ]; then
    echo "Error: SQL file '$EXPECTED_OUTPUT_PATH' does not exist"
    exit 1
fi

if [ ! -x "$BINARY_PATH" ]; then
    echo "Error: Binary '$BINARY_PATH' does not exist or is not executable"
    exit 1
fi

mkdir --parents "$GEN_OUT_PATH"
mkdir --parents "$WORKDIR_PATH"

# Read the SQL file content and perform replacements, save it to a file so that one can run it themselves
SQL_CONTENT=$(cat "$SQL_FILE" | sed "s|TESTDATA|$TESTDATA_PATH|g" | sed "s|WORKDIR|$WORKDIR_PATH|g")
echo "$SQL_CONTENT" > "$GEN_OUT_PATH"/"$(basename "$SQL_FILE" .sql)"_gen.sql
EXPECTED_OUTPUT_1=$(cat "$EXPECTED_OUTPUT_PATH" | sed "s|TESTDATA|$TESTDATA_PATH|g" | sed "s|WORKDIR|$WORKDIR_PATH|g")
EXPECTED_OUTPUT_2=$(cat "$EXPECTED_OUTPUT_PATH" | sed "s|TESTDATA|$TESTDATA_PATH|g" | sed "s|WORKDIR|$WORKDIR_PATH|g" | sed "s|Running|Registered|g")
EXPECTED_OUTPUT_3=$(cat "$EXPECTED_OUTPUT_PATH" | sed "s|TESTDATA|$TESTDATA_PATH|g" | sed "s|WORKDIR|$WORKDIR_PATH|g" | sed "s|Running|Started|g")
echo "$EXPECTED_OUTPUT_1" > "$GEN_OUT_PATH"/"$(basename "$EXPECTED_OUTPUT_PATH" .sql)"_gen1.sql
echo "$EXPECTED_OUTPUT_2" > "$GEN_OUT_PATH"/"$(basename "$EXPECTED_OUTPUT_PATH" .sql)"_gen2.sql
echo "$EXPECTED_OUTPUT_3" > "$GEN_OUT_PATH"/"$(basename "$EXPECTED_OUTPUT_PATH" .sql)"_gen3.sql

# Run the binary and pipe the modified SQL content as stdin
ACTUAL_OUTPUT=$(echo "$SQL_CONTENT" | "$BINARY_PATH" -w -f JSON )

if [[ "$ACTUAL_OUTPUT" == "$EXPECTED_OUTPUT_1" || "$ACTUAL_OUTPUT" == "$EXPECTED_OUTPUT_2" || "$ACTUAL_OUTPUT" == "$EXPECTED_OUTPUT_3" ]]; then
  exit 0
else
  echo "Actual output does not match expected, diff 1 was:"
  echo "$(diff <(echo "$EXPECTED_OUTPUT_1") <(echo "$ACTUAL_OUTPUT"))"
  echo "Actual output does not match expected, diff 2 was:"
  echo "$(diff <(echo "$EXPECTED_OUTPUT_2") <(echo "$ACTUAL_OUTPUT"))"
  echo "Actual output does not match expected, diff 3 was:"
  echo "$(diff <(echo "$EXPECTED_OUTPUT_3") <(echo "$ACTUAL_OUTPUT"))"
  exit 1
fi
