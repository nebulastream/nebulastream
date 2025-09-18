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
setup_file() {
  # Validate SYSTEST environment variable once for all tests
  if [ -z "$SYSTEST" ]; then
    echo "ERROR: SYSTEST environment variable must be set" >&2
    echo "Usage: SYSTEST=/path/to/systest bats systest.bats" >&2
    exit 1
  fi

  if [ -z "$SYSTEST_TESTDATA" ]; then
    echo "ERROR: SYSTEST_TESTDATA environment variable must be set" >&2
    echo "Usage: SYSTEST_TESTDATA=/path/to/systest/testdata" >&2
    exit 1
  fi

  if [ ! -f "$SYSTEST" ]; then
    echo "ERROR: SYSTEST file does not exist: $SYSTEST" >&2
    exit 1
  fi

  if [ ! -x "$SYSTEST" ]; then
    echo "ERROR: SYSTEST file is not executable: $SYSTEST" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using SYSTEST: $SYSTEST" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r $SYSTEST_TESTDATA $TMP_DIR
  find $TMP_DIR/testdata -name "*.dummy" -type f -exec bash -c 'mv "$1" "${1%.dummy}.test"' _ {} \;
  cd $TMP_DIR

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

#teardown() {
##    rm -rf $TMP_DIR
#}

@test "systest shows help" {
  run $SYSTEST --help
  [ "$status" -eq 0 ]
}

@test "loads single test file with all queries" {
  run $SYSTEST --log-path $TMP_DIR/systest.log -t $TMP_DIR/testdata/comments.test
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 5 queries" ]]
}

@test "loads single test file with specific query" {
  run $SYSTEST -t $TMP_DIR/testdata/comments.test:1
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 1 queries" ]]
}

@test "handles non-existing test file" {
  run $SYSTEST -t $TMP_DIR/testdata/comments-not-existing.test
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Test file/directory does not exist" ]]
}

@test "handles non-existing test file with query number" {
  run $SYSTEST -t $TMP_DIR/testdata/comments-not-existing.test:1
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Test file/directory does not exist" ]]
}

@test "loads test directory" {
  run $SYSTEST -t testdata
  [ "$status" -ne 0 ] # Some tests fail
  [[ "$output" =~ "Loaded 17/23 test files containing a total of 46 queries" ]]
}

@test "verify failing tests" {
  run $SYSTEST -t testdata/errors/MultipleCorrectAndIncorrect.test
  [ "$status" -ne 0 ] # Some tests fail

  [[ "$output" =~ '/8 MultipleCorrectAndIncorrect:01' ]]
  [[ "$output" =~ '/8 MultipleCorrectAndIncorrect:05' ]]
  [[ "$output" =~ '/8 MultipleCorrectAndIncorrect:06' ]]
  [[ "$output" =~ '/8 MultipleCorrectAndIncorrect:08' ]]

  [[ "$output" =~ 'MultipleCorrectAndIncorrect:4' ]]
  [[ "$output" =~ 'MultipleCorrectAndIncorrect:7' ]]
  [[ "$output" =~ 'MultipleCorrectAndIncorrect:3' ]]
  [[ "$output" =~ 'MultipleCorrectAndIncorrect:2' ]]
}

@test "handles directory with query number (should fail)" {
  run $SYSTEST -t $TMP_DIR/testdata:1
  [ "$status" -ne 0 ]
  [[ "$output" =~ "is not a test file" ]]
}

@test "filters tests by group (case insensitive)" {
  run $SYSTEST -t testdata -g "Comments"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 5 queries" ]]

  run $SYSTEST -t testdata -g "comments"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 5 queries" ]]

  run $SYSTEST -t testdata -g comments
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 5 queries" ]]
}

@test "filters tests by multiple groups" {
  run $SYSTEST -t testdata -g "invalid" comments
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Loaded 1/4 test files containing a total of 5 queries" ]]
}

@test "excludes tests by group" {
  run $SYSTEST -t testdata -e Comments
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Loaded 16/22 test files containing a total of 41 queries" ]]
}

@test "excludes multiple test groups" {
  run $SYSTEST -t testdata -e "comments" "invalid"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Loaded 16/19 test files containing a total of 41 queries" ]]
}

@test "creates working directory and outputs files" {
  run $SYSTEST -t testdata --workingDir $TMP_DIR/work-dir -g "comments"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Loaded 1/1 test files containing a total of 5 queries" ]]

  # Check that files were created 5 sinks for 5 query + 1 source file
  csv_count=$(find $TMP_DIR/work-dir -name '*.csv' | wc -l)
  [ "$csv_count" -eq 6 ]

  file_count=$(ls $TMP_DIR/work-dir | wc -l)
  [ "$file_count" -eq 2 ]
}

@test "creates log file at specified path" {
  run $SYSTEST -t testdata --workingDir $TMP_DIR/work-dir --log-path $TMP_DIR/test.log -g "comments"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Find the log at: file://$TMP_DIR/test.log" ]]
  [ -f "$TMP_DIR/test.log" ]
}

@test "benchmark mode with source group" {
  run $SYSTEST -b -t testdata -g source --configurationLocation testdata/configs --topology testdata/configs/topologies/default_distributed.yaml
  [ "$status" -eq 0 ]
}

@test "benchmark mode creates results JSON" {
  run $SYSTEST -b -t testdata -g valid -e source --workingDir $TMP_DIR/benchmark-dir
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Running systests in benchmarking mode, a query at a time" ]]

  # Verify the JSON structure
  run jq -e '.failedQueries == 2 and .totalQueries == 7 and (.queryResults | length == 5) and .queryResults[0].testName == "comments:1"' $TMP_DIR/benchmark-dir/BenchmarkResults.json
  [ "$status" -eq 0 ]
}

@test "benchmark mode rejects concurrency" {
  run $SYSTEST -b -n 100 -t testdata
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Cannot run systest in benchmarking mode with concurrency enabled" ]]
}

# Helper function for background process tests
kill_and_wait() {
  local pid=$1
  kill -SIGINT $pid
  wait $pid 2>/dev/null || true
}

@test "endless benchmark mode can be interrupted" {
  $SYSTEST -b --endless -t testdata -g valid --log-path $TMP_DIR/endless_test.log --workingDir $TMP_DIR/endless-dir &
  local pid=$!

  sleep 1
  kill_and_wait $pid

  [ -f "$TMP_DIR/endless-dir/BenchmarkResults.json" ]
}

@test "concurrent endless benchmark modes" {
  # Start three concurrent processes
  $SYSTEST -b --endless -t testdata -g valid --log-path $TMP_DIR/endless_test1.log --workingDir $TMP_DIR/benchmark-dir1 &
  local pid1=$!
  $SYSTEST -b --endless -t testdata -g valid --log-path $TMP_DIR/endless_test2.log --workingDir $TMP_DIR/benchmark-dir2 &
  local pid2=$!
  $SYSTEST -b --endless -t testdata -g valid --log-path $TMP_DIR/endless_test3.log --workingDir $TMP_DIR/benchmark-dir3 &
  local pid3=$!

  sleep 3

  kill_and_wait $pid1
  kill_and_wait $pid2
  kill_and_wait $pid3

  # Verify all three created valid results
  run jq -e '.failedQueries == 2 and .totalQueries > 5' $TMP_DIR/benchmark-dir1/BenchmarkResults.json
  [ "$status" -eq 0 ]

  run jq -e '.failedQueries == 2 and .totalQueries > 5' $TMP_DIR/benchmark-dir2/BenchmarkResults.json
  [ "$status" -eq 0 ]

  run jq -e '.failedQueries == 2 and .totalQueries > 5' $TMP_DIR/benchmark-dir3/BenchmarkResults.json
  [ "$status" -eq 0 ]
}
