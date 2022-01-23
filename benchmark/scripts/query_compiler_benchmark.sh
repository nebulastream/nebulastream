#!/usr/bin/env sh
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This script takes cmake build folders as arguments and will run query-compiler-tests in each of them
# outputting the printed timings in TSV format.
#
# Parameters: Paths to cmake build folders
# STDOUT: TSV including headers
# STDERR: Log messages
#
# ENV configuration options:
# - DELIMITER: delimiter used for the output (default: "\t" [tabs]).
#              WARNING: Occurrences of the delimiter in the output is not escaped!
#              Choose a delimiter that will not be in the filtered test log messages (or fix this script).
# - RUNS     : number of repetitions of all test runs (default: 10).
#
# Usage example:
# /usr/bin/env sh /nebulastream/benchmark/scripts/query_compiler_benchmark.sh \
#    "/nebulastream/cmake-build-impro/" "/nebulastream/cmake-build-no-impro/" \
#    | sed -e "s/\/nebulastream\/cmake-build-impro\//header selection/" \
#          -e "s/\/nebulastream\/cmake-build-no-impro\//all headers/" \
#          -e "s/build-folder/variant/" -e "s/Query\t/\t/" \
#   > impro-query-compiler-benchmark-$(date --iso-8601=minutes).tsv

libnes="nes-core/libnes.so"
test_exec="nes-core/tests/query-compiler-tests --gtest_color=no"
test_prefix="QueryCompilerTest."
# output from --gtest_list_tests
tests="filterQuery
       filterQueryBitmask
       mapQuery
       windowQuery
       windowQueryEventTime
       tumblingWindowQueryIngestionTime
       tumblingWindowQueryEventTime
       unionQuery
       joinQuery
       externalOperatorTest"

echo >&2 "[DEBUG] Going to run $test_prefix tests in: $*"

# print CSV header
delimiter="${DELIMITER:=\t}"
echo "run_number${delimiter}build-folder${delimiter}query${delimiter}timed_unit${delimiter}time${delimiter}unit"

for run_number in $(seq "${RUNS:=10}"); do
  for test_folder in "$@"; do
    if [ -d "$test_folder" ]; then
      echo >&2 "[INFO] Build Folder: $test_folder"
      (
      cd $test_folder
      for testCase in $tests; do
        echo >&2 "[INFO][$run_number] Running test: $testCase"
        LD_PRELOAD="$libnes" sh -c "exec $test_exec --gtest_filter=$test_prefix$testCase" \
          | grep -A 8 "overall runtime: " `# filter to collect only the relevant outputs`\
          | sed -e 's/\t//' -e "s/:[\s]*/$delimiter/" -e "s/ ms/${delimiter}ms/" | awk "{print \"$run_number$delimiter$test_folder$delimiter$testCase$delimiter\""' $0}' `# transform to CSV`
      done
      )
    else
      echo >&2 "[ERROR] Folder '$test_folder' not found. Skipping."
    fi
  done
done
