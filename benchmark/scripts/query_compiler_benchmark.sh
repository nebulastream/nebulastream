#!/usr/bin/env sh


# This script takes cmake build folders as arguments and will run query-compiler-tests in each of them
# outputting the printed timings in TSV format.
#
# Parameters: Paths to cmake build folders
# STDOUT: TSV including headers
# STDERR: Log messages
#
# ENV configuration options:
# - DELIMITER: delimiter used for the output (default: "\t" [tabs])
#              WARNING: Occurrences of the delimiter in the output is not escaped!
#              Choose a delimiter that will not be in the filtered test log messages (or fix this script).
#
# Usage example:
# /usr/bin/env sh /nebulastream/benchmark/scripts/query_compiler_benchmark.sh "/nebulastream/cmake-build-impro/" \
#   | sed -i -e "s/\/nebulastream\/cmake-build-impro\//header selection/" \
#            -e "s/\/nebulastream\/cmake-build-no-impro\//all headers/" \
#            -e "s/build-folder/variant/" \
#   > impro-query-compiler-benchmark.tsv

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
echo "build-folder${delimiter}query${delimiter}timed_unit${delimiter}time${delimiter}unit"

for test_folder in "$@"; do
  if [ -d "$test_folder" ]; then
    echo >&2 "[INFO] Build Folder: $test_folder"
    (
    cd $test_folder
    for testCase in $tests; do
      echo >&2 "[INFO] Running test: $testCase"
      LD_PRELOAD="$libnes" sh -c "exec $test_exec --gtest_filter=$test_prefix$testCase" \
        | grep -A 8 "overall runtime: " `# filter to collect only the relevant outputs`\
        | sed -e 's/\t//' -e "s/:[\s]*/$delimiter/" -e "s/ ms/${delimiter}ms/" | awk "{print \"$test_folder$delimiter$testCase$delimiter\""' $0}' `# transform to CSV`
    done
    )
  else
    echo >&2 "[ERROR] Folder '$test_folder' not found. Skipping."
  fi
done

