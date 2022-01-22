#!/usr/bin/env sh


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
for test_folder in "$@"; do
  if [ -d "$test_folder" ]; then
    echo "Build Folder: $test_folder"
    (
    cd $test_folder
    for testCase in $tests; do
      echo "Running test: $testCase"
      LD_PRELOAD="$libnes" sh -c "exec $test_exec --gtest_filter=$test_prefix$testCase" \
        | grep -A 8 "overall runtime: "
    done
    )
  else
    echo >&2 "[ERROR] Folder '$test_folder' not found. Skipping."
  fi
done

