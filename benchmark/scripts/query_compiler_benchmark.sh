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

# print CSV header
csv_delimiter="${CSV_DELIMITER:=\t}"
echo "build-folder${csv_delimiter}query${csv_delimiter}timed_unit${csv_delimiter}time${csv_delimiter}unit"

for test_folder in "$@"; do
  if [ -d "$test_folder" ]; then
    echo >&2 "Build Folder: $test_folder"
    (
    cd $test_folder
    for testCase in $tests; do
      echo >&2 "Running test: $testCase"
      LD_PRELOAD="$libnes" sh -c "exec $test_exec --gtest_filter=$test_prefix$testCase" \
        | grep -A 8 "overall runtime: " `# filter to collect only the relevant outputs`\
        | sed -e 's/\t//' -e "s/:[\s]*/$csv_delimiter/" -e "s/ ms/${csv_delimiter}ms/" | awk "{print \"$test_folder$csv_delimiter$testCase$csv_delimiter\""' $0}' `# transform to CSV`
    done
    )
  else
    echo >&2 "[ERROR] Folder '$test_folder' not found. Skipping."
  fi
done

