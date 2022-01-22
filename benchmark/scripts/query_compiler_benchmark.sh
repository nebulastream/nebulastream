#!/usr/bin/env sh


libnes="nes-core/libnes.so"
test_exec="nes-core/tests/query-compiler-tests --gtest_color=no"
#test_exec="nes-core/tests/query-compiler-tests --gtest_filter=QueryCompilerTest.filterQuery:QueryCompilerTest/*.filterQuery:QueryCompilerTest.filterQuery/*:*/QueryCompilerTest.filterQuery/*:*/QueryCompilerTest/*.filterQuery --gtest_color=no"

for test_folder in ${TEST_FOLDERS:="../../cmake-build-impro/"}; do
  if [ -d "$test_folder" ]; then
    (
    cd $test_folder
    LD_PRELOAD="$libnes" sh -c "exec $test_exec" \
      | grep -A 8 "overall runtime: "
    )
  else
    echo >&2 "Folder '$test_folder' not found. Skipping."
  fi
done