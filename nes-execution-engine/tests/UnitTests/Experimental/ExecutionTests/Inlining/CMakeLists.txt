
add_executable(execution-engine-inlining-benchmarks "Benchmarks.cpp")
target_link_libraries(execution-engine-inlining-benchmarks
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-inlining-benchmarks COMMAND execution-engine-inlining-benchmarks)

add_executable(execution-engine-scan-query-benchmark "ScanQuery.cpp")
target_link_libraries(execution-engine-scan-query-benchmark
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-scan-query-benchmark COMMAND execution-engine-scan-query-benchmark)

add_executable(execution-engine-filter-query-benchmark "FilterQuery.cpp")
target_link_libraries(execution-engine-filter-query-benchmark
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-filter-query-benchmark COMMAND execution-engine-filter-query-benchmark)


