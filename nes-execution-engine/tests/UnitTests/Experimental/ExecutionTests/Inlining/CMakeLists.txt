
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

add_executable(execution-engine-std-dev-benchmark "StdDevBenchmark.cpp")
target_link_libraries(execution-engine-std-dev-benchmark
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-std-dev-benchmark COMMAND execution-engine-std-dev-benchmark)

add_executable(execution-engine-mlir-scan-benchmark "MLIRScanBenchmark.cpp")
target_link_libraries(execution-engine-mlir-scan-benchmark
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-mlir-scan-benchmark COMMAND execution-engine-mlir-scan-benchmark)

add_executable(execution-engine-mlir-doubleagg-benchmark "DoubleAggBenchmark.cpp")
target_link_libraries(execution-engine-mlir-doubleagg-benchmark
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-mlir-doubleagg-benchmark COMMAND execution-engine-mlir-doubleagg-benchmark)


