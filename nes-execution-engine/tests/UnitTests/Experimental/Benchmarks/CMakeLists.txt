
add_executable(execution-engine-inlining-benchmark "InliningBenchmark.cpp")
target_link_libraries(execution-engine-inlining-benchmark
        nes-execution-engine)
add_test(NAME execution-engine-inlining-benchmark COMMAND execution-engine-inlining-benchmark)

add_executable(execution-engine-mlir-phases-benchmark "MLIRPhasesTest.cpp")
target_link_libraries(execution-engine-mlir-phases-benchmark
        nes-execution-engine)
add_test(NAME xecution-engine-mlir-phases-benchmark COMMAND xecution-engine-mlir-phases-benchmark)