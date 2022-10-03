
add_executable(execution-engine-inlining-benchmarks "Benchmarks.cpp")
target_link_libraries(execution-engine-inlining-benchmarks
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-inlining-benchmarks COMMAND execution-engine-inlining-benchmarks)


