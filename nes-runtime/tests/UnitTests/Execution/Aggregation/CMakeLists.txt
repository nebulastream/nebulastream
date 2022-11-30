add_executable(runtime-aggregation-test "AggregationFunctionTest.cpp")
target_link_libraries(runtime-aggregation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-aggregation-test COMMAND runtime-aggregation-test)