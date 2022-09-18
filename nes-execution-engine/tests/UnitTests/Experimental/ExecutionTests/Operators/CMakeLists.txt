
add_executable(execution-engine-emit-operator-test "EmitOperatorTest.cpp")
target_link_libraries(execution-engine-emit-operator-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-emit-operator-test COMMAND execution-engine-emit-operator-test)

add_executable(execution-engine-join-operator-test "JoinOperatorTest.cpp")
target_link_libraries(execution-engine-join-operator-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-join-operator-test COMMAND execution-engine-join-operator-test)

add_executable(execution-engine-aggregation-operator-test "AggregationOperatorTest.cpp")
target_link_libraries(execution-engine-aggregation-operator-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-aggregation-operator-test COMMAND execution-engine-aggregation-operator-test)
