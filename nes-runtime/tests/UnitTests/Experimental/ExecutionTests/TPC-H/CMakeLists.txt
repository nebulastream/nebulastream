
add_executable(execution-engine-query-1-test "Query1Test.cpp")
target_link_libraries(execution-engine-query-1-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-query-1-test COMMAND execution-engine-query-1-test)

add_executable(execution-engine-query-3-test "Query3Test.cpp")
target_link_libraries(execution-engine-query-3-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-query-3-test COMMAND execution-engine-query-3-test)

add_executable(execution-engine-query-6-test "Query6Test.cpp")
target_link_libraries(execution-engine-query-6-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-query-6-test COMMAND execution-engine-query-6-test)


