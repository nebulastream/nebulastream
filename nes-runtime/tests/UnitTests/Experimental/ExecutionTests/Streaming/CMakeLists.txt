
add_executable(execution-engine-query-ysb-test "YSBTest.cpp")
target_link_libraries(execution-engine-query-ysb-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-query-ysb-test COMMAND execution-engine-query-ysb-test)
