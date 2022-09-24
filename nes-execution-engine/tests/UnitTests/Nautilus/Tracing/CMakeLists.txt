add_executable(nautilus-symbolic-trace-test "SymbolicExecutionTest.cpp")
target_link_libraries(nautilus-symbolic-trace-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-symbolic-trace-test COMMAND nautilus-symbolic-trace-test)
