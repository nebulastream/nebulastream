add_executable(nautilus-symbolic-tracing-test "SymbolicTracingTest.cpp")
target_link_libraries(nautilus-symbolic-tracing-test
        nes-runtime ${GTEST_LIBRARIES})
add_test(NAME nautilus-symbolic-tracing-test COMMAND nautilus-symbolic-tracing-test)
