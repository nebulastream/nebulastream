add_executable(nautilus-symbolic-tracing-test "SymbolicTracingTest.cpp")
target_link_libraries(nautilus-symbolic-tracing-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-symbolic-tracing-test COMMAND nautilus-symbolic-tracing-test)

add_executable(nautilus-tag-creation-test "TagCreationTest.cpp")
target_link_libraries(nautilus-tag-creation-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-tag-creation-test COMMAND nautilus-tag-creation-test)

