

add_executable(runtime-add-expression-test "AddExpressionTest.cpp")
target_link_libraries(runtime-add-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-add-expression-test COMMAND runtime-add-expression-test)


