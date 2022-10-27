add_executable(runtime-mod-expression-test "ModExpressionTest.cpp")
target_link_libraries(runtime-mod-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-mod-expression-test COMMAND runtime-mod-expression-test)

add_executable(runtime-atan-expression-test "AtanExpressionTest.cpp")
target_link_libraries(runtime-atan-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-atan-expression-test COMMAND runtime-atan-expression-test)

