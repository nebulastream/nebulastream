

add_executable(runtime-mod-expression-test "ModExpressionTest.cpp")
target_link_libraries(runtime-mod-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-mod-expression-test COMMAND runtime-mod-expression-test)
add_executable(runtime-cos-expression-test "CosExpressionTest.cpp")
target_link_libraries(runtime-cos-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-cos-expression-test COMMAND runtime-cos-expression-test)
