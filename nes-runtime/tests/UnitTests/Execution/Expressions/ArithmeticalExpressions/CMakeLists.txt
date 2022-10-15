

add_executable(runtime-add-expression-test "AddExpressionTest.cpp")
target_link_libraries(runtime-add-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-add-expression-test COMMAND runtime-add-expression-test)

add_executable(runtime-sub-expression-test "SubExpressionTest.cpp")
target_link_libraries(runtime-sub-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-sub-expression-test COMMAND runtime-sub-expression-test)

add_executable(runtime-mul-expression-test "MulExpressionTest.cpp")
target_link_libraries(runtime-mul-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-mul-expression-test COMMAND runtime-mul-expression-test)

add_executable(runtime-div-expression-test "DivExpressionTest.cpp")
target_link_libraries(runtime-div-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-div-expression-test COMMAND runtime-div-expression-test)