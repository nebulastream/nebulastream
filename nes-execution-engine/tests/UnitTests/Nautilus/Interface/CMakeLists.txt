

add_executable(nautilus-data-type-test "DataTypeTest.cpp")
target_link_libraries(nautilus-data-type-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-data-type-test COMMAND nautilus-data-type-test)


add_executable(nautilus-custom-data-type-test "CustomDataTypeTest.cpp")
target_link_libraries(nautilus-custom-data-type-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-custom-data-type-test COMMAND nautilus-custom-data-type-test)

add_executable(nautilus-function-call-test "FunctionExecutionTest.cpp")
target_link_libraries(nautilus-function-call-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME nautilus-function-call-test COMMAND nautilus-function-call-test)