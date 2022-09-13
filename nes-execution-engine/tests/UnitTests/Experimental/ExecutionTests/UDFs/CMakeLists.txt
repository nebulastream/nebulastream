
if (NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI OR NES_ENABLE_EXPERIMENTAL_EXECUTION_BABELFISH)
    add_executable(execution-engine-query-udf-test "UDFTest.cpp")
    target_link_libraries(execution-engine-query-udf-test
            nes-execution-engine ${GTEST_LIBRARIES})
    add_test(NAME execution-engine-query-udf-test COMMAND execution-engine-query-udf-test)
endif ()