if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR)
        add_executable(execution-engine-loop-execution-test "LoopExecutionTest.cpp")
        target_link_libraries(execution-engine-loop-execution-test nes-execution-engine ${GTEST_LIBRARIES})
        add_test(NAME execution-engine-expression-loop-test COMMAND execution-engine-loop-execution-test)

        add_executable(execution-engine-if-execution-test "IfExecutionTest.cpp")
        target_link_libraries(execution-engine-if-execution-test
                nes-execution-engine ${GTEST_LIBRARIES})
        add_test(NAME execution-engine-expression-if-test COMMAND execution-engine-if-execution-test)

        add_executable(execution-engine-memory-access-execution-test "MemoryAccessExecutionTest.cpp")
        target_link_libraries(execution-engine-memory-access-execution-test
                nes-execution-engine ${GTEST_LIBRARIES})
        add_test(NAME execution-engine-memory-access-test COMMAND execution-engine-memory-access-execution-test)

        add_executable(execution-engine-expression-execution-test "ExpressionExecutionTest.cpp")
        target_link_libraries(execution-engine-expression-execution-test
                nes-execution-engine ${GTEST_LIBRARIES})
        add_test(NAME execution-engine-expression-execution-test COMMAND execution-engine-expression-execution-test)
endif ()
        
add_subdirectory(TPC-H)
add_subdirectory(Streaming)
add_subdirectory(Operators)
add_subdirectory(UDFs)