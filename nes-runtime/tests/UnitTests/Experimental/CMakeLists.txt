
add_executable(execution-engine-trace-test "TraceTest.cpp")
target_link_libraries(execution-engine-trace-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-trace-test COMMAND execution-engine-trace-test)

add_executable(execution-engine-loop-inference-phase-test "LoopInferencePhaseTest.cpp")
target_link_libraries(execution-engine-loop-inference-phase-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-loop-inference-phase-test COMMAND execution-engine-loop-inference-phase-test)


add_executable(execution-engine-value-test "ValueTest.cpp")
target_link_libraries(execution-engine-value-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-value-test COMMAND execution-engine-value-test)

add_executable(execution-engine-operator-test "Interpreter/OperatorTest.cpp")
target_link_libraries(execution-engine-operator-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-operator-test COMMAND execution-engine-operator-test)

add_executable(execution-engine-expression-test "Interpreter/ExpressionTest.cpp")
target_link_libraries(execution-engine-expression-test  nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-expression-test COMMAND execution-engine-expression-test)

add_subdirectory(ExecutionTests)

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_FLOUNDER)
 add_executable(execution-engine-flounder-test "Flounder/FlounderTest.cpp")
 target_link_libraries(execution-engine-flounder-test
         nes-execution-engine ${GTEST_LIBRARIES})
 add_test(NAME execution-engine-flounder-test COMMAND execution-engine-flounder-test)

 add_executable(execution-engine-flounder-exec-test "Flounder/FlounderExpressionExecutionTest.cpp")
 target_link_libraries(execution-engine-flounder-exec-test  nes-execution-engine ${GTEST_LIBRARIES})
 add_test(NAME execution-engine-flounder-exec-test COMMAND execution-engine-flounder-exec-test)
endif ()


add_subdirectory(UtilityTests)
