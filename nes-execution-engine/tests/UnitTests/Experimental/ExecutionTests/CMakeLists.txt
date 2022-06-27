
add_executable(execution-engine-expression-execution-test "ExpressionExecutionTest.cpp")
target_link_libraries(execution-engine-expression-execution-test
        nes-execution-engine)
add_test(NAME execution-engine-expression-execution-test COMMAND execution-engine-expression-execution-test)

add_executable(execution-engine-if-execution-test "IfExecutionTest.cpp")
target_link_libraries(execution-engine-if-execution-test
        nes-execution-engine)
add_test(NAME execution-engine-expression-if-test COMMAND execution-engine-if-execution-test)

add_executable(execution-engine-loop-execution-test "LoopExecutionTest.cpp")
target_link_libraries(execution-engine-loop-execution-test
        nes-execution-engine)
add_test(NAME execution-engine-expression-loop-test COMMAND execution-engine-loop-execution-test)

add_executable(execution-engine-query-execution-test "QueryExecutionTest.cpp")
target_link_libraries(execution-engine-query-execution-test
        nes-execution-engine)
add_test(NAME execution-engine-expression-query-test COMMAND execution-engine-query-execution-test)

