
add_executable(wasm-expression-test "WASMExpressionTest.cpp")
target_link_libraries(wasm-expression-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME wasm-expression-test COMMAND wasm-expression-test)