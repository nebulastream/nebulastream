
add_executable(wasm-compiler-test "WASMCompilerTest.cpp")
target_link_libraries(wasm-compiler-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME wasm-compiler-test COMMAND wasm-compiler-test)