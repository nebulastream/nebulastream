add_executable(execution-engine-proxy-inlining-test "ProxyInliningTest.cpp")
target_link_libraries(execution-engine-proxy-inlining-test
        nes-execution-engine)
add_test(NAME execution-engine-proxy-inlining-test COMMAND execution-engine-proxy-inlining-test)

