
if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR)
    add_executable(execution-engine-proxy-inlining-test "ProxyInliningTest.cpp")
    target_link_libraries(execution-engine-proxy-inlining-test
            nes-execution-engine ${GTEST_LIBRARIES})
    add_test(NAME execution-engine-proxy-inlining-test COMMAND execution-engine-proxy-inlining-test)
endif ()

add_executable(tpch-util-test "TPCHUtilTest.cpp")
target_link_libraries(tpch-util-test
        nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME tpch-util-test COMMAND tpch-util-test)