
add_nes_execution_test(runtime-arrow-operator-tests "ArrowScanEmitPipelineTest.cpp" "ArrowScanOperatorTest.cpp")
target_link_libraries(runtime-arrow-operator-tests nes-arrow)