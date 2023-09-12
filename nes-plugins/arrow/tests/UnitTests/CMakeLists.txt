
add_nes_runtime_test(runtime-arrow-operator-tests "ArrowScanEmitPipelineTest.cpp" "ArrowScanOperatorTest.cpp")
target_link_libraries(runtime-arrow-operator-tests PUBLIC nes-arrow)