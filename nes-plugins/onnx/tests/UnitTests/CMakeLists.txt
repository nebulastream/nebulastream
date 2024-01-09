add_nes_execution_test(runtime-onnx-inference-operator-test "ONNXInferenceOperatorTest.cpp")
target_link_libraries(runtime-onnx-inference-operator-test nes-onnx nes-client)
