/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp"
//#include "tensorflow/lite/interpreter.h"
//#include "tensorflow/lite/kernels/register.h"
//#include "tensorflow/lite/model.h"
//#include "tensorflow/lite/optional_debug_tools.h"
#include <fstream>
#include <iostream>
#include <stdarg.h>
#include <stdio.h>

#define TFLITE_MINIMAL_CHECK(x)                              \
  if (!(x)) {                                                \
    fprintf(stderr, "Error at %s:%d\n", __FILE__, __LINE__); \
    exit(1);                                                 \
  }

NES::TensorflowAdapter::TensorflowAdapter() {}

void NES::TensorflowAdapter::initializeModel(std::string model){
    std::cout << "INITIALIZING MODEL: " << model << std::endl;

    std::ifstream input(model, std::ios::in | std::ios::binary);

    std::string bytes(
            (std::istreambuf_iterator<char>(input)),
            (std::istreambuf_iterator<char>()));

    input.close();
    std::cout << "MODEL CONTENT: " << std::endl;
    std::cout << bytes.c_str();

//    std::unique_ptr<tflite::FlatBufferModel> ml_model =
//        tflite::FlatBufferModel::BuildFromFile(model.c_str());
//
//    tflite::ops::builtin::BuiltinOpResolver resolver;
//    tflite::InterpreterBuilder builder(*ml_model, resolver);
//
//    builder(&interpreter);
//    TFLITE_MINIMAL_CHECK(interpreter != nullptr);
//
//    // Allocate tensor buffers.
//    TFLITE_MINIMAL_CHECK(interpreter->AllocateTensors() == kTfLiteOk);
}

 NES::TensorflowAdapterPtr NES::TensorflowAdapter::create() {
    return std::make_shared<TensorflowAdapter>();
}

float NES::TensorflowAdapter::getResultAt(int i) {
    return 42.0f;
    return output[i];
}

void NES::TensorflowAdapter::infer(int n, ...){
    std::cout << "inference... " << std::endl;
    va_list vl;
    va_start(vl, n);

//    float* input = interpreter->typed_input_tensor<float>(0);
//
//    for (int i = 0; i < n; ++i) {
//        input[i] = (float) va_arg(vl, double);
//    }
//
//    va_end(vl);
//
//    // Run inference
//    TFLITE_MINIMAL_CHECK(interpreter->Invoke() == kTfLiteOk);
//
//    output = interpreter->typed_output_tensor<float>(0);
}
