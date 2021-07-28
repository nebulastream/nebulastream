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
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/c_api_experimental.h>
#include <tensorflow/lite/c/common.h>
#include <fstream>
#include <iostream>
#include <stdarg.h>
#include <stdio.h>

#define TFLITE_MINIMAL_CHECK(x)                              \
  if (!(x)) {                                                \
    fprintf(stderr, "Error at %s:%d\n", __FILE__, __LINE__); \
    exit(1);                                                 \
  }

class TfLiteModel;

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

    printf("Hello from TensorFlow C library version %s\n", TfLiteVersion());

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
    return output[i];
    return 42.0f;
}

void NES::TensorflowAdapter::infer(int n, ...){
    std::cout << "inference... " << std::endl;
    va_list vl;
    va_start(vl, n);

    TfLiteModel* model = TfLiteModelCreateFromFile("/home/sumegim/Documents/tub/thesis/tflite/hello_world/iris_97acc.tflite");
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
//   TfLiteInterpreterOptionsSetNumThreads(options, 2);
    TfLiteInterpreter* interpreter = TfLiteInterpreterCreate(model, options);
    TfLiteInterpreterAllocateTensors(interpreter);

    TfLiteTensor* input_tensor = TfLiteInterpreterGetInputTensor(interpreter, 0);

    float* input = (float*) malloc(4 * sizeof(float));
    float* o = (float*) malloc(3 * sizeof(float));

    for (int i = 0; i < n; ++i) {
        input[i] = (float) va_arg(vl, double);
    }
    va_end(vl);

    TfLiteTensorCopyFromBuffer(input_tensor, input, 4 * sizeof(float));
    TfLiteInterpreterInvoke(interpreter);
    const TfLiteTensor* output_tensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);

    TfLiteTensorCopyToBuffer(output_tensor, o, 3 * sizeof(float));

    output = o;
    free(input);


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
