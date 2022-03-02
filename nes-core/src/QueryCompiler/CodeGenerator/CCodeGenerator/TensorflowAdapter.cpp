/*
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

NES::TensorflowAdapter::TensorflowAdapter() {}

void NES::TensorflowAdapter::initializeModel(std::string model){
    std::cout << "INITIALIZING MODEL: " << model << std::endl;

    std::ifstream input(model, std::ios::in | std::ios::binary);

    std::string bytes(
            (std::istreambuf_iterator<char>(input)),
            (std::istreambuf_iterator<char>()));

    input.close();
    std::cout << "MODEL SIZE: " << bytes.size() << std::endl;

    printf("Hello from TensorFlow C library version %s\n", TfLiteVersion());

    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(model.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);

}

 NES::TensorflowAdapterPtr NES::TensorflowAdapter::create() {
    return std::make_shared<TensorflowAdapter>();
}

float NES::TensorflowAdapter::getResultAt(int i) {
    return output[i];
}

void NES::TensorflowAdapter::infer(int n, ...){
    va_list vl;
    va_start(vl, n);

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
}
