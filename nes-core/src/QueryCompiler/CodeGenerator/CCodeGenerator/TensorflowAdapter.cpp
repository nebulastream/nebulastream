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
#ifdef TFDEF
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>
#endif
#include "QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp"
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

    #ifdef TFDEF
    printf("Hello from TensorFlow C library version %s\n", TfLiteVersion());
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(model.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);
    #else
    printf("Tensoflow Disabled, use -DUSE_TF cmake option\n");
    #endif

}

 NES::TensorflowAdapterPtr NES::TensorflowAdapter::create() {
    return std::make_shared<TensorflowAdapter>();
}

float NES::TensorflowAdapter::getResultAt(int i) {
    #ifdef TFDEF
    return output[i];
    #else
    if(i) printf("");
    return 0.0;
    #endif
}

void NES::TensorflowAdapter::infer(int n, ...){
#ifdef TFDEF
    va_list vl;
    va_start(vl, n);

    TfLiteTensor* input_tensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
    int input_size = (int) (TfLiteTensorByteSize(input_tensor));

    float* input = (float*) malloc(input_size);

    for (int i = 0; i < n; ++i) {
        input[i] = (float) va_arg(vl, double);
    }
    va_end(vl);

    TfLiteTensorCopyFromBuffer(input_tensor, input, input_size);
    TfLiteInterpreterInvoke(interpreter);
    const TfLiteTensor* output_tensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);

    free(input);
    if(output != nullptr){
        free(output);
    }

    int output_size = (int) (TfLiteTensorByteSize(output_tensor));
    output = (float*) malloc(output_size);
    TfLiteTensorCopyToBuffer(output_tensor, output, output_size);
#else
    if(n) printf("");
    if(output) printf("");
#endif
}
