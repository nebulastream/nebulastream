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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <iostream>
#include <stdarg.h>
#include <stdio.h>

#ifdef TFDEF
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>
#endif// TFDEF

#ifdef TFDEF
NES::TensorflowAdapter::TensorflowAdapter() {}

void NES::TensorflowAdapter::initializeModel(std::string model) {
    NES_DEBUG("INITIALIZING MODEL: " << model);

    std::ifstream input(model, std::ios::in | std::ios::binary);

    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));

    input.close();
    NES_INFO("MODEL SIZE: " + std::to_string(bytes.size()));
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(model.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);
}

NES::TensorflowAdapterPtr NES::TensorflowAdapter::create() { return std::make_shared<TensorflowAdapter>(); }

float NES::TensorflowAdapter::getResultAt(int i) { return output[i]; }

void NES::TensorflowAdapter::infer(uint8_t dataType, int n, ...) {

    if (dataType == BasicPhysicalType::NativeType::INT_64){
        va_list vl;
        va_start(vl, n);
        TfLiteTensor* input_tensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
        int input_size = (int) (TfLiteTensorByteSize(input_tensor));

        int* input = (int*) malloc(input_size);

        for (int i = 0; i < n; ++i) {
            input[i] = (int) va_arg(vl, int);
        }
        va_end(vl);

        TfLiteTensorCopyFromBuffer(input_tensor, input, input_size);
        TfLiteInterpreterInvoke(interpreter);
        const TfLiteTensor* output_tensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);

        free(input);
        if (output != nullptr) {
            free(output);
        }
        int output_size = (int) (TfLiteTensorByteSize(output_tensor));
        output = (double*) malloc(output_size);
        TfLiteTensorCopyToBuffer(output_tensor, output, output_size);
    } else if (dataType == BasicPhysicalType::NativeType::FLOAT) {
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

        const TfLiteTensor* output_tensor = (TfLiteInterpreterGetOutputTensor(interpreter, 0));

        free(input);
        if (output != nullptr) {
            free(output);
        }
        int output_size = (int) (TfLiteTensorByteSize(output_tensor));
        output = (double*) malloc(output_size);
        TfLiteTensorCopyToBuffer(output_tensor, output, output_size);
    } else if (dataType == BasicPhysicalType::NativeType::DOUBLE) {
        va_list vl;
        va_start(vl, n);
        TfLiteTensor* input_tensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
        int input_size = (int) (TfLiteTensorByteSize(input_tensor));

        double* input = (double*) malloc(input_size);

        for (int i = 0; i < n; ++i) {
            input[i] = (double) va_arg(vl, double);
        }
        va_end(vl);

        TfLiteTensorCopyFromBuffer(input_tensor, input, input_size);
        TfLiteInterpreterInvoke(interpreter);

        const TfLiteTensor* output_tensor = (TfLiteInterpreterGetOutputTensor(interpreter, 0));

        free(input);
        if (output != nullptr) {
            free(output);
        }
        int output_size = (int) (TfLiteTensorByteSize(output_tensor));
        output = (double*) malloc(output_size);
        TfLiteTensorCopyToBuffer(output_tensor, output, output_size);
    } else if (dataType == BasicPhysicalType::NativeType::BOOLEAN) {
        va_list vl;
        va_start(vl, n);
        TfLiteTensor* input_tensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
        int input_size = (int) (TfLiteTensorByteSize(input_tensor));

        bool* input = (bool*) malloc(input_size);

        for (int i = 0; i < n; ++i) {
            input[i] = (bool) va_arg(vl, int);
        }
        va_end(vl);

        TfLiteTensorCopyFromBuffer(input_tensor, input, input_size);
        TfLiteInterpreterInvoke(interpreter);
        const TfLiteTensor* output_tensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);

        free(input);
        if (output != nullptr) {
            free(output);
        }
        int output_size = (int) (TfLiteTensorByteSize(output_tensor));
        output = (double*) malloc(output_size);
        TfLiteTensorCopyToBuffer(output_tensor, output, output_size);
    }
}

#endif// TFDEF