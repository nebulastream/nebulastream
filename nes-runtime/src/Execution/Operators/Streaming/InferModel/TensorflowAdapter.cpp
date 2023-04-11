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
#include <Execution/Operators/Streaming/InferModel/TensorflowAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdarg>
#include <fstream>
#include <iostream>

#ifdef TFDEF
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>

namespace NES::Runtime::Execution::Operators {

void TensorflowAdapter::initializeModel(std::string model) {
    NES_DEBUG2("INITIALIZING MODEL:  {}", model);

    std::ifstream input(model, std::ios::in | std::ios::binary);

    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));

    input.close();
    NES_INFO2("MODEL SIZE: {}", std::to_string(bytes.size()));
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(model.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);
}

TensorflowAdapterPtr TensorflowAdapter::create() { return std::make_shared<TensorflowAdapter>(); }

float TensorflowAdapter::getResultAt(int i) { return output[i]; }

void TensorflowAdapter::infer(BasicPhysicalType::NativeType dataType, std::vector<NES::Nautilus::Value<>> modelInput) {

    //create input for tensor
    TfLiteTensor* inputTensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
    int inputSize = (int) (TfLiteTensorByteSize(inputTensor));

    //Prepare input parameters based on data type
    if (dataType == BasicPhysicalType::NativeType::INT_64) {

        int* inputData = (int*) malloc(inputSize);

        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = (int) modelInput.at(i);
        }

        //Copy input tensor
        TfLiteTensorCopyFromBuffer(inputTensor, inputData, inputSize);
        //Invoke tensor model and perform inference
        TfLiteInterpreterInvoke(interpreter);

        //Release allocated memory for input
        free(inputData);

        //Clear allocated memory to output
        if (output != nullptr) {
            free(output);
        }

        //Prepare output tensor
        const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
        int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
        output = (float*) malloc(outputSize);

        //Copy value to the output
        TfLiteTensorCopyToBuffer(outputTensor, output, outputSize);

    } else if (dataType == BasicPhysicalType::NativeType::FLOAT) {
        //create input for tensor
        float* inputData = (float*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = (float) modelInput.at(i);
        }

        //Copy input tensor
        TfLiteTensorCopyFromBuffer(inputTensor, inputData, inputSize);
        //Invoke tensor model and perform inference
        TfLiteInterpreterInvoke(interpreter);

        //Release allocated memory for input
        free(inputData);

        //Clear allocated memory to output
        if (output != nullptr) {
            free(output);
        }

        //Prepare output tensor
        const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
        int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
        output = (float*) malloc(outputSize);

        //Copy value to the output
        TfLiteTensorCopyToBuffer(outputTensor, output, outputSize);

    } else if (dataType == BasicPhysicalType::NativeType::DOUBLE) {
        //create input for tensor
        double* inputData = (double*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = (double) modelInput.at(i);
        }

        //Copy input tensor
        TfLiteTensorCopyFromBuffer(inputTensor, inputData, inputSize);
        //Invoke tensor model and perform inference
        TfLiteInterpreterInvoke(interpreter);

        //Release allocated memory for input
        free(inputData);

        //Clear allocated memory to output
        if (output != nullptr) {
            free(output);
        }

        //Prepare output tensor
        const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
        int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
        output = (float*) malloc(outputSize);

        //Copy value to the output
        TfLiteTensorCopyToBuffer(outputTensor, output, outputSize);

    } else if (dataType == BasicPhysicalType::NativeType::BOOLEAN) {
        //create input for tensor
        bool* inputData = (bool*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = (bool) modelInput.at(i);
        }

        //Copy input tensor
        TfLiteTensorCopyFromBuffer(inputTensor, inputData, inputSize);
        //Invoke tensor model and perform inference
        TfLiteInterpreterInvoke(interpreter);

        //Release allocated memory for input
        free(inputData);

        //Clear allocated memory to output
        if (output != nullptr) {
            free(output);
        }

        //Prepare output tensor
        const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
        int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
        output = (float*) malloc(outputSize);

        //Copy value to the output
        TfLiteTensorCopyToBuffer(outputTensor, output, outputSize);
    }
}

}// namespace NES::Runtime::Execution::Operators

#endif// TFDEF