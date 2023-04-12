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
#include <Util/magicenum/magic_enum.hpp>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>

namespace NES::Runtime::Execution::Operators {

TensorflowAdapterPtr TensorflowAdapter::create() { return std::make_shared<TensorflowAdapter>(); }

void TensorflowAdapter::initializeModel(std::string model) {
    NES_DEBUG2("INITIALIZING MODEL:  {}", model);
    std::ifstream input(model, std::ios::in | std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
    input.close();
    NES_INFO2("MODEL SIZE: {}", std::to_string(bytes.size()));
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(model.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);
    this->inputTensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
    this->tensorSize = (int) (TfLiteTensorByteSize(inputTensor));
    this->inputData = (void*) malloc(tensorSize);
}

float TensorflowAdapter::getResultAt(int i) { return outputData[i]; }

void TensorflowAdapter::infer() {

    //Copy input tensor
    TfLiteTensorCopyFromBuffer(inputTensor, inputData, tensorSize);
    //Invoke tensor model and perform inference
    TfLiteInterpreterInvoke(interpreter);

    //Clear allocated memory to output
    if (outputData != nullptr) {
        free(outputData);
    }

    const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
    int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
    outputData = (float*) malloc(outputSize);

    //Copy value to the output
    TfLiteTensorCopyToBuffer(outputTensor, outputData, outputSize);


    /* //Prepare output tensor
    const TfLiteTensor* outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
    int outputSize = (int) (TfLiteTensorByteSize(outputTensor));
    output = (float*) malloc(outputSize);

    //Copy value to the output
    TfLiteTensorCopyToBuffer(outputTensor, output, outputSize);



    //Prepare input parameters based on data type
    if (modelInput[0]->isType<Nautilus::Int32>()) {
        //create input for tensor
        *//*auto* inputData = (<int>*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = modelInput.at(i).as<Nautilus::Int32>();
        }*//*

        //Copy input tensor
        TfLiteTensorCopyFromBuffer(inputTensor, input, inputSize);
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

    } else if (modelInput[0]->isType<Nautilus::Float>()) {
        //create input for tensor
        auto* inputData = (float*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = modelInput.at(i).as<Nautilus::Float>();
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

    } else if (modelInput[0]->isType<Nautilus::Double>()) {
        //create input for tensor
        auto* inputData = (double*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = modelInput.at(i).as<Nautilus::Double>();
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

    } else if (modelInput[0]->isType<Nautilus::Boolean>()) {
        //create input for tensor
        auto* inputData = (bool*) malloc(inputSize);
        for (uint32_t i = 0; i < modelInput.size(); ++i) {
            inputData[i] = modelInput.at(i).as<Nautilus::Boolean>();
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
    }*/
}

}// namespace NES::Runtime::Execution::Operators

#endif// TFDEF