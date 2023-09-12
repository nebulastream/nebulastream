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
#include <Execution/Operators/Tensorflow/TensorflowAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fstream>
#include <iostream>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>

namespace NES::Runtime::Execution::Operators {

TensorflowAdapterPtr TensorflowAdapter::create() { return std::make_shared<TensorflowAdapter>(); }

void TensorflowAdapter::initializeModel(std::string pathToModel) {
    NES_DEBUG("INITIALIZING MODEL:  {}", pathToModel);
    std::ifstream input(pathToModel, std::ios::in | std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
    input.close();
    NES_INFO("MODEL SIZE: {}", std::to_string(bytes.size()));
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(pathToModel.c_str()), options);
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
}

TensorflowAdapter::~TensorflowAdapter() {
    free(inputData);
    free(outputData);
}

}// namespace NES::Runtime::Execution::Operators
