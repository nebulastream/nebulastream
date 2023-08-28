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
#include <Util/magicenum/magic_enum.hpp>
#include <fstream>
#include <iostream>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/common.h>

namespace NES::Runtime::Execution::Operators {

InferenceAdapterPtr TensorflowAdapter::create() { return std::make_shared<TensorflowAdapter>(); }

void TensorflowAdapter::inferInternal(std::span<int8_t>&& input, std::span<int8_t>&& output) {
    //Copy input tensor
    TfLiteTensorCopyFromBuffer(inputTensor, input.data(), input.size());
    //Invoke tensor model and perform inference
    TfLiteInterpreterInvoke(interpreter);
    //Copy value to the output
    TfLiteTensorCopyToBuffer(outputTensor, output.data(), output.size());
}

void TensorflowAdapter::loadModel(const std::string& pathToModel) {
    NES_DEBUG("INITIALIZING MODEL:  {}", pathToModel);
    std::ifstream input(pathToModel, std::ios::in | std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
    input.close();
    NES_INFO("MODEL SIZE: {}", std::to_string(bytes.size()));
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    interpreter = TfLiteInterpreterCreate(TfLiteModelCreateFromFile(pathToModel.c_str()), options);
    TfLiteInterpreterAllocateTensors(interpreter);
    this->outputTensor = TfLiteInterpreterGetOutputTensor(interpreter, 0);
    this->inputTensor = TfLiteInterpreterGetInputTensor(interpreter, 0);
}

size_t TensorflowAdapter::getModelOutputSizeInBytes() { return TfLiteTensorByteSize(outputTensor); }
size_t TensorflowAdapter::getModelInputSizeInBytes() { return TfLiteTensorByteSize(inputTensor); }

}// namespace NES::Runtime::Execution::Operators
