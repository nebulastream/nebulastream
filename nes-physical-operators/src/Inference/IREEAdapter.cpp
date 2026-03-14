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

#include <Inference/IREEAdapter.hpp>

#include <span>
#include <Model.hpp>

namespace NES::Inference
{

std::shared_ptr<IREEAdapter> IREEAdapter::create()
{
    return std::make_shared<IREEAdapter>();
}

void IREEAdapter::initializeModel(NES::Nebuli::Inference::Model& model)
{
    this->runtimeWrapper = IREERuntimeWrapper();
    runtimeWrapper.setup(iree_const_byte_span_t{
        .data = reinterpret_cast<const uint8_t*>(model.getByteCode().data()), .data_length = model.getByteCode().size()});

    runtimeWrapper.setInputShape(model.getInputShape());
    runtimeWrapper.setNDim(model.getNDim());
    this->functionName = model.getFunctionName();

    this->inputData = std::make_unique<std::byte[]>(model.inputSize());
    this->inputSize = model.inputSize();

    this->outputData = std::make_unique<std::byte[]>(model.outputSize());
    this->outputSize = model.outputSize();
}

void IREEAdapter::infer()
{
    runtimeWrapper.execute(functionName, inputData.get(), inputSize, std::span<std::byte>{outputData.get(), outputSize});
}

}
