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

#include "IREEAdapter.hpp"
#include <fstream>
#include <Util/Logger/Logger.hpp>
#include <iree/runtime/api.h>
#include "IREERuntimeWrapper.hpp"

#include <Model.hpp>

namespace NES
{

std::shared_ptr<IREEAdapter> IREEAdapter::create()
{
    return std::make_shared<IREEAdapter>();
}

namespace
{
iree_const_byte_span_t asIREESpan(std::span<const std::byte> span)
{
    return iree_const_byte_span_t{.data = reinterpret_cast<const uint8_t*>(span.data()), .data_length = span.size()};
}
}

void IREEAdapter::initializeModel(Nebuli::Inference::Model& model)
{
    this->runtimeWrapper = IREERuntimeWrapper();
    runtimeWrapper.setup(asIREESpan(model.getByteCode()));

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
    runtimeWrapper.execute(functionName, inputData.get(), inputSize, reinterpret_cast<float*>(outputData.get()));
}

}
