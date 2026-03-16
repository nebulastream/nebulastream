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

#include <algorithm>
#include <bit>
#include <cstddef>
#include <span>
#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES::Inference
{

std::shared_ptr<IREEAdapter> IREEAdapter::create()
{
    return std::make_shared<IREEAdapter>();
}

void IREEAdapter::initializeModel(NES::Model& model)
{
    this->runtimeWrapper = IREERuntimeWrapper();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) IREE API requires uint8_t*
    runtimeWrapper.setup(iree_const_byte_span_t{
        .data = reinterpret_cast<const uint8_t*>(model.getByteCode().data()), .data_length = model.getByteCode().size()});

    runtimeWrapper.setInputShape(model.getInputShape());
    runtimeWrapper.setNDim(model.getNDim());
    this->functionName = model.getFunctionName();

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->inputData = std::make_unique<std::byte[]>(model.inputSize());
    this->inputSize = model.inputSize();

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->outputData = std::make_unique<std::byte[]>(model.outputSize());
    this->outputSize = model.outputSize();
}

float IREEAdapter::getResultAt(size_t idx)
{
    PRECONDITION(idx < outputSize / sizeof(float), "Index is too large");
    return std::bit_cast<float*>(outputData.get())[idx];
}

void IREEAdapter::copyResultTo(std::span<std::byte> content)
{
    PRECONDITION(outputSize == content.size(), "Output size does not match");
    std::ranges::copy_n(outputData.get(), static_cast<std::ptrdiff_t>(std::min(content.size(), outputSize)), content.data());
}

size_t IREEAdapter::getOutputSize() const
{
    return outputSize;
}

void IREEAdapter::addModelInput(std::span<std::byte> content)
{
    std::ranges::copy_n(content.data(), static_cast<std::ptrdiff_t>(std::min(content.size(), inputSize)), inputData.get());
}

void IREEAdapter::infer()
{
    runtimeWrapper.execute(
        functionName, std::span<const std::byte>{inputData.get(), inputSize}, std::span<std::byte>{outputData.get(), outputSize});
}

}
