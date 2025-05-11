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

#pragma once

#include <ostream>
#include <ErrorHandling.hpp>
#include <Model.hpp>
#include "IREERuntimeWrapper.hpp"

namespace NES
{

class IREEAdapter
{
public:
    static std::shared_ptr<IREEAdapter> create();

    IREEAdapter() = default;

    void initializeModel(Nebuli::Inference::Model& model);

    template <class T>
    void addModelInput(size_t index, T value)
    {
        PRECONDITION(index < inputSize / sizeof(T), "Index is too large");
        std::bit_cast<T*>(inputData.get())[index] = value;
    }

    float getResultAt(size_t idx)
    {
        PRECONDITION(idx < outputSize / 4, "Index is too large");
        return std::bit_cast<float*>(outputData.get())[idx];
    }

    void copyResultTo(std::span<std::byte> content)
    {
        PRECONDITION(outputSize == content.size(), "Output size does not match");
        std::ranges::copy_n(outputData.get(), std::min(content.size(), outputSize), content.data());
    }

    void addModelInput(std::span<std::byte> content)
    {
        std::ranges::copy_n(content.data(), std::min(content.size(), inputSize), inputData.get());
    }

    void infer();

private:
    std::unique_ptr<std::byte[]> inputData{};
    size_t inputSize;
    std::unique_ptr<std::byte[]> outputData{};
    size_t outputSize;
    std::string functionName;
    IREERuntimeWrapper runtimeWrapper;
};

}
