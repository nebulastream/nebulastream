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

#include "IREECompilerWrapper.hpp"
#include "IREERuntimeWrapper.hpp"

#include <ostream>
#include <Model.hpp>

namespace NES::Runtime::Execution::Operators
{

class IREEAdapter
{
public:
    static std::shared_ptr<IREEAdapter> create();

    IREEAdapter() = default;
    ~IREEAdapter();

    void initializeModel(Nebuli::Inference::Model& model);

    template<class T>
    void addModelInput(int index, T value)
    {
        ((T*) inputData)[index] = value;
    };

    void addModelInput(int8_t* content, uint32_t size)
    {
        memcpy(static_cast<float*>(inputData), content, size);
    }

    void infer();
    float getResultAt(int idx) { return outputData[idx]; }

private:
    void* inputData{};
    size_t inputSize;
    float* outputData{};
    std::string functionName;
    IREERuntimeWrapper runtimeWrapper;
};

}