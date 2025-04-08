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

#include <iree/runtime/api.h>
#include <vector>

namespace NES::Runtime::Execution::Operators
{

class IREERuntimeWrapper
{
public:
    IREERuntimeWrapper() = default;
    void setup(iree_const_byte_span_t compiledModel);
    float* execute(std::string functionName, void* inputData, size_t inputSize, float* outputData);
    void setInputShape(std::vector<int> inputShape);
    void setNDim(int nDim);
private:
    std::vector<int> inputShape;
    int nDim;
    iree_runtime_instance_t* instance;
    iree_runtime_session_t* session;
    iree_hal_device_t* device;
    iree_vm_function_t function;
};

}