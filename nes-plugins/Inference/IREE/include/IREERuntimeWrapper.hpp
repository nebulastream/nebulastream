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

#include <unordered_map>
#include <vector>
#include <iree/runtime/api.h>

namespace NES
{

class IREERuntimeWrapper
{
public:
    IREERuntimeWrapper()
        : nDim(0)
        , inputDtype(IREE_HAL_ELEMENT_TYPE_NONE)
        , outputDtype(IREE_HAL_ELEMENT_TYPE_NONE)
        , instance(nullptr, &iree_runtime_instance_release)
        , device(nullptr, &iree_hal_device_release)
        , session(nullptr, &iree_runtime_session_release)
        , function{} { };
    void setup(iree_const_byte_span_t compiledModel);
    void execute(std::string functionName, void* inputData, size_t inputSize, void* outputData);
    void setInputShape(std::vector<size_t> inputShape);
    void setNDim(size_t nDim);
    void setInputDtype(iree_hal_element_types_t dtype);
    void setOutputDtype(iree_hal_element_types_t dtype);

private:
    std::vector<size_t> inputShape;
    size_t nDim;
    iree_hal_element_types_t inputDtype;
    iree_hal_element_types_t outputDtype;
    std::unique_ptr<iree_runtime_instance_t, decltype(&iree_runtime_instance_release)> instance;
    std::unique_ptr<iree_hal_device_t, decltype(&iree_hal_device_release)> device;
    std::unique_ptr<iree_runtime_session_t, decltype(&iree_runtime_session_release)> session;
    iree_vm_function_t function;
};

}
