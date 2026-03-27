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

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <iree/runtime/api.h>

namespace NES::Inference
{

class IREERuntimeWrapper
{
public:
    IREERuntimeWrapper() = default;
    ~IREERuntimeWrapper() = default;

    IREERuntimeWrapper(const IREERuntimeWrapper&) = delete;
    IREERuntimeWrapper& operator=(const IREERuntimeWrapper&) = delete;
    IREERuntimeWrapper(IREERuntimeWrapper&&) noexcept;
    IREERuntimeWrapper& operator=(IREERuntimeWrapper&&) noexcept;

    void setup(iree_const_byte_span_t compiledModel);
    void execute(const std::string& functionName, std::span<const std::byte> input, std::span<std::byte> output);
    void setInputShape(std::vector<size_t> shape);
    void setNDim(size_t numDims);

private:
    std::vector<size_t> inputShape;
    size_t nDim = 0;

    struct InstanceDeleter
    {
        void operator()(iree_runtime_instance_t* ptr) const
        {
            if (ptr != nullptr)
            {
                iree_runtime_instance_release(ptr);
            }
        }
    };

    struct SessionDeleter
    {
        void operator()(iree_runtime_session_t* ptr) const
        {
            if (ptr != nullptr)
            {
                iree_runtime_session_release(ptr);
            }
        }
    };

    std::unique_ptr<iree_runtime_instance_t, InstanceDeleter> instance;
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> session;
    iree_vm_function_t function{};
};

}
